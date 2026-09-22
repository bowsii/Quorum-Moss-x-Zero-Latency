"""
quorum/bus/claim_engine.py
--------------------------
Authoritative claim decision engine for Quorum semantic coordination (§9.1 - §9.2).

Distinguishes:
A. NO_CONFLICT — No semantic conflict; challenger acquires ownership.
B. ACTIVE_EXISTING_CLAIM — Active incumbent exists outside race window; late challenger absorbed.
C. RACE_WINDOW_CONFLICT — Concurrent conflict within 50ms epsilon window; decided by should_claim_win().
D. COMPLETED_FINDING — Semantically equivalent finding already published; challenger absorbed.
E. EXPIRED_CLAIM — Incumbent claim has expired/timed out; challenger supersedes.
F. CHALLENGER_WINS — Race-window tiebreak decided in favour of challenger.
G. CHALLENGER_LOSES — Race-window tiebreak decided in favour of incumbent.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Optional

from bus.schemas import Claim, Finding
from bus.tiebreak import should_claim_win, _EPSILON_SECONDS
from config.settings import settings

logger = logging.getLogger(__name__)


class ClaimResolutionOutcome(str, Enum):
    """Exhaustive categories of claim decision outcomes."""

    NO_CONFLICT = "no_conflict"
    ACTIVE_EXISTING_CLAIM = "active_existing_claim"
    RACE_WINDOW_CONFLICT = "race_window_conflict"
    COMPLETED_FINDING = "completed_finding"
    EXPIRED_CLAIM = "expired_claim"
    CHALLENGER_WINS = "challenger_wins"
    CHALLENGER_LOSES = "challenger_loses"


@dataclass(frozen=True)
class ClaimDecision:
    """Structured decision output from the ClaimDecisionEngine."""

    outcome: ClaimResolutionOutcome
    is_duplicate: bool
    winner_claim_id: Optional[str]
    existing_claim_id: Optional[str] = None
    existing_finding_id: Optional[str] = None
    superseded_claim_id: Optional[str] = None
    reason: str = ""

    @property
    def external_work_allowed(self) -> bool:
        """True only if challenger won ownership and may proceed to external work."""
        return not self.is_duplicate


class ClaimDecisionEngine:
    """Evaluates competing claims and completed findings against a challenger."""

    def __init__(
        self,
        dedup_threshold: float = 0.85,
        ttl_seconds: Optional[float] = None,
    ) -> None:
        self.dedup_threshold = dedup_threshold
        self.distance_threshold = 1.0 - dedup_threshold
        self.ttl_seconds = ttl_seconds if ttl_seconds is not None else float(settings.HEARTBEAT_TTL_SECONDS)

    def evaluate(
        self,
        challenger: Claim,
        sensed_claims: list[dict[str, Any]],
        sensed_findings: list[dict[str, Any]],
    ) -> ClaimDecision:
        """Evaluate a challenger against existing sensed claims and findings.

        Parameters
        ----------
        challenger:
            The incoming Claim requesting ownership.
        sensed_claims:
            Existing claim search results with 'id', 'document', 'metadata', 'distance'.
        sensed_findings:
            Existing finding search results with 'id', 'document', 'metadata', 'distance'.

        Returns
        -------
        ClaimDecision
            Authoritative decision on whether challenger is granted ownership.
        """
        # ------------------------------------------------------------------
        # 1. Check for COMPLETED FINDINGS (D)
        # ------------------------------------------------------------------
        for hit in sensed_findings:
            dist = hit.get("distance")
            meta = hit.get("metadata", {})
            # Only consider findings belonging to the same run (or global if cross-run)
            hit_run_id = meta.get("run_id")
            if hit_run_id and hit_run_id != challenger.run_id:
                continue

            if dist is not None and dist < self.distance_threshold:
                finding_id = hit.get("id") or meta.get("finding_id", "unknown-finding")
                logger.info(
                    "[claim_engine] Completed finding detected: %s (dist=%.3f < %.3f) for challenger %s",
                    finding_id,
                    dist,
                    self.distance_threshold,
                    challenger.id,
                )
                return ClaimDecision(
                    outcome=ClaimResolutionOutcome.COMPLETED_FINDING,
                    is_duplicate=True,
                    winner_claim_id=None,
                    existing_finding_id=finding_id,
                    reason="completed_finding",
                )

        # ------------------------------------------------------------------
        # 2. Filter candidate incumbent claims
        # ------------------------------------------------------------------
        now = datetime.now(timezone.utc)
        deadline = now - timedelta(seconds=self.ttl_seconds)

        active_incumbent: Optional[Claim] = None
        min_distance = 2.0

        for hit in sensed_claims:
            cid = hit.get("id")
            if cid == challenger.id:
                continue  # ignore self

            dist = hit.get("distance")
            meta = hit.get("metadata", {})
            hit_run_id = meta.get("run_id")
            if hit_run_id and hit_run_id != challenger.run_id:
                continue

            if dist is not None and dist < self.distance_threshold:
                status = meta.get("status", "active")
                if status != "active":
                    continue  # already superseded or reaped

                # Parse created_at and last_heartbeat
                last_hb_raw = meta.get("last_heartbeat")
                is_expired = False
                if last_hb_raw:
                    try:
                        last_hb = datetime.fromisoformat(last_hb_raw)
                        if last_hb.tzinfo is None:
                            last_hb = last_hb.replace(tzinfo=timezone.utc)
                        if last_hb < deadline:
                            is_expired = True
                    except Exception:
                        is_expired = True
                else:
                    is_expired = True

                if is_expired:
                    logger.info("[claim_engine] Incumbent claim %s is expired (TTL=%ss)", cid, self.ttl_seconds)
                    continue

                # Found an active, unexpired competing claim
                if dist < min_distance:
                    min_distance = dist
                    created_at_raw = meta.get("created_at")
                    try:
                        created_at = datetime.fromisoformat(created_at_raw) if created_at_raw else challenger.created_at
                        if created_at.tzinfo is None:
                            created_at = created_at.replace(tzinfo=timezone.utc)
                    except Exception:
                        created_at = challenger.created_at

                    active_incumbent = Claim(
                        id=cid,
                        run_id=challenger.run_id,
                        participant_id=meta.get("participant_id", "unknown-agent"),
                        participant_type=meta.get("participant_type", "agent"),
                        content=hit.get("document") or challenger.content,
                        status="active",
                        created_at=created_at,
                        ts_monotonic=float(meta.get("ts_monotonic", challenger.ts_monotonic)),
                        supersedes=meta.get("supersedes") or None,
                    )

        # ------------------------------------------------------------------
        # 3. Decision evaluation against active incumbent
        # ------------------------------------------------------------------
        if active_incumbent is None:
            # A. NO_CONFLICT
            logger.info("[claim_engine] No semantic conflict for challenger %s", challenger.id)
            return ClaimDecision(
                outcome=ClaimResolutionOutcome.NO_CONFLICT,
                is_duplicate=False,
                winner_claim_id=challenger.id,
                reason="no_semantic_conflict",
            )

        # We have an active incumbent. Evaluate race window vs late duplicate.
        mono_diff = challenger.ts_monotonic - active_incumbent.ts_monotonic
        wall_diff = (challenger.created_at - active_incumbent.created_at).total_seconds()
        diff_s = mono_diff if abs(mono_diff) >= 0.001 else wall_diff

        within_race_window = abs(diff_s) <= _EPSILON_SECONDS

        if within_race_window:
            # C. RACE_WINDOW_CONFLICT — Evaluate tiebreak
            challenger_wins = should_claim_win(challenger=challenger, incumbent=active_incumbent)
            if challenger_wins:
                # F. CHALLENGER_WINS
                logger.info(
                    "[claim_engine] Race window tiebreak: Challenger %s (%s) WON against Incumbent %s (%s)",
                    challenger.id,
                    challenger.participant_type,
                    active_incumbent.id,
                    active_incumbent.participant_type,
                )
                return ClaimDecision(
                    outcome=ClaimResolutionOutcome.CHALLENGER_WINS,
                    is_duplicate=False,
                    winner_claim_id=challenger.id,
                    superseded_claim_id=active_incumbent.id,
                    reason="challenger_won_race_tiebreak",
                )
            else:
                # G. CHALLENGER_LOSES
                logger.info(
                    "[claim_engine] Race window tiebreak: Challenger %s (%s) LOST against Incumbent %s (%s)",
                    challenger.id,
                    challenger.participant_type,
                    active_incumbent.id,
                    active_incumbent.participant_type,
                )
                return ClaimDecision(
                    outcome=ClaimResolutionOutcome.CHALLENGER_LOSES,
                    is_duplicate=True,
                    winner_claim_id=active_incumbent.id,
                    existing_claim_id=active_incumbent.id,
                    reason="duplicate_claim",
                )
        else:
            # Outside race window:
            # If challenger is earlier (by >50ms), challenger strictly wins
            if diff_s < -_EPSILON_SECONDS:
                logger.info(
                    "[claim_engine] Challenger %s has strictly earlier timestamp than incumbent %s (diff=%.3fs)",
                    challenger.id,
                    active_incumbent.id,
                    diff_s,
                )
                return ClaimDecision(
                    outcome=ClaimResolutionOutcome.CHALLENGER_WINS,
                    is_duplicate=False,
                    winner_claim_id=challenger.id,
                    superseded_claim_id=active_incumbent.id,
                    reason="challenger_earlier_timestamp",
                )
            else:
                # B. ACTIVE_EXISTING_CLAIM — Late duplicate!
                logger.info(
                    "[claim_engine] Late duplicate claim rejected: Challenger %s is later than active incumbent %s (diff=%.3fs)",
                    challenger.id,
                    active_incumbent.id,
                    diff_s,
                )
                return ClaimDecision(
                    outcome=ClaimResolutionOutcome.ACTIVE_EXISTING_CLAIM,
                    is_duplicate=True,
                    winner_claim_id=active_incumbent.id,
                    existing_claim_id=active_incumbent.id,
                    reason="duplicate_claim",
                )
