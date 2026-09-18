"""
Tiebreaking logic for concurrent claims on the Moss semantic board (§9.2).

Priority rules:
  1. Participant type priority: human (0) > adjudicator (1) > agent (2) > reaper (3).
  2. Human epsilon window: if the challenger is a human and its created_at is
     within 50 ms of the incumbent's created_at, the human wins unconditionally.
  3. On equal priority: lower participant_id string wins (deterministic lexicographic).
"""

from datetime import datetime, timedelta
from bus.schemas import Claim, ParticipantType

# ---------------------------------------------------------------------------
# Priority table
# ---------------------------------------------------------------------------

_PRIORITY: dict[str, int] = {
    "human": 0,
    "adjudicator": 1,
    "agent": 2,
    "reaper": 3,
}

_HUMAN_EPSILON_MS: int = 50
"""Milliseconds within which a human claim wins regardless of timestamp order."""


def participant_priority(p_type: ParticipantType) -> int:
    """
    Return the numeric priority for a participant type.

    Lower values indicate higher precedence.

    Args:
        p_type: One of 'human', 'adjudicator', 'agent', 'reaper'.

    Returns:
        Integer priority (0 = highest).
    """
    return _PRIORITY[p_type]


def should_claim_win(challenger: Claim, incumbent: Claim) -> bool:
    """
    Return True if challenger should supersede incumbent.

    Resolution algorithm (§9.2):
      1. Human epsilon check: if the challenger is 'human' and the absolute
         time difference between challenger.created_at and incumbent.created_at
         is ≤ 50 ms, the challenger wins regardless of other factors.
      2. Priority comparison: the participant with the lower priority number
         wins (human beats agent, etc.).
      3. Tie on priority: the challenger wins only if its participant_id is
         lexicographically less than the incumbent's participant_id.

    Args:
        challenger: The incoming Claim that wants to take precedence.
        incumbent:  The currently active Claim on the board.

    Returns:
        True if the challenger should supersede the incumbent.
    """
    challenger_priority = participant_priority(challenger.participant_type)
    incumbent_priority = participant_priority(incumbent.participant_type)

    # --- Human epsilon window -------------------------------------------
    if challenger.participant_type == "human":
        delta_ms = abs(
            (challenger.created_at - incumbent.created_at).total_seconds() * 1000
        )
        if delta_ms <= _HUMAN_EPSILON_MS:
            # Human is within the epsilon window: human wins unconditionally.
            return True

    # --- Priority comparison ---------------------------------------------
    if challenger_priority < incumbent_priority:
        return True
    if challenger_priority > incumbent_priority:
        return False

    # --- Tie: deterministic lexicographic fallback ----------------------
    return challenger.participant_id < incumbent.participant_id
