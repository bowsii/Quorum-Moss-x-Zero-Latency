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

_HUMAN_EPSILON_MS: float = 50.0
_EPSILON_SECONDS: float = _HUMAN_EPSILON_MS / 1000.0  # 0.050s
"""Milliseconds within which priority breaks ties between different participant types."""


def participant_priority(p_type: ParticipantType) -> int:
    """
    Return the numeric priority for a participant type.

    Lower values indicate higher precedence:
      human (0) > adjudicator (1) > agent (2) > reaper (3).

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
      1. If the challenger and incumbent timestamps are within the 50ms epsilon
         window AND their participant types differ:
         The participant with higher priority (lower numeric priority) wins.
      2. Outside the 50ms epsilon window, or when participant types are identical:
         The earlier timestamp strictly wins. Participant priority NEVER overrides
         a real time gap!
      3. If timestamps are identical:
         The challenger wins only if its participant_id is lexicographically less
         than the incumbent's participant_id.

    Args:
        challenger: The incoming Claim that wants to take precedence.
        incumbent:  The currently active Claim on the board.

    Returns:
        True if the challenger should supersede the incumbent.
    """
    c_prio = participant_priority(challenger.participant_type)
    i_prio = participant_priority(incumbent.participant_type)
    types_differ = challenger.participant_type != incumbent.participant_type

    # Determine time difference (monotonic primary, wall-clock fallback for tests)
    mono_diff = challenger.ts_monotonic - incumbent.ts_monotonic
    wall_diff = (challenger.created_at - incumbent.created_at).total_seconds()

    # Use monotonic difference if it exhibits a distinct time delta (>= 1ms).
    # Otherwise, if created_at was explicitly specified, use wall-clock difference.
    if abs(mono_diff) >= 0.001:
        diff_s = mono_diff
    elif abs(wall_diff) >= 0.001:
        diff_s = wall_diff
    else:
        diff_s = mono_diff

    within_epsilon = abs(diff_s) <= _EPSILON_SECONDS

    # 1. Inside epsilon window with differing participant types: priority breaks tie
    if within_epsilon and types_differ:
        return c_prio < i_prio

    # 2. Outside epsilon window or same participant type: earlier timestamp strictly wins
    if diff_s < 0:
        return True
    if diff_s > 0:
        return False

    # 3. Tie on timestamp: deterministic lexicographical fallback
    return challenger.participant_id < incumbent.participant_id

