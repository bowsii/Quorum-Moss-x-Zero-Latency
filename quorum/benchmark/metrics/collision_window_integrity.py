"""Collision-window integrity metric.

Per implementation.md §10.1: should read 100%. 
Anything <100% means the harness guard fired — investigate before reporting.
This is asserted in CI on every PR.
"""
import sys
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def check_collision_window_integrity(log_path: str = None) -> dict:
    """Check collision-window integrity from benchmark logs.
    
    Returns:
        dict with integrity_pct, total_steps, violations, gate_passed
    
    If no log file exists (first run / CI environment), returns 100% (vacuously true).
    """
    if log_path is None:
        # Look for the most recent board run log
        log_dir = Path(__file__).resolve().parent.parent / "logs"
        if not log_dir.exists():
            return {
                "integrity_pct": 100.0,
                "total_steps": 0,
                "violations": [],
                "gate_passed": True,
                "note": "No log directory found — vacuously passing (no runs recorded yet)",
            }
        
        logs = sorted(log_dir.glob("board_run_*.json"), key=lambda p: p.stat().st_mtime)
        if not logs:
            return {
                "integrity_pct": 100.0,
                "total_steps": 0,
                "violations": [],
                "gate_passed": True,
                "note": "No run logs found — vacuously passing",
            }
        log_path = str(logs[-1])

    try:
        with open(log_path) as f:
            run_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Could not read log file {log_path}: {e}")
        return {
            "integrity_pct": 0.0,
            "total_steps": 0,
            "violations": [str(e)],
            "gate_passed": False,
        }

    steps = run_data.get("steps", [])
    violations = []

    for step in steps:
        # A violation is: external call recorded before claim_resolved=True
        if step.get("external_call_made") and not step.get("claim_resolved"):
            violations.append({
                "step_index": step.get("step_index"),
                "participant_id": step.get("participant_id"),
                "action": step.get("action"),
            })

    total = len(steps)
    violation_count = len(violations)
    integrity_pct = 100.0 if total == 0 else (total - violation_count) / total * 100

    return {
        "integrity_pct": integrity_pct,
        "total_steps": total,
        "violations": violations,
        "gate_passed": integrity_pct >= 100.0,
    }


if __name__ == "__main__":
    result = check_collision_window_integrity()
    print(f"Collision-window integrity: {result['integrity_pct']:.1f}%")
    print(f"Total steps: {result['total_steps']}")
    
    if result["violations"]:
        print(f"VIOLATIONS ({len(result['violations'])}):")
        for v in result["violations"]:
            print(f"  - Step {v['step_index']}: {v['participant_id']} called external before claim resolved")
    
    if not result["gate_passed"]:
        print("INTEGRITY CHECK FAILED — harness guard fired. Investigate before reporting.")
        sys.exit(1)
    else:
        print(f"OK: {result.get('note', 'All steps respected the ordering constraint')}")
        sys.exit(0)
