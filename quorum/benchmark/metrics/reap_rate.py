"""quorum/benchmark/metrics/reap_rate.py
---------------------------------------
Reap rate metric calculation.
Measures the percentage of claims reaped due to heartbeat timeouts or terminations.
"""
import json
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def calculate_reap_rate(log_path: Optional[str] = None) -> dict:
    """Compute the reap rate from run replay logs or board state.

    Returns:
        dict with total_claims, reaped_claims, reap_rate_pct, reap_reasons breakdown.
    """
    if log_path is None:
        log_dir = Path(__file__).resolve().parent.parent / "logs"
        if not log_dir.exists():
            return {
                "total_claims": 0,
                "reaped_claims": 0,
                "reap_rate_pct": 0.0,
                "reasons": {},
                "note": "No log directory found",
            }
        logs = sorted(log_dir.glob("board_run_*.json"), key=lambda p: p.stat().st_mtime)
        if not logs:
            return {
                "total_claims": 0,
                "reaped_claims": 0,
                "reap_rate_pct": 0.0,
                "reasons": {},
                "note": "No run logs found",
            }
        log_path = str(logs[-1])

    try:
        with open(log_path) as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Failed to read log {log_path}: {e}")
        return {"error": str(e), "reap_rate_pct": 0.0}

    claims = data.get("claims", [])
    total = len(claims)
    reaped = [c for c in claims if c.get("status") == "reaped"]
    reasons = {}
    for c in reaped:
        r = c.get("reap_reason", "unknown")
        reasons[r] = reasons.get(r, 0) + 1

    rate = (len(reaped) / total * 100.0) if total > 0 else 0.0

    return {
        "total_claims": total,
        "reaped_claims": len(reaped),
        "reap_rate_pct": round(rate, 2),
        "reasons": reasons,
    }


if __name__ == "__main__":
    res = calculate_reap_rate()
    print(f"Reap Rate: {res.get('reap_rate_pct', 0.0)}% ({res.get('reaped_claims', 0)}/{res.get('total_claims', 0)})")
    if res.get("reasons"):
        print("Reasons breakdown:", res["reasons"])
