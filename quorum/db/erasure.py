"""
quorum/db/erasure.py
---------------------
Right-to-erasure implementation for the Quorum platform.

Full-erasure contract
---------------------
When a caller issues ``DELETE /runs/{id}``, this module guarantees that *every*
piece of data linked to that run is destroyed or suppressed:

1. **ChromaDB claims collection** — all documents whose ``run_id`` metadata
   field matches are deleted from the MOSS claims collection.
2. **ChromaDB findings collection** — same treatment for the findings
   collection.
3. **SQLite relational data** — rows in ``consent_events`` and ``replay_logs``
   that reference the run are deleted first (FK ordering), then the parent
   ``runs`` row is removed.
4. **Telemetry tombstones** — for each span_id captured in ``replay_logs``
   before deletion, a tombstone row is inserted into ``telemetry_tombstones``
   so that the telemetry pipeline can suppress / redact those spans from
   downstream sinks (e.g. Jaeger, OTLP collectors).

The function is intentionally *not* wrapped in a single SQLite transaction with
the ChromaDB deletes (ChromaDB has no two-phase-commit), but SQLite operations
are executed within an explicit transaction so that relational data is either
fully erased or fully preserved on failure.

Return value
------------
A ``dict`` summarising the count of erased objects in each category — suitable
for surfacing in an API response body.
"""

from __future__ import annotations

import aiosqlite
from datetime import datetime, timezone
from typing import Any


async def erase_run(
    run_id: str,
    moss_claims_collection: Any,  # chromadb.Collection
    moss_findings_collection: Any,  # chromadb.Collection
    db: aiosqlite.Connection,
) -> dict[str, int]:
    """Erase all data associated with *run_id* from every storage layer.

    Parameters
    ----------
    run_id:
        The UUID string that identifies the run to be erased.
    moss_claims_collection:
        An open ChromaDB ``Collection`` instance for agent claims.
    moss_findings_collection:
        An open ChromaDB ``Collection`` instance for agent findings.
    db:
        An open ``aiosqlite.Connection`` (caller is responsible for lifecycle).

    Returns
    -------
    dict[str, int]
        Keys and example values::

            {
                "claims_deleted": 12,
                "findings_deleted": 4,
                "consent_events_deleted": 3,
                "replay_logs_deleted": 88,
                "tombstones_inserted": 88,
                "runs_deleted": 1,
            }

    Raises
    ------
    Exception
        Any ChromaDB or SQLite error propagates to the caller unchanged.
        SQLite deletes are rolled back automatically; ChromaDB deletes are
        already committed and cannot be reversed (callers should treat this
        as best-effort on ChromaDB failure after SQLite success).
    """
    erasure_counts: dict[str, int] = {
        "claims_deleted": 0,
        "findings_deleted": 0,
        "consent_events_deleted": 0,
        "replay_logs_deleted": 0,
        "tombstones_inserted": 0,
        "runs_deleted": 0,
    }

    # ------------------------------------------------------------------
    # 1 & 2. Purge ChromaDB vector stores (fail fast before SQLite)
    # ------------------------------------------------------------------
    for collection, count_key in (
        (moss_claims_collection, "claims_deleted"),
        (moss_findings_collection, "findings_deleted"),
    ):
        try:
            result = collection.get(where={"run_id": run_id})
            doc_ids: list[str] = result.get("ids", [])
            if doc_ids:
                collection.delete(ids=doc_ids)
            erasure_counts[count_key] = len(doc_ids)
        except Exception as e:
            raise RuntimeError(f"ChromaDB erasure failed for {count_key}: {e}") from e

    # ------------------------------------------------------------------
    # 3 & 4. Relational erasure inside an explicit SQLite transaction
    # ------------------------------------------------------------------
    now_iso: str = datetime.now(tz=timezone.utc).isoformat()

    await db.execute("BEGIN")
    try:
        # 4a. Collect span_ids from replay_logs BEFORE deletion.
        # Extract the real span_id from JSON payload if present, falling back to row ID.
        cursor = await db.execute(
            "SELECT id, payload FROM replay_logs WHERE run_id = ?",
            (run_id,),
        )
        replay_rows = await cursor.fetchall()
        span_ids: list[str] = []
        for row in replay_rows:
            try:
                import json
                payload_data = json.loads(row[1]) if row[1] else {}
                real_span = payload_data.get("span_id")
                span_ids.append(str(real_span if real_span else row[0]))
            except Exception:
                span_ids.append(str(row[0]))

        # 3a. Delete child tables first to satisfy FK constraints.
        cursor = await db.execute(
            "DELETE FROM consent_events WHERE run_id = ?",
            (run_id,),
        )
        erasure_counts["consent_events_deleted"] = cursor.rowcount  # type: ignore[assignment]

        cursor = await db.execute(
            "DELETE FROM replay_logs WHERE run_id = ?",
            (run_id,),
        )
        erasure_counts["replay_logs_deleted"] = cursor.rowcount  # type: ignore[assignment]

        # 4b. Insert tombstones for every erased span.
        if span_ids:
            tombstone_rows = [
                (span_id, run_id, now_iso) for span_id in span_ids
            ]
            await db.executemany(
                """
                INSERT OR IGNORE INTO telemetry_tombstones
                    (span_id, run_id, tombstoned_at)
                VALUES (?, ?, ?)
                """,
                tombstone_rows,
            )
            # Count the actual tombstones verified in the table for this run
            t_cur = await db.execute(
                "SELECT COUNT(*) FROM telemetry_tombstones WHERE run_id = ?",
                (run_id,),
            )
            t_row = await t_cur.fetchone()
            erasure_counts["tombstones_inserted"] = t_row[0] if t_row else len(span_ids)

        # 3b. Delete the parent run row.
        cursor = await db.execute(
            "DELETE FROM runs WHERE id = ?",
            (run_id,),
        )
        erasure_counts["runs_deleted"] = cursor.rowcount  # type: ignore[assignment]

        await db.execute("COMMIT")

    except Exception:
        await db.execute("ROLLBACK")
        raise

    return erasure_counts

