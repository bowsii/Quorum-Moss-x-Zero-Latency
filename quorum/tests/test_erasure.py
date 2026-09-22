"""tests/test_erasure.py
-----------------------
Unit tests for db/erasure.py.
Verifies the non-negotiable: DELETE /runs/{id} drops Moss namespaces,
deletes SQLite rows, and tombstones telemetry spans (all three verified).
"""
import pytest
import aiosqlite
import uuid
from db.models import init_db, get_db
from db.erasure import erase_run
from bus.namespaces import get_claims_namespace, get_findings_namespace
from bus.moss_client import write_claim, write_finding
from bus.schemas import Claim, Finding


@pytest.mark.asyncio
async def test_full_erasure_targets_all_three_layers(tmp_path):
    """Verify that erase_run:
    1. Removes documents from ChromaDB claims & findings
    2. Deletes runs, consent_events, replay_logs from SQLite
    3. Inserts tombstones into telemetry_tombstones
    """
    db_file = str(tmp_path / "test_quorum.db")
    await init_db(db_path=db_file)

    run_id = f"test-run-{uuid.uuid4()}"

    # 1. Seed ChromaDB
    claims_col = get_claims_namespace()
    findings_col = get_findings_namespace()

    claim = Claim(
        id=f"c-{run_id}",
        run_id=run_id,
        participant_id="agent-1",
        participant_type="agent",
        content="Claim to be erased",
    )
    await write_claim(claim)

    finding = Finding(
        id=f"f-{run_id}",
        run_id=run_id,
        participant_id="agent-1",
        participant_type="agent",
        title="Finding to be erased",
        content="Finding content",
    )
    await write_finding(finding)

    # 2. Seed SQLite
    async with aiosqlite.connect(db_file) as db:
        await db.execute(
            "INSERT INTO runs (id, question, started_at, status) VALUES (?, ?, ?, ?)",
            (run_id, "Erasure question", "2026-09-18T00:00:00Z", "completed"),
        )
        await db.execute(
            "INSERT INTO consent_events (run_id, event_type, timestamp, participant_id) VALUES (?, ?, ?, ?)",
            (run_id, "opt_in", "2026-09-18T00:00:00Z", "user-1"),
        )
        await db.execute(
            "INSERT INTO replay_logs (run_id, step_index, participant_id, action, payload, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
            (run_id, 1, "agent-1", "sense", '{"span_id": "span-abc-123"}', "2026-09-18T00:00:00Z"),
        )
        await db.commit()

        # 3. Perform Erase
        result = await erase_run(
            run_id=run_id,
            moss_claims_collection=claims_col,
            moss_findings_collection=findings_col,
            db=db,
        )

        # 4. Verify SQLite layers
        async with db.execute("SELECT COUNT(*) FROM runs WHERE id = ?", (run_id,)) as cur:
            row = await cur.fetchone()
            assert row[0] == 0, "Run row must be deleted"

        async with db.execute("SELECT COUNT(*) FROM consent_events WHERE run_id = ?", (run_id,)) as cur:
            row = await cur.fetchone()
            assert row[0] == 0, "Consent events must be deleted"

        async with db.execute("SELECT COUNT(*) FROM replay_logs WHERE run_id = ?", (run_id,)) as cur:
            row = await cur.fetchone()
            assert row[0] == 0, "Replay logs must be deleted"

        async with db.execute("SELECT COUNT(*) FROM telemetry_tombstones WHERE run_id = ?", (run_id,)) as cur:
            row = await cur.fetchone()
            assert row[0] >= 1, "Telemetry tombstone must be created for erased spans"

    # 5. Verify ChromaDB layers
    res_claims = claims_col.get(where={"run_id": run_id})
    assert len(res_claims["ids"]) == 0, "Chroma claims must be deleted"

    res_findings = findings_col.get(where={"run_id": run_id})
    assert len(res_findings["ids"]) == 0, "Chroma findings must be deleted"

    assert result["runs_deleted"] == 1
    assert result["claims_deleted"] >= 1
    assert result["findings_deleted"] >= 1


@pytest.mark.asyncio
async def test_erasure_tombstone_count_and_retryability(tmp_path):
    """Verify multiple spans in replay logs produce exact tombstone counts, and erase_run is safely retryable."""
    db_file = str(tmp_path / "test_quorum_retry.db")
    await init_db(db_path=db_file)

    run_id = f"test-run-retry-{uuid.uuid4()}"
    claims_col = get_claims_namespace()
    findings_col = get_findings_namespace()

    async with aiosqlite.connect(db_file) as db:
        await db.execute(
            "INSERT INTO runs (id, question, started_at, status) VALUES (?, ?, ?, ?)",
            (run_id, "Erasure retry question", "2026-09-18T00:00:00Z", "completed"),
        )
        # Insert 3 distinct replay logs with real span_ids
        for idx in range(3):
            await db.execute(
                "INSERT INTO replay_logs (run_id, step_index, participant_id, action, payload, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
                (run_id, idx, "agent-1", "action", f'{{"span_id": "span-distinct-{idx}"}}', "2026-09-18T00:00:00Z"),
            )
        await db.commit()

        # First erase run
        res1 = await erase_run(
            run_id=run_id,
            moss_claims_collection=claims_col,
            moss_findings_collection=findings_col,
            db=db,
        )
        assert res1["runs_deleted"] == 1
        assert res1["replay_logs_deleted"] == 3
        assert res1["tombstones_inserted"] == 3

        # Second erase run (retry) - should safely succeed as an idempotent operation
        res2 = await erase_run(
            run_id=run_id,
            moss_claims_collection=claims_col,
            moss_findings_collection=findings_col,
            db=db,
        )
        assert res2["runs_deleted"] == 0
        assert res2["claims_deleted"] == 0
        assert res2["findings_deleted"] == 0
        assert res2["consent_events_deleted"] == 0
        assert res2["replay_logs_deleted"] == 0
        assert res2["tombstones_inserted"] == 0


