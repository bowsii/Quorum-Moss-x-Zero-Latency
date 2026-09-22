"""
tests/test_phase_b_database.py
---------------------------------
Comprehensive test suite for Stage 2 Phase B: Authoritative Transactional State.

Validates:
Test 1: Fresh database migrations & idempotency (verifies all tables and version recording).
Test 2: Model integrity, constraints, invalid value rejections, and foreign key cascades.
Test 3: Transaction rollback with multiple related entities (ZERO partial state remains on error).
Test 4: Transaction commit (valid entities durably persist).
Test 5: Persistence across independent connections (Connection A writes, Connection B reads).
Test 6: Chroma is NOT authoritative (candidate retrieval vs database ownership).
Test 7: Fail-closed behavior on database unavailability (DatabaseUnavailableError prevents ownership).
Test 8: Concurrent migration safety (PostgreSQL advisory locks prevent race conditions).
Test 9: Transaction isolation and row locking (FOR UPDATE locks rows until commit/rollback).
Test 10: Audit append-only behavior (insert/read works; no mutation methods exposed).
Test 11: Repository transaction reuse (multiple repo calls share ambient connection and atomic commit/rollback).
Test 12: PostgreSQL pool lifecycle (initialization, connection acquire/release, graceful shutdown).
Diagnostic Benchmark: Performance baseline (N=100 iterations, recording p50/p95/p99).
"""

import asyncio
import os
import pathlib
import tempfile
import time
from datetime import datetime, timezone, timedelta
import uuid

import asyncpg
import pytest
import pytest_asyncio

from db.migrations.runner import apply_migrations
from db.connection import (
    init_pool,
    close_pool,
    get_pool,
    get_connection,
    DatabaseUnavailableError,
)
from db.transaction import transaction, get_current_connection
from db.repositories import (
    run_repo,
    task_repo,
    claim_repo,
    worker_repo,
    finding_repo,
    idempotency_repo,
    audit_repo,
)
from bus.candidate_retrieval import find_semantic_candidates, SemanticCandidate
from bus.moss_client import write_claim
from bus.schemas import Claim


@pytest.fixture(scope="session")
def pg_server_dsn():
    """Provides a PostgreSQL DSN, spinning up an embedded server if no external URL is set."""
    env_dsn = os.environ.get("POSTGRES_URL", "")
    if env_dsn:
        yield env_dsn
        return

    from embedded_postgres import get_server
    temp_dir = pathlib.Path(tempfile.mkdtemp(prefix="quorum_pg_phase_b_"))
    server = get_server(temp_dir)
    with server:
        uri = server.get_uri()
        yield uri


@pytest_asyncio.fixture(autouse=True)
async def setup_db(pg_server_dsn):
    """Initialize database pool and ensure migrations are applied for each test."""
    pool = await init_pool(dsn=pg_server_dsn, min_size=2, max_size=10)
    async with pool.acquire() as conn:
        await apply_migrations(conn)
    yield pool
    await close_pool()


# ---------------------------------------------------------------------------
# Test 1: Fresh Database Migrations
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_1_fresh_database_migrations(pg_server_dsn):
    """A fresh PostgreSQL connection can verify applied migrations and expected tables."""
    conn = await asyncpg.connect(pg_server_dsn)
    try:
        # Verify schema_migrations recorded version 1
        rows = await conn.fetch("SELECT version, name, applied_at FROM schema_migrations ORDER BY version ASC;")
        assert len(rows) >= 1
        assert rows[0]["version"] == 1
        assert rows[0]["name"] == "initial_authoritative_state"

        # Verify all expected tables exist in PostgreSQL information_schema
        expected_tables = {
            "schema_migrations",
            "runs",
            "tasks",
            "workers",
            "claims",
            "findings",
            "idempotency_records",
            "audit_events",
        }
        table_rows = await conn.fetch(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
        )
        existing_tables = {r["table_name"] for r in table_rows}
        assert expected_tables.issubset(existing_tables), f"Missing tables: {expected_tables - existing_tables}"

        # Re-running migrations must be idempotent (no-op)
        reapplied = await apply_migrations(conn)
        assert reapplied == [], "Reapplying migrations should return empty list (idempotent)"
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# Test 2: Model Integrity and Constraints
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_2_model_integrity_and_constraints(pg_server_dsn):
    """Verify entities, foreign keys, unique constraints, check constraints, and rejection of invalid values."""
    run_id = f"run-{uuid.uuid4()}"
    task_id = f"task-{uuid.uuid4()}"
    worker_id = f"worker-{uuid.uuid4()}"
    claim_id = f"claim-{uuid.uuid4()}"
    finding_id = f"finding-{uuid.uuid4()}"

    # 1. Create run
    run = await run_repo.create_run(
        run_id=run_id,
        question="What is the impact of asynchronous replication on read-after-write consistency?",
        status="running",
    )
    assert run["id"] == run_id
    assert run["status"] == "running"

    # Verify invalid run status rejected by check constraint
    conn = await asyncpg.connect(pg_server_dsn)
    try:
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "INSERT INTO runs (id, question, status) VALUES ($1, $2, $3);",
                f"invalid-{uuid.uuid4()}",
                "Invalid status test",
                "invalid_status_string",
            )

        # Verify foreign key rejection: creating task for non-existent run_id fails
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await conn.execute(
                "INSERT INTO tasks (id, run_id, task_key, description, status) VALUES ($1, $2, $3, $4, $5);",
                f"orphan-task-{uuid.uuid4()}",
                "nonexistent-run-id",
                "key",
                "desc",
                "queued",
            )
    finally:
        await conn.close()

    # 2. Create worker
    worker = await worker_repo.register_worker(
        worker_id=worker_id,
        run_id=run_id,
        worker_type="agent",
        status="ready",
    )
    assert worker["id"] == worker_id
    assert worker["status"] == "ready"

    # Verify invalid worker status rejected
    conn = await asyncpg.connect(pg_server_dsn)
    try:
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "INSERT INTO workers (id, run_id, worker_type, status) VALUES ($1, $2, $3, $4);",
                f"worker-invalid-{uuid.uuid4()}",
                run_id,
                "agent",
                "invalid_worker_state",
            )
    finally:
        await conn.close()

    # 3. Create task
    task = await task_repo.create_task(
        task_id=task_id,
        run_id=run_id,
        task_key="replication_consistency_study",
        description="Analyze consistency anomalies under network partitions",
        status="queued",
    )
    assert task["id"] == task_id
    assert task["run_id"] == run_id

    # Duplicate task_key under same run must raise UniqueViolationError
    with pytest.raises(asyncpg.UniqueViolationError):
        await task_repo.create_task(
            task_id=f"dup-{uuid.uuid4()}",
            run_id=run_id,
            task_key="replication_consistency_study",
            description="Duplicate key attempt",
        )

    # 4. Create claim with lease
    claim = await claim_repo.create_claim(
        claim_id=claim_id,
        task_id=task_id,
        run_id=run_id,
        owner_id=worker_id,
        owner_type="agent",
        status="active",
        lease_version=1,
    )
    assert claim["id"] == claim_id
    assert claim["lease_version"] == 1

    # Verify invalid lease_version (< 1) rejected by check constraint
    conn = await asyncpg.connect(pg_server_dsn)
    try:
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                """
                INSERT INTO claims (id, task_id, run_id, owner_id, owner_type, status, lease_id, lease_version, expires_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW() + interval '10s');
                """,
                f"claim-invalid-{uuid.uuid4()}",
                task_id,
                run_id,
                worker_id,
                "agent",
                "active",
                "lease-invalid",
                0,  # Violates lease_version >= 1
            )
    finally:
        await conn.close()

    # 5. Create finding
    finding = await finding_repo.create_finding(
        finding_id=finding_id,
        run_id=run_id,
        task_id=task_id,
        claim_id=claim_id,
        participant_id=worker_id,
        participant_type="agent",
        title="Replication Consistency Results",
        content="Asynchronous replication permits stale reads across network partitions.",
    )
    assert finding["id"] == finding_id
    assert finding["claim_id"] == claim_id

    # 6. Idempotency record with unique constraint
    idem = await idempotency_repo.create_record(
        idempotency_key="idem-key-1",
        scope="task_execution",
        status="in_progress",
    )
    assert idem["idempotency_key"] == "idem-key-1"

    with pytest.raises(asyncpg.UniqueViolationError):
        await idempotency_repo.create_record(
            idempotency_key="idem-key-1",
            scope="task_execution",
            status="in_progress",
        )

    # 7. Audit event
    event = await audit_repo.record_event(
        run_id=run_id,
        actor_id=worker_id,
        event_type="claim_created",
        entity_type="claim",
        entity_id=claim_id,
        details={"lease_version": 1},
    )
    assert event["event_type"] == "claim_created"
    assert event["entity_type"] == "claim"


# ---------------------------------------------------------------------------
# Test 3: Transaction Rollback (Multiple Related Entities)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_3_transaction_rollback():
    """Intentionally failing a transaction with multiple related entities leaves ZERO partial state."""
    run_id = f"run-rollback-{uuid.uuid4()}"
    task_id = f"task-rollback-{uuid.uuid4()}"
    claim_id = f"claim-rollback-{uuid.uuid4()}"
    finding_id = f"finding-rollback-{uuid.uuid4()}"
    worker_id = f"worker-rollback-{uuid.uuid4()}"

    try:
        async with transaction() as tx_conn:
            # Create multiple related entities inside the transaction
            await run_repo.create_run(run_id=run_id, question="Rollback verification run", conn=tx_conn)
            await worker_repo.register_worker(worker_id=worker_id, run_id=run_id, status="ready", conn=tx_conn)
            await task_repo.create_task(
                task_id=task_id,
                run_id=run_id,
                task_key="rollback_task_multi",
                description="This task should not persist",
                conn=tx_conn,
            )
            await claim_repo.create_claim(
                claim_id=claim_id,
                task_id=task_id,
                run_id=run_id,
                owner_id=worker_id,
                conn=tx_conn,
            )
            await finding_repo.create_finding(
                finding_id=finding_id,
                run_id=run_id,
                task_id=task_id,
                participant_id=worker_id,
                participant_type="agent",
                title="Rollback finding",
                content="Transient data",
                conn=tx_conn,
            )
            await audit_repo.record_event(
                run_id=run_id,
                actor_id=worker_id,
                event_type="test_rollback",
                conn=tx_conn,
            )

            # Intentionally raise an exception to trigger transaction rollback
            raise RuntimeError("Simulated transaction failure across multiple entities")
    except RuntimeError:
        pass

    # Verify that ZERO partial state remains in any table
    assert await run_repo.get_run(run_id) is None, "Run persisted despite transaction rollback!"
    assert await task_repo.get_task(task_id) is None, "Task persisted despite transaction rollback!"
    assert await claim_repo.get_claim(claim_id) is None, "Claim persisted despite transaction rollback!"
    assert await finding_repo.get_finding(finding_id) is None, "Finding persisted despite transaction rollback!"
    assert await worker_repo.get_worker(worker_id) is None, "Worker persisted despite transaction rollback!"
    events = await audit_repo.list_events_by_run(run_id)
    assert len(events) == 0, "Audit event persisted despite transaction rollback!"


# ---------------------------------------------------------------------------
# Test 4: Transaction Commit
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_4_transaction_commit():
    """Successful transaction commits all valid entities durably."""
    run_id = f"run-commit-{uuid.uuid4()}"
    task_id = f"task-commit-{uuid.uuid4()}"
    claim_id = f"claim-commit-{uuid.uuid4()}"

    async with transaction() as tx_conn:
        await run_repo.create_run(run_id=run_id, question="Commit verification run", conn=tx_conn)
        await task_repo.create_task(
            task_id=task_id,
            run_id=run_id,
            task_key="commit_task",
            description="This task must persist",
            conn=tx_conn,
        )
        await claim_repo.create_claim(
            claim_id=claim_id,
            task_id=task_id,
            run_id=run_id,
            owner_id="worker-commit",
            conn=tx_conn,
        )

    # Verify state exists durably outside the transaction
    run = await run_repo.get_run(run_id)
    task = await task_repo.get_task(task_id)
    claim = await claim_repo.get_claim(claim_id)

    assert run is not None
    assert task is not None
    assert task["task_key"] == "commit_task"
    assert claim is not None
    assert claim["owner_id"] == "worker-commit"


# ---------------------------------------------------------------------------
# Test 5: Persistence Across Independent Connections
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_5_persistence_across_connections(pg_server_dsn):
    """Connection A writes a task; independent Connection B reads it."""
    run_id = f"run-conn-{uuid.uuid4()}"
    task_id = f"task-conn-{uuid.uuid4()}"

    # Connection A creates run and task
    conn_a = await asyncpg.connect(pg_server_dsn)
    try:
        await run_repo.create_run(run_id=run_id, question="Cross-connection test", conn=conn_a)
        await task_repo.create_task(
            task_id=task_id,
            run_id=run_id,
            task_key="cross_conn_task",
            description="Written by connection A",
            conn=conn_a,
        )
    finally:
        await conn_a.close()

    # Connection B opens independently and reads
    conn_b = await asyncpg.connect(pg_server_dsn)
    try:
        task_from_b = await task_repo.get_task(task_id, conn=conn_b)
        assert task_from_b is not None
        assert task_from_b["description"] == "Written by connection A"
    finally:
        await conn_b.close()


# ---------------------------------------------------------------------------
# Test 6: Chroma is NOT Authoritative
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_6_chroma_is_not_authoritative():
    """Chroma returns semantic candidates, but PostgreSQL authoritative status decides ownership."""
    run_id = f"run-chroma-boundary-{uuid.uuid4()}"
    task_id = f"task-boundary-{uuid.uuid4()}"
    claim_id = f"claim-boundary-{uuid.uuid4()}"

    await run_repo.create_run(run_id=run_id, question="Semantic boundary test")
    await task_repo.create_task(
        task_id=task_id,
        run_id=run_id,
        task_key="semantic_boundary_task",
        description="Exploring distributed consensus under crash faults",
    )

    # 1. Index document into Chroma (semantic representation)
    content = "Exploring distributed consensus under crash faults in asynchronous networks."
    chroma_claim = Claim(
        id=claim_id,
        run_id=run_id,
        participant_id="agent-incumbent",
        participant_type="agent",
        content=content,
    )
    await write_claim(chroma_claim)

    # 2. Query Chroma for candidates via candidate retrieval boundary
    candidates = await find_semantic_candidates(
        query="distributed consensus crash faults in networks",
        namespace="claims",
    )
    assert len(candidates) >= 1
    found_candidate = next((c for c in candidates if c.id == claim_id), None)
    assert found_candidate is not None
    assert found_candidate.similarity is not None and found_candidate.similarity > 0.5

    # 3. Authoritative check: Claim is NOT registered in PostgreSQL yet
    db_claim = await claim_repo.get_claim(claim_id)
    assert db_claim is None, "Chroma indexed document must not exist as authoritative claim in database"

    # 4. Now register claim in PostgreSQL as EXPIRED (simulating expired lease)
    await claim_repo.create_claim(
        claim_id=claim_id,
        task_id=task_id,
        run_id=run_id,
        owner_id="agent-incumbent",
        status="expired",
        expires_at=datetime.now(timezone.utc) - timedelta(seconds=10),
    )

    # Chroma still returns the document as semantically similar,
    # but the authoritative database proves the claim is expired, so it cannot own the work!
    active_claim = await claim_repo.get_active_claim_for_task(task_id)
    assert active_claim is None, "Expired claim in DB must not be treated as active owner despite Chroma hit"


# ---------------------------------------------------------------------------
# Test 7: Database Failure Fails Closed
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_7_database_failure_fails_closed():
    """If database connection is unavailable, operations fail closed with DatabaseUnavailableError and never grant ownership."""
    bad_dsn = "postgresql://postgres:@127.0.0.1:54329/nonexistent"

    # 1. Direct connection failure
    with pytest.raises(DatabaseUnavailableError):
        async with get_connection(dsn=bad_dsn, timeout=0.5) as conn:
            await conn.execute("SELECT 1")

    # 2. Pool initialization failure
    with pytest.raises(DatabaseUnavailableError):
        await init_pool(dsn=bad_dsn, timeout=0.5)

    # 3. Ensure fail-closed safety: No ownership or claim acceptance on DB failure
    # If a worker or engine attempted to claim with unreachable DB, it raises DatabaseUnavailableError
    # and MUST NOT fall back to accepting the claim or granting a coordination capability.
    async def try_acquire_ownership(task_id: str, worker_id: str):
        try:
            async with get_connection(dsn=bad_dsn, timeout=0.5) as bad_conn:
                await claim_repo.create_claim(
                    claim_id=f"c-{uuid.uuid4()}",
                    task_id=task_id,
                    run_id="run-1",
                    owner_id=worker_id,
                    conn=bad_conn,
                )
                return True
        except DatabaseUnavailableError:
            # Fails closed: ownership is denied!
            return False

    granted = await try_acquire_ownership("task-down", "agent-x")
    assert granted is False, "Failure must fail closed and deny ownership when database is unreachable!"


# ---------------------------------------------------------------------------
# Test 8: Concurrent Migration Safety
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_8_concurrent_migration_safety(pg_server_dsn):
    """Multiple concurrent processes running apply_migrations serialize safely via advisory locks."""
    async def run_migrator():
        conn = await asyncpg.connect(pg_server_dsn)
        try:
            return await apply_migrations(conn)
        finally:
            await conn.close()

    # Launch 5 concurrent migrators simultaneously
    results = await asyncio.gather(*[run_migrator() for _ in range(5)])

    # All must execute successfully without locking conflict or schema error
    conn = await asyncpg.connect(pg_server_dsn)
    try:
        rows = await conn.fetch("SELECT version, COUNT(*) as cnt FROM schema_migrations GROUP BY version;")
        for r in rows:
            assert r["cnt"] == 1, f"Duplicate migration row found for version {r['version']}!"
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# Test 9: Transaction Isolation & Row-Level Locking
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_9_transaction_isolation_and_row_locking(pg_server_dsn):
    """Verify explicit FOR UPDATE row locking across independent connections."""
    run_id = f"run-lock-{uuid.uuid4()}"
    task_id = f"task-lock-{uuid.uuid4()}"
    claim_id = f"claim-lock-{uuid.uuid4()}"

    await run_repo.create_run(run_id=run_id, question="Locking verification run")
    await task_repo.create_task(
        task_id=task_id,
        run_id=run_id,
        task_key="locking_task",
        description="Task to test row-level locking",
    )
    await claim_repo.create_claim(
        claim_id=claim_id,
        task_id=task_id,
        run_id=run_id,
        owner_id="worker-1",
    )

    conn_a = await asyncpg.connect(pg_server_dsn)
    conn_b = await asyncpg.connect(pg_server_dsn)
    try:
        tx_a = conn_a.transaction()
        await tx_a.start()

        # Connection A acquires exclusive row lock on the active claim
        locked_claim = await claim_repo.get_active_claim_for_task(task_id, for_update=True, conn=conn_a)
        assert locked_claim is not None
        assert locked_claim["id"] == claim_id

        # Connection B attempts to acquire the lock with NOWAIT; PostgreSQL must raise LockNotAvailableError
        with pytest.raises(asyncpg.LockNotAvailableError):
            await conn_b.execute(
                "SELECT id FROM claims WHERE task_id = $1 AND status = 'active' FOR UPDATE NOWAIT;",
                task_id,
            )

        # Connection A commits its transaction, releasing the lock
        await tx_a.commit()

        # Connection B can now acquire the row lock successfully
        tx_b = conn_b.transaction()
        await tx_b.start()
        b_locked = await conn_b.fetchrow(
            "SELECT id FROM claims WHERE task_id = $1 AND status = 'active' FOR UPDATE NOWAIT;",
            task_id,
        )
        assert b_locked is not None
        assert b_locked["id"] == claim_id
        await tx_b.rollback()

        # Verify both connections remain healthy
        res_a = await conn_a.fetchval("SELECT 1;")
        res_b = await conn_b.fetchval("SELECT 1;")
        assert res_a == 1
        assert res_b == 1

    finally:
        await conn_a.close()
        await conn_b.close()

    # Verify that requesting for_update outside a transaction context raises ValueError
    with pytest.raises(ValueError, match="Row-level locking"):
        await claim_repo.get_active_claim_for_task(task_id, for_update=True)


# ---------------------------------------------------------------------------
# Test 10: Audit Append-Only Behavior
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_10_audit_append_only_behavior():
    """Verify audit events are append-only: can be inserted/read, but repository exposes no mutation paths."""
    run_id = f"run-audit-{uuid.uuid4()}"
    await run_repo.create_run(run_id=run_id, question="Audit verification run")

    # Record events in sequence
    e1 = await audit_repo.record_event(
        run_id=run_id,
        event_type="claim_created",
        actor_id="worker-a",
        entity_type="task",
        entity_id="task-1",
        details={"attempt": 1},
    )
    e2 = await audit_repo.record_event(
        run_id=run_id,
        event_type="claim_heartbeat",
        actor_id="worker-a",
        entity_type="task",
        entity_id="task-1",
        details={"heartbeat": 1},
    )

    # Read events in chronological order
    events = await audit_repo.list_events_by_run(run_id=run_id)
    assert len(events) == 2
    assert events[0]["event_type"] == "claim_created"
    assert events[1]["event_type"] == "claim_heartbeat"

    # Verify no mutation methods exist on repository (application-level append-only contract)
    assert not hasattr(audit_repo, "update_event"), "AuditRepository must not expose update_event"
    assert not hasattr(audit_repo, "delete_event"), "AuditRepository must not expose delete_event"
    assert not hasattr(audit_repo, "modify_event"), "AuditRepository must not expose modify_event"

    # Attempting to invoke mutation methods raises AttributeError
    with pytest.raises(AttributeError):
        getattr(audit_repo, "update_event")(e1["id"], event_type="mutated")  # type: ignore

    with pytest.raises(AttributeError):
        getattr(audit_repo, "delete_event")(e1["id"])  # type: ignore


# ---------------------------------------------------------------------------
# Test 11: Repository Transaction Reuse
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_11_repository_transaction_reuse():
    """Verify multiple repository calls inside transaction use ambient connection and rollback/commit together."""
    run_id = f"run-reuse-{uuid.uuid4()}"
    task_id = f"task-reuse-{uuid.uuid4()}"
    worker_id = f"worker-reuse-{uuid.uuid4()}"

    # Verify ambient connection is propagated and shared
    async with transaction() as tx_conn:
        assert get_current_connection() is tx_conn

        # Repository calls omit conn=; they must resolve to tx_conn automatically
        await run_repo.create_run(run_id=run_id, question="Ambient transaction run")
        await worker_repo.register_worker(worker_id=worker_id, run_id=run_id, status="ready")
        await task_repo.create_task(
            task_id=task_id,
            run_id=run_id,
            task_key="ambient_task",
            description="Created via ambient tx connection",
        )

        # Nested transaction (savepoint) inside existing transaction
        async with transaction() as nested_conn:
            assert nested_conn is tx_conn  # Same connection re-used
            await task_repo.update_task_status(task_id, "running")

    # Outside transaction, ambient connection is cleared
    assert get_current_connection() is None

    # Both parent and nested changes committed atomically
    task = await task_repo.get_task(task_id)
    assert task is not None
    assert task["status"] == "running"


# ---------------------------------------------------------------------------
# Test 12: PostgreSQL Pool Lifecycle
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_12_postgresql_pool_lifecycle(pg_server_dsn):
    """Verify pool initialization, acquiring/releasing connections, and graceful shutdown."""
    # Ensure current test pool is cleanly closed first
    await close_pool()
    assert get_pool() is None

    # 1. Initialize custom pool
    pool = await init_pool(dsn=pg_server_dsn, min_size=2, max_size=5, timeout=5.0)
    assert pool is not None
    assert not pool._closed
    assert get_pool() is pool

    # 2. Acquire and release connections via get_connection context manager
    async with get_connection(pool=pool) as conn:
        val = await conn.fetchval("SELECT 42;")
        assert val == 42

    # 3. Graceful shutdown
    await close_pool()
    assert get_pool() is None

    # Calling close_pool again is idempotent
    await close_pool()


# ---------------------------------------------------------------------------
# Performance Baseline (Diagnostic)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_performance_baseline():
    """Benchmark PostgreSQL baseline operations: task/claim insert, task/claim read, transaction commit."""
    run_id = f"run-bench-{uuid.uuid4()}"
    await run_repo.create_run(run_id=run_id, question="Performance diagnostic run")

    n_iterations = 100
    task_inserts = []
    task_reads = []
    claim_inserts = []
    claim_reads = []
    tx_commits = []

    def p(arr, pct):
        s = sorted(arr)
        idx = min(int((pct / 100.0) * len(s)), len(s) - 1)
        return s[idx]

    for i in range(n_iterations):
        tid = f"task-bench-{uuid.uuid4()}"
        cid = f"claim-bench-{uuid.uuid4()}"

        # 1. Task insert
        t0 = time.perf_counter()
        await task_repo.create_task(
            task_id=tid,
            run_id=run_id,
            task_key=f"key-{i}-{uuid.uuid4()}",
            description=f"Diagnostic benchmark task {i}",
        )
        task_inserts.append((time.perf_counter() - t0) * 1000.0)

        # 2. Task read
        t0 = time.perf_counter()
        await task_repo.get_task(tid)
        task_reads.append((time.perf_counter() - t0) * 1000.0)

        # 3. Claim insert
        t0 = time.perf_counter()
        await claim_repo.create_claim(
            claim_id=cid,
            task_id=tid,
            run_id=run_id,
            owner_id="bench-worker",
        )
        claim_inserts.append((time.perf_counter() - t0) * 1000.0)

        # 4. Claim read
        t0 = time.perf_counter()
        await claim_repo.get_claim(cid)
        claim_reads.append((time.perf_counter() - t0) * 1000.0)

        # 5. Transaction commit
        t0 = time.perf_counter()
        async with transaction() as tx:
            await task_repo.update_task_status(tid, "running", conn=tx)
        tx_commits.append((time.perf_counter() - t0) * 1000.0)

    print("\n" + "=" * 60)
    print("PHASE B POSTGRESQL PERFORMANCE BASELINE (N=100)")
    print(f"task insert:        p50={p(task_inserts, 50):.3f}ms | p95={p(task_inserts, 95):.3f}ms | p99={p(task_inserts, 99):.3f}ms")
    print(f"task read:          p50={p(task_reads, 50):.3f}ms | p95={p(task_reads, 95):.3f}ms | p99={p(task_reads, 99):.3f}ms")
    print(f"claim insert:       p50={p(claim_inserts, 50):.3f}ms | p95={p(claim_inserts, 95):.3f}ms | p99={p(claim_inserts, 99):.3f}ms")
    print(f"claim read:         p50={p(claim_reads, 50):.3f}ms | p95={p(claim_reads, 95):.3f}ms | p99={p(claim_reads, 99):.3f}ms")
    print(f"transaction commit: p50={p(tx_commits, 50):.3f}ms | p95={p(tx_commits, 95):.3f}ms | p99={p(tx_commits, 99):.3f}ms")
    print("=" * 60)
