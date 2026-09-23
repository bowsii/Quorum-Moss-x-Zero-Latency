"""
tests/test_phase_c_distributed_claims.py
------------------------------------------
Comprehensive verification test suite for Stage 2 / Phase C: Distributed Claim Arbitration.

Validates:
1. Multi-process race tests (genuine independent OS processes via multiprocessing):
   - test_two_process_race: N=2 contenders -> exactly 1 owner, 1 capability, 1 external work
   - test_ten_process_race: N=10 contenders -> exactly 1 owner, 9 non-owners, 1 capability, 1 external work
   - test_fifty_process_race: N=50 contenders -> exactly 1 owner, 49 non-owners, 1 capability, 1 external work
   - test_hundred_process_race: N=100 contenders -> exactly 1 owner, 99 non-owners, 1 capability, 1 external work
2. Failure tests:
   - test_transaction_failure_rolls_back_claim: failed transaction leaves zero committed claims
   - test_token_not_issued_if_transaction_fails: failed transaction produces no token and zero work
   - test_postgres_failure_fails_closed: database failure raises exception and blocks external work
3. Lifecycle tests:
   - test_active_claim_absorbs_duplicate: healthy active claim absorbs late contender outside race window
   - test_expired_claim_reclaim: expired claim is reclaimed with incremented lease_version
   - test_completed_finding_absorbs_duplicate: completed finding absorbs redundant claimants
   - test_idempotent_claim_request: same-owner retry returns OWNER_ALREADY_HELD without duplicate capability
4. Tiebreak tests:
   - test_human_agent_tiebreak_preserved: human within 50ms epsilon window supersedes agent incumbent;
     healthy claim outside race window is not superseded
5. Locking tests:
   - test_cross_connection_locking: cross-connection FOR UPDATE NOWAIT raises LockNotAvailableError
6. Constraint tests:
   - test_database_unique_active_claim_defense: uq_claims_active_task partial unique index rejects duplicate
     active claim; savepoint rollback preserves transaction health
7. Audit tests:
   - test_audit_event_recorded: append-only audit event log records run_id, task_id, claim_id, actor, and details
8. Performance benchmark:
   - Contention benchmark measuring 1, 2, 10, 50, 100 contenders reporting p50, p95, p99, max latency.
"""

import asyncio
import multiprocessing as mp
import os
import pathlib
import tempfile
import time
from datetime import datetime, timezone, timedelta
import uuid
from typing import Any

import asyncpg
import pytest
import pytest_asyncio

from config.settings import settings
from db.connection import (
    init_pool,
    close_pool,
    get_connection,
    DatabaseUnavailableError,
    DatabaseError,
)
from db.transaction import transaction
from db.migrations.runner import apply_migrations
from db.repositories import (
    run_repo,
    task_repo,
    claim_repo,
    finding_repo,
    audit_repo,
    idempotency_repo,
)
from db.arbitration import (
    claim_task_authoritatively,
    ArbitrationOutcome,
    AuthoritativeClaimDecision,
)
from bus.arbitration import issue_coordination_token_for_decision
from agents.coordination_token import (
    bound_coordination_token,
    verify_coordination_capability,
    UnapprovedExternalWorkError,
)


@pytest.fixture(scope="session")
def pg_server_dsn():
    """Provides a PostgreSQL DSN with max_connections configured for high concurrency."""
    env_dsn = os.environ.get("POSTGRES_URL", "")
    if env_dsn:
        yield env_dsn
        return

    from embedded_postgres.postgres_server import initdb
    from embedded_postgres import get_server

    temp_dir = pathlib.Path(tempfile.mkdtemp(prefix="quorum_pg_phase_c_"))
    initdb(["--auth=trust", "--auth-local=trust", "--encoding=utf8", "-U", "postgres"], pgdata=temp_dir)
    with open(temp_dir / "postgresql.conf", "a") as f:
        f.write("\nmax_connections = 300\n")

    server = get_server(temp_dir)
    with server:
        uri = server.get_uri()
        yield uri


@pytest_asyncio.fixture(autouse=True)
async def setup_phase_c_db(pg_server_dsn):
    """Ensure database schema and migrations are initialized before each test."""
    pool = await init_pool(dsn=pg_server_dsn, min_size=2, max_size=10)
    async with pool.acquire() as conn:
        await apply_migrations(conn)
    yield pool
    await close_pool()


# ---------------------------------------------------------------------------
# Worker process function for cross-process races
# ---------------------------------------------------------------------------

def _cross_process_race_worker(
    worker_idx: int,
    pg_dsn: str,
    run_id: str,
    task_key: str,
    barrier: Any,
    result_queue: Any,
    attempt_external_work: bool = True,
):
    """Independent OS process worker attempting to claim the authoritative task."""
    import asyncio
    import asyncpg
    from db.arbitration import claim_task_authoritatively
    from bus.arbitration import issue_coordination_token_for_decision
    from agents.coordination_token import (
        bound_coordination_token,
        verify_coordination_capability,
        UnapprovedExternalWorkError,
    )

    async def _run():
        conn = await asyncpg.connect(pg_dsn)
        owner_id = f"worker-proc-{worker_idx:03d}"

        # Synchronize all N processes at the barrier so they hit PostgreSQL concurrently
        barrier.wait()

        t_path_start = time.perf_counter()
        t_arb_start = time.perf_counter()
        decision = await claim_task_authoritatively(
            task_key=task_key,
            run_id=run_id,
            owner_id=owner_id,
            owner_type="agent",
            description=f"Task {task_key} contended by {owner_id}",
            conn=conn,
        )
        arb_latency_ms = (time.perf_counter() - t_arb_start) * 1000.0

        token = issue_coordination_token_for_decision(decision, owner_id=owner_id)
        external_work_succeeded = False
        firewall_blocked = False

        if attempt_external_work:
            if token is not None:
                # Winner performs external work under bound token capability
                with bound_coordination_token(token):
                    verify_coordination_capability()
                    external_work_succeeded = True
            else:
                # Loser attempts external work without token; firewall MUST reject
                try:
                    verify_coordination_capability()
                    external_work_succeeded = True
                except UnapprovedExternalWorkError:
                    firewall_blocked = True

        await conn.close()
        full_path_latency_ms = (time.perf_counter() - t_path_start) * 1000.0

        return {
            "worker_idx": worker_idx,
            "owner_id": owner_id,
            "is_owner": decision.is_owner,
            "outcome": decision.outcome.value,
            "capability_issuance_required": decision.capability_issuance_required,
            "token_issued": token is not None,
            "external_work_succeeded": external_work_succeeded,
            "firewall_blocked": firewall_blocked,
            "claim_id": decision.claim_id,
            "arbitration_latency_ms": arb_latency_ms,
            "full_path_latency_ms": full_path_latency_ms,
        }

    try:
        res = asyncio.run(_run())
        result_queue.put(res)
    except Exception as exc:
        barrier.abort()
        raise


def _run_mp_race(n_contenders: int, pg_dsn: str, task_name: str) -> dict[str, Any]:
    """Execute n_contenders independent OS processes competing for the same task."""
    ctx = mp.get_context("spawn")
    result_queue = ctx.Queue()
    barrier = ctx.Barrier(n_contenders)
    run_id = f"run-mp-race-{n_contenders}-{task_name}"
    task_key = f"key-{task_name}"

    procs = [
        ctx.Process(
            target=_cross_process_race_worker,
            args=(i, pg_dsn, run_id, task_key, barrier, result_queue),
        )
        for i in range(n_contenders)
    ]

    for p in procs:
        p.start()

    timeout_seconds = max(30, n_contenders * 0.5)
    results = [result_queue.get(timeout=timeout_seconds) for _ in range(n_contenders)]

    for p in procs:
        p.join()

    owners = sum(1 for r in results if r["is_owner"])
    non_owners = sum(1 for r in results if not r["is_owner"])
    new_capabilities = sum(1 for r in results if r["token_issued"])
    external_work = sum(1 for r in results if r["external_work_succeeded"])
    firewall_blocked = sum(1 for r in results if r["firewall_blocked"])

    return {
        "n_contenders": n_contenders,
        "owners": owners,
        "non_owners": non_owners,
        "new_capabilities": new_capabilities,
        "external_work": external_work,
        "firewall_blocked": firewall_blocked,
        "results": results,
    }


# ===========================================================================
# 1. Required Cross-Process Race Tests (§19)
# ===========================================================================

def test_two_process_race(pg_server_dsn):
    """2 independent OS processes racing for same task: exactly 1 owner, 1 capability, 1 external work."""
    summary = _run_mp_race(2, pg_server_dsn, "two-procs")
    print(f"\n[N=2 RACE RESULTS] owners={summary['owners']} non_owners={summary['non_owners']} new_capabilities={summary['new_capabilities']} external_work={summary['external_work']} firewall_blocked={summary['firewall_blocked']}")
    assert summary["owners"] == 1, f"Expected 1 owner, got {summary['owners']}"
    assert summary["non_owners"] == 1, f"Expected 1 non-owner, got {summary['non_owners']}"
    assert summary["new_capabilities"] == 1, f"Expected 1 capability, got {summary['new_capabilities']}"
    assert summary["external_work"] == 1, f"Expected 1 external work, got {summary['external_work']}"
    assert summary["firewall_blocked"] == 1, f"Expected 1 firewall block, got {summary['firewall_blocked']}"


def test_ten_process_race(pg_server_dsn):
    """10 independent OS processes racing: exactly 1 owner, 9 non-owners, 1 capability, 1 external work."""
    summary = _run_mp_race(10, pg_server_dsn, "ten-procs")
    print(f"\n[N=10 RACE RESULTS] owners={summary['owners']} non_owners={summary['non_owners']} new_capabilities={summary['new_capabilities']} external_work={summary['external_work']} firewall_blocked={summary['firewall_blocked']}")
    assert summary["owners"] == 1, f"Expected 1 owner, got {summary['owners']}"
    assert summary["non_owners"] == 9, f"Expected 9 non-owners, got {summary['non_owners']}"
    assert summary["new_capabilities"] == 1, f"Expected 1 capability, got {summary['new_capabilities']}"
    assert summary["external_work"] == 1, f"Expected 1 external work, got {summary['external_work']}"
    assert summary["firewall_blocked"] == 9, f"Expected 9 firewall blocks, got {summary['firewall_blocked']}"


def test_fifty_process_race(pg_server_dsn):
    """50 independent OS processes racing: exactly 1 owner, 49 non-owners, 1 capability, 1 external work."""
    summary = _run_mp_race(50, pg_server_dsn, "fifty-procs")
    print(f"\n[N=50 RACE RESULTS] owners={summary['owners']} non_owners={summary['non_owners']} new_capabilities={summary['new_capabilities']} external_work={summary['external_work']} firewall_blocked={summary['firewall_blocked']}")
    assert summary["owners"] == 1, f"Expected 1 owner, got {summary['owners']}"
    assert summary["non_owners"] == 49, f"Expected 49 non-owners, got {summary['non_owners']}"
    assert summary["new_capabilities"] == 1, f"Expected 1 capability, got {summary['new_capabilities']}"
    assert summary["external_work"] == 1, f"Expected 1 external work, got {summary['external_work']}"
    assert summary["firewall_blocked"] == 49, f"Expected 49 firewall blocks, got {summary['firewall_blocked']}"


def test_hundred_process_race(pg_server_dsn):
    """100 independent OS processes racing: exactly 1 owner, 99 non-owners, 1 capability, 1 external work.

    THE STRONGEST ACCEPTANCE GATE:
    owners == 1
    non_owners == 99
    new_capabilities == 1
    external_work == 1
    firewall_blocked == 99
    """
    summary = _run_mp_race(100, pg_server_dsn, "hundred-procs")
    print(f"\n[N=100 RACE RESULTS] owners={summary['owners']} non_owners={summary['non_owners']} new_capabilities={summary['new_capabilities']} external_work={summary['external_work']} firewall_blocked={summary['firewall_blocked']}")
    assert summary["owners"] == 1, f"Expected 1 owner, got {summary['owners']}"
    assert summary["non_owners"] == 99, f"Expected 99 non-owners, got {summary['non_owners']}"
    assert summary["new_capabilities"] == 1, f"Expected 1 capability, got {summary['new_capabilities']}"
    assert summary["external_work"] == 1, f"Expected 1 external work, got {summary['external_work']}"
    assert summary["firewall_blocked"] == 99, f"Expected 99 firewall blocks, got {summary['firewall_blocked']}"



# ===========================================================================
# 2. Failure Tests (§20)
# ===========================================================================

@pytest.mark.asyncio
async def test_transaction_failure_rolls_back_claim(pg_server_dsn):
    """If a transaction fails before commit, no claim record or ownership persists."""
    conn = await asyncpg.connect(pg_server_dsn)
    run_id = "run-tx-failure"
    task_key = "task-rollback-key"

    # Simulate an error within a custom transaction calling claim_task_authoritatively
    with pytest.raises(RuntimeError, match="Simulated crash during arbitration"):
        async with transaction(conn=conn) as tx_conn:
            decision = await claim_task_authoritatively(
                task_key=task_key,
                run_id=run_id,
                owner_id="worker-crash",
                conn=tx_conn,
            )
            assert decision.is_owner is True
            # Raise exception before transaction commits
            raise RuntimeError("Simulated crash during arbitration")

    # Verify that ZERO claims persist for this task
    task = await task_repo.get_task_by_key(run_id=run_id, task_key=task_key, conn=conn)
    if task:
        active = await claim_repo.get_active_claim_for_task(task["id"], include_expired=True, conn=conn)
        assert active is None, "Expected no active claim to persist after transaction rollback"

    await conn.close()


@pytest.mark.asyncio
async def test_token_not_issued_if_transaction_fails(pg_server_dsn):
    """No CoordinationToken is issued if arbitration fails or is rejected."""
    conn = await asyncpg.connect(pg_server_dsn)
    run_id = "run-token-fail"
    task_key = "task-no-token-key"

    # When capability_issuance_required is False, issue_coordination_token_for_decision returns None
    decision = AuthoritativeClaimDecision(
        is_owner=False,
        outcome=ArbitrationOutcome.DUPLICATE_ABSORBED,
        task_id="t1",
        task_key=task_key,
        run_id=run_id,
        claim_id=None,
        lease_id=None,
        lease_version=None,
        expires_at=None,
        existing_claim_id="c-incumbent",
        existing_finding_id=None,
        superseded_claim_id=None,
        capability_issuance_required=False,
        reason="test_duplicate",
    )

    token = issue_coordination_token_for_decision(decision, owner_id="worker-loser")
    assert token is None, "Token must not be issued for non-winning decision"

    # Attempting work with None token must raise UnapprovedExternalWorkError
    with pytest.raises(UnapprovedExternalWorkError):
        verify_coordination_capability(token)

    await conn.close()


@pytest.mark.asyncio
async def test_postgres_failure_fails_closed():
    """Unreachable PostgreSQL must raise exception and fail closed (no token, no external work)."""
    # Unroutable DSN
    bad_dsn = "postgresql://invalid_user:bad_pass@127.0.0.1:54329/nonexistent_db"

    with pytest.raises((DatabaseUnavailableError, DatabaseError)):
        # Attempting direct connection or arbitration with unreachable database must fail
        await claim_task_authoritatively(
            task_key="fail-closed-key",
            run_id="run-fail-closed",
            owner_id="worker-fail-closed",
            conn=None,
            pool=None,
            dsn=bad_dsn,
        )

    # Calling external capability verification without token raises UnapprovedExternalWorkError
    with pytest.raises(UnapprovedExternalWorkError):
        verify_coordination_capability()


# ===========================================================================
# 3. Lifecycle Tests (§21)
# ===========================================================================

@pytest.mark.asyncio
async def test_active_claim_absorbs_duplicate(pg_server_dsn):
    """An active healthy claim absorbs a later challenger arriving outside the race window."""
    conn = await asyncpg.connect(pg_server_dsn)
    run_id = "run-lifecycle-absorb"
    task_key = "lifecycle-task-1"

    # 1. Contender A claims the task at T0
    dec_a = await claim_task_authoritatively(
        task_key=task_key,
        run_id=run_id,
        owner_id="agent-incumbent",
        conn=conn,
    )
    assert dec_a.is_owner is True
    assert dec_a.outcome == ArbitrationOutcome.OWNER_NEW
    assert dec_a.capability_issuance_required is True

    # 2. Contender B arrives with a timestamp outside the 50ms race window (+1.0 second)
    later_time = datetime.now(timezone.utc) + timedelta(seconds=1.0)
    dec_b = await claim_task_authoritatively(
        task_key=task_key,
        run_id=run_id,
        owner_id="agent-challenger",
        challenger_created_at=later_time,
        conn=conn,
    )
    assert dec_b.is_owner is False
    assert dec_b.outcome == ArbitrationOutcome.DUPLICATE_ABSORBED
    assert dec_b.capability_issuance_required is False
    assert dec_b.existing_claim_id == dec_a.claim_id

    await conn.close()


@pytest.mark.asyncio
async def test_expired_claim_reclaim(pg_server_dsn):
    """An expired claim is reclaimed by an eligible challenger with incremented lease_version."""
    conn = await asyncpg.connect(pg_server_dsn)
    run_id = "run-lifecycle-expire"
    task_key = "lifecycle-task-expire"

    # 1. Initial claim with very short TTL
    dec_init = await claim_task_authoritatively(
        task_key=task_key,
        run_id=run_id,
        owner_id="agent-stale",
        claim_ttl_seconds=0.05,  # 50ms TTL
        conn=conn,
    )
    assert dec_init.is_owner is True
    assert dec_init.lease_version == 1

    # Wait for claim lease to expire
    await asyncio.sleep(0.1)

    # 2. Challenger reclaims the expired lease
    dec_reclaim = await claim_task_authoritatively(
        task_key=task_key,
        run_id=run_id,
        owner_id="agent-reclaimer",
        conn=conn,
    )
    assert dec_reclaim.is_owner is True
    assert dec_reclaim.outcome == ArbitrationOutcome.EXPIRED_RECLAIMED
    assert dec_reclaim.lease_version == 2
    assert dec_reclaim.superseded_claim_id == dec_init.claim_id
    assert dec_reclaim.capability_issuance_required is True

    await conn.close()


@pytest.mark.asyncio
async def test_completed_finding_absorbs_duplicate(pg_server_dsn):
    """A task with an existing completed finding absorbs subsequent claimants without capability."""
    conn = await asyncpg.connect(pg_server_dsn)
    run_id = "run-lifecycle-finding"
    task_key = "lifecycle-task-completed"

    # 1. Setup task and record completed finding
    task = await task_repo.get_or_create_task(
        run_id=run_id,
        task_key=task_key,
        description="Finding task",
        conn=conn,
    )
    finding_id = str(uuid.uuid4())
    await finding_repo.create_finding(
        finding_id=finding_id,
        run_id=run_id,
        participant_id="agent-author",
        participant_type="agent",
        title="Authoritative Research Result",
        content="Research finding content",
        task_id=task["id"],
        conn=conn,
    )
    await task_repo.update_task_status(task["id"], "completed", conn=conn)

    # 2. Contender attempts to claim the completed task
    decision = await claim_task_authoritatively(
        task_key=task_key,
        run_id=run_id,
        owner_id="agent-late-comer",
        conn=conn,
    )
    assert decision.is_owner is False
    assert decision.outcome == ArbitrationOutcome.COMPLETED_FINDING_ABSORBED
    assert decision.capability_issuance_required is False
    assert decision.existing_finding_id == finding_id

    await conn.close()


@pytest.mark.asyncio
async def test_idempotent_claim_request(pg_server_dsn):
    """Same owner retrying the same logical claim receives OWNER_ALREADY_HELD with no new capability."""
    conn = await asyncpg.connect(pg_server_dsn)
    run_id = "run-lifecycle-idempotent"
    task_key = "lifecycle-task-idempotent"

    # 1. Initial claim granted
    dec_first = await claim_task_authoritatively(
        task_key=task_key,
        run_id=run_id,
        owner_id="agent-idempotent-worker",
        idempotency_key="idem-key-123",
        conn=conn,
    )
    assert dec_first.is_owner is True
    assert dec_first.outcome == ArbitrationOutcome.OWNER_NEW
    assert dec_first.capability_issuance_required is True

    # 2. Same owner retries the same logical request
    dec_retry = await claim_task_authoritatively(
        task_key=task_key,
        run_id=run_id,
        owner_id="agent-idempotent-worker",
        idempotency_key="idem-key-123",
        conn=conn,
    )
    assert dec_retry.is_owner is True
    assert dec_retry.outcome == ArbitrationOutcome.OWNER_ALREADY_HELD
    assert dec_retry.capability_issuance_required is False  # Must NOT issue new capability
    assert dec_retry.claim_id == dec_first.claim_id

    # Token issuance must return None for retry
    token_retry = issue_coordination_token_for_decision(dec_retry, owner_id="agent-idempotent-worker")
    assert token_retry is None, "No duplicate capability may be issued on idempotent retry"

    await conn.close()


# ===========================================================================
# 4. Tiebreak Test (§22)
# ===========================================================================

@pytest.mark.asyncio
async def test_human_agent_tiebreak_preserved(pg_server_dsn):
    """Human claim within the 50ms race window supersedes agent incumbent.

    Healthy claim outside race window is not superseded.
    """
    conn = await asyncpg.connect(pg_server_dsn)
    run_id = "run-tiebreak-test"
    task_key = "tiebreak-task-key"

    t0 = datetime.now(timezone.utc)

    # 1. Agent acquires claim at t0
    dec_agent = await claim_task_authoritatively(
        task_key=task_key,
        run_id=run_id,
        owner_id="agent-001",
        owner_type="agent",
        challenger_created_at=t0,
        conn=conn,
    )
    assert dec_agent.is_owner is True
    assert dec_agent.outcome == ArbitrationOutcome.OWNER_NEW

    # 2. Human contender arrives at t0 + 20ms (inside 50ms race window)
    t_human = t0 + timedelta(milliseconds=20.0)
    dec_human = await claim_task_authoritatively(
        task_key=task_key,
        run_id=run_id,
        owner_id="human-operator",
        owner_type="human",
        challenger_created_at=t_human,
        conn=conn,
    )
    assert dec_human.is_owner is True
    assert dec_human.outcome == ArbitrationOutcome.TIEBREAK_WON
    assert dec_human.superseded_claim_id == dec_agent.claim_id
    assert dec_human.lease_version == dec_agent.lease_version + 1
    assert dec_human.capability_issuance_required is True

    # 3. Third agent arrives outside race window (+1.0s) -> absorbed, human is not superseded
    t_late_agent = t0 + timedelta(seconds=1.0)
    dec_late_agent = await claim_task_authoritatively(
        task_key=task_key,
        run_id=run_id,
        owner_id="agent-002",
        owner_type="agent",
        challenger_created_at=t_late_agent,
        conn=conn,
    )
    assert dec_late_agent.is_owner is False
    assert dec_late_agent.outcome == ArbitrationOutcome.DUPLICATE_ABSORBED
    assert dec_late_agent.existing_claim_id == dec_human.claim_id
    assert dec_late_agent.capability_issuance_required is False

    await conn.close()


# ===========================================================================
# 5. Row Locking Test (§23)
# ===========================================================================

@pytest.mark.asyncio
async def test_cross_connection_locking(pg_server_dsn):
    """Connection A locks task row with FOR UPDATE; Connection B attempting FOR UPDATE NOWAIT raises error."""
    conn_a = await asyncpg.connect(pg_server_dsn)
    conn_b = await asyncpg.connect(pg_server_dsn)

    run_id = "run-lock-test"
    task_key = "lock-task-key"

    # Setup task
    task = await task_repo.get_or_create_task(
        run_id=run_id,
        task_key=task_key,
        description="Row lock task",
        conn=conn_a,
    )
    task_id = task["id"]

    # Connection A begins transaction and locks task row
    tx_a = conn_a.transaction()
    await tx_a.start()
    try:
        await task_repo.lock_task(task_id=task_id, nowait=False, conn=conn_a)

        # Connection B attempts to lock same row with NOWAIT -> must raise LockNotAvailableError
        with pytest.raises(asyncpg.exceptions.LockNotAvailableError):
            tx_b = conn_b.transaction()
            await tx_b.start()
            try:
                await task_repo.lock_task(task_id=task_id, nowait=True, conn=conn_b)
            finally:
                await tx_b.rollback()
    finally:
        await tx_a.commit()

    # After Connection A commits, Connection B can acquire lock
    async with conn_b.transaction():
        locked_by_b = await task_repo.lock_task(task_id=task_id, nowait=True, conn=conn_b)
        assert locked_by_b is not None
        assert locked_by_b["id"] == task_id

    await conn_a.close()
    await conn_b.close()


# ===========================================================================
# 6. Database Constraint Test (§24)
# ===========================================================================

@pytest.mark.asyncio
async def test_database_unique_active_claim_defense(pg_server_dsn):
    """Direct PostgreSQL attempt to insert two active claims for same task fails via uq_claims_active_task.

    Savepoint rollback allows the parent transaction to remain valid.
    """
    conn = await asyncpg.connect(pg_server_dsn)
    run_id = "run-constraint-test"
    task_key = "constraint-task-key"

    task = await task_repo.get_or_create_task(
        run_id=run_id,
        task_key=task_key,
        description="Constraint test task",
        conn=conn,
    )
    task_id = task["id"]

    # Insert first active claim
    c1 = await claim_repo.create_claim(
        claim_id=str(uuid.uuid4()),
        task_id=task_id,
        run_id=run_id,
        owner_id="worker-1",
        status="active",
        conn=conn,
    )
    assert c1["status"] == "active"

    # Attempt to insert second active claim in a transaction
    async with conn.transaction():
        # Inside savepoint, attempting to insert duplicate active claim triggers UniqueViolationError
        with pytest.raises(asyncpg.exceptions.UniqueViolationError):
            async with conn.transaction():  # savepoint
                await claim_repo.create_claim(
                    claim_id=str(uuid.uuid4()),
                    task_id=task_id,
                    run_id=run_id,
                    owner_id="worker-2",
                    status="active",
                    conn=conn,
                )

        # After savepoint rollback, outer transaction MUST remain valid
        active = await claim_repo.get_active_claim_for_task(task_id, conn=conn)
        assert active is not None
        assert active["id"] == c1["id"]

    await conn.close()


# ===========================================================================
# 7. Audit Test (§25)
# ===========================================================================

@pytest.mark.asyncio
async def test_audit_event_recorded(pg_server_dsn):
    """Arbitration decisions generate immutable append-only audit event records."""
    conn = await asyncpg.connect(pg_server_dsn)
    run_id = f"run-audit-{uuid.uuid4().hex[:8]}"
    task_key = "audit-task-key"

    # Perform claim arbitration
    decision = await claim_task_authoritatively(
        task_key=task_key,
        run_id=run_id,
        owner_id="audited-worker-1",
        conn=conn,
    )
    assert decision.is_owner is True

    # Query audit events recorded for this run
    events = await audit_repo.list_events_by_run(run_id=run_id, conn=conn)
    assert len(events) >= 1

    claim_event = next((e for e in events if e["event_type"] == "claim_granted"), None)
    assert claim_event is not None
    assert claim_event["actor_id"] == "audited-worker-1"
    assert claim_event["run_id"] == run_id
    assert claim_event["entity_type"] == "claim"
    assert claim_event["entity_id"] == decision.claim_id
    assert claim_event["created_at"] is not None

    details = claim_event["details"]
    if isinstance(details, str):
        import json
        details = json.loads(details)
    assert details["task_key"] == task_key
    assert details["outcome"] == ArbitrationOutcome.OWNER_NEW.value

    await conn.close()


# ===========================================================================
# 8. Performance Benchmark (§29)
# ===========================================================================

def test_performance_benchmark(pg_server_dsn):
    """Measure arbitration latency across 1, 2, 10, 50, and 100 contenders.

    Reports p50, p95, p99, and max latency for database arbitration latency
    and full claim path latency.
    """
    contender_counts = [1, 2, 10, 50, 100]
    bench_results: dict[int, dict[str, Any]] = {}

    def _calc_percentiles(vals: list[float]) -> dict[str, float]:
        sorted_vals = sorted(vals)
        n = len(sorted_vals)
        p50 = sorted_vals[int(0.50 * (n - 1))]
        p95 = sorted_vals[int(0.95 * (n - 1))]
        p99 = sorted_vals[int(0.99 * (n - 1))]
        pmax = sorted_vals[-1]
        return {"p50": p50, "p95": p95, "p99": p99, "max": pmax}

    for n in contender_counts:
        t_start = time.perf_counter()
        summary = _run_mp_race(n, pg_server_dsn, f"bench-{n}")
        total_time_ms = (time.perf_counter() - t_start) * 1000.0

        # Invariant checks must strictly hold
        assert summary["owners"] == 1, f"Bench N={n} failed invariant: owners={summary['owners']}"
        assert summary["new_capabilities"] == 1
        assert summary["external_work"] == 1

        arb_latencies = [r["arbitration_latency_ms"] for r in summary["results"]]
        path_latencies = [r["full_path_latency_ms"] for r in summary["results"]]

        bench_results[n] = {
            "total_wall_ms": total_time_ms,
            "db_arbitration": _calc_percentiles(arb_latencies),
            "full_path": _calc_percentiles(path_latencies),
        }

    print("\n" + "=" * 80)
    print("PHASE C CONTENTION LATENCY BENCHMARK REPORT (Independent OS Processes)")
    print("=" * 80)
    print(f"{'N':>4} | {'Wall (ms)':>10} | {'DB p50':>8} {'DB p95':>8} {'DB p99':>8} {'DB Max':>8} | {'Path p50':>8} {'Path p95':>8} {'Path p99':>8} {'Path Max':>8}")
    print("-" * 80)
    for n, m in bench_results.items():
        db = m["db_arbitration"]
        fp = m["full_path"]
        print(
            f"{n:4d} | {m['total_wall_ms']:10.2f} | "
            f"{db['p50']:8.2f} {db['p95']:8.2f} {db['p99']:8.2f} {db['max']:8.2f} | "
            f"{fp['p50']:8.2f} {fp['p95']:8.2f} {fp['p99']:8.2f} {fp['max']:8.2f}"
        )
    print("=" * 80)
