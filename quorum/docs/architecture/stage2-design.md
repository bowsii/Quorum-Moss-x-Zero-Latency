# Quorum Stage 2 Architecture Design: Authoritative Transactional State

## 1. Executive Summary & Problem Statement

Stage 1 established the core Quorum coordination invariant:
```text
SENSE → CLAIM → CLAIM VALIDATION / TIEBREAK → EXTERNAL_WORK_ALLOWED → EXPENSIVE_EXTERNAL_WORK → PUBLISH_FINDING
```

Within a single process and event loop, Stage 1 proved:
- Exactly-one ownership under a 100-agent race (`1 owner`, `99 absorbed`, `1 external execution`).
- Zero unauthorized external calls through the external work firewall (`UnapprovedExternalWorkError`).
- In-process coordination lock protection via `_claims_lock` (`asyncio.Lock`).

However, an `asyncio.Lock` is strictly process-local. It does not provide atomicity, mutual exclusion, or state durability across independent operating system processes (`Process A`, `Process B`, `Process C`). If multiple independent processes run concurrently, an in-process lock cannot prevent split-brain ownership or duplicated external executions.

Stage 2 establishes a durable, transactional foundation across distributed processes.

**Phase B establishes the authoritative PostgreSQL persistence foundation** that later phases will use for distributed coordination.

---

## 2. Architectural Separation of Concerns

Quorum enforces a strict boundary between semantic candidate discovery and authoritative transactional ownership:

```text
                    ┌─────────────────────────┐
                    │       PostgreSQL        │
                    │                         │
                    │ AUTHORITATIVE STAGE 2   │
                    │ COORDINATION STATE      │
                    │                         │
                    │ runs                    │
                    │ tasks                   │
                    │ claims                  │
                    │ workers                 │
                    │ findings metadata       │
                    │ idempotency records     │
                    │ audit events            │
                    └────────────┬────────────┘
                                 │
                                 │ authoritative state
                                 ▼
                    ┌─────────────────────────┐
                    │   Coordination Layer    │
                    └────────────┬────────────┘
                                 │
                                 │ candidate retrieval
                                 ▼
                    ┌─────────────────────────┐
                    │        ChromaDB         │
                    │                         │
                    │ SEMANTIC INDEX ONLY     │
                    │                         │
                    │ "What may be related?"  │
                    └─────────────────────────┘
```

- **PostgreSQL answers**: *"Who authoritatively owns this task, for what duration, under what lease version, and with what lifecycle state?"*
- **ChromaDB answers**: *"What existing claims or findings might be semantically related?"*

### Critical Safety Invariants
1. Chroma **NEVER** decides ownership.
2. Chroma **NEVER** grants ownership or issues a `CoordinationToken`.
3. Chroma is **NEVER** the authoritative claim store.
4. If Chroma is unavailable, stale, or partitioned, PostgreSQL guarantees mutual exclusion and safety.

---

## 3. Authoritative State

PostgreSQL is the single authoritative source of truth for Stage 2 coordination entities, established via migration `001_initial_authoritative_state.sql`:

1. **`runs`**:
   - Fields: `id` (PK), `question`, `status`, `metadata` (JSONB), `created_at`, `updated_at`.
   - Check constraint on `status IN ('created', 'running', 'completed', 'failed', 'cancelled')`.
   - Serves as the coordination boundary for tasks and claims.
2. **`tasks`**:
   - Fields: `id` (PK), `run_id` (FK), `task_key`, `description`, `status`, `metadata` (JSONB), `created_at`, `updated_at`.
   - Check constraint on `status IN ('queued', 'running', 'retrying', 'completed', 'failed', 'cancelled', 'dead_lettered')`.
   - Unique constraint: `UNIQUE(run_id, task_key)` ensuring deterministic canonical identity per run.
3. **`claims`**:
   - Fields: `id` (PK), `task_id` (FK), `run_id` (FK), `owner_id`, `owner_type`, `status`, `lease_id`, `lease_version`, `expires_at`, `created_at`, `updated_at`.
   - Check constraints: `status IN ('active', 'superseded', 'expired', 'released', 'completed')`, `owner_type IN ('agent', 'human', 'adjudicator', 'reaper')`, and `lease_version >= 1`.
   - Designed to support future lease fencing in Phase C/D.
4. **`workers`**:
   - Fields: `id` (PK), `run_id` (FK nullable), `worker_type`, `status`, `last_heartbeat_at`, `created_at`, `updated_at`.
   - Check constraint: `status IN ('starting', 'ready', 'busy', 'draining', 'stopped', 'failed')`.
5. **`findings`**:
   - Fields: `id` (PK), `task_id` (FK nullable), `run_id` (FK), `claim_id` (FK nullable), `participant_id`, `participant_type`, `title`, `content_hash`, `sources` (JSONB), `supersedes`, `created_at`, `updated_at`.
   - Stores authoritative metadata and sha256 content hashes. (Semantic embeddings remain in Chroma).
6. **`idempotency_records`**:
   - Fields: `id` (PK), `idempotency_key`, `scope`, `status`, `payload` (JSONB), `expires_at`, `created_at`, `updated_at`.
   - Unique constraint: `UNIQUE(idempotency_key, scope)`.
7. **`audit_events`**:
   - Fields: `id` (BIGSERIAL PK), `run_id` (FK), `actor_id`, `event_type`, `entity_type`, `entity_id`, `details` (JSONB), `created_at`.
   - Append-only coordination audit log.

---

## 4. Semantic State

ChromaDB operates strictly as an approximate nearest-neighbor vector search index:
- **Namespace isolation**: Collections `quorum_claims` and `quorum_findings`.
- **Candidate boundary**: Mediated exclusively via `quorum/bus/candidate_retrieval.py` (`find_semantic_candidates()`).
- **Data returned**: Document candidates, cosine distances, and normalized similarity scores.
- **Coordination protocol**: The coordination layer queries Chroma to identify candidate collisions, but **all claim creation, lease validation, and status checks query PostgreSQL**. A candidate hit from Chroma does not confer ownership or authorization.

---

## 5. SQLite Transitional Responsibilities

To preserve 100% backward compatibility with Stage 1 without breaking running APIs or test suites:
- **SQLite remains responsible for**:
  - Stage 1 API compatibility (`POST /runs`, `GET /runs/{id}`, `DELETE /runs/{id}`).
  - Legacy run tracking (`runs` table in `quorum.db`).
  - Participant consent records (`consent_events`).
  - Session replay ordered action log (`replay_logs`).
  - Telemetry suppression records (`telemetry_tombstones`).
  - Right-to-erasure GDPR cascading workflow (`erase_run` in `quorum/db/erasure.py`).
- **PostgreSQL becomes responsible for**:
  - Stage 2 distributed coordination entities (`runs`, `tasks`, `claims`, `workers`, `findings`, `idempotency_records`, `audit_events`).
- **No Competing Authority**:
  During Phase B, SQLite and PostgreSQL do NOT compete for the same authority:
  - SQLite serves the existing Stage 1 HTTP API and right-to-erasure workflows.
  - PostgreSQL serves the Stage 2 authoritative coordination state.
  - Consolidation of the run lifecycle will take place deliberately in later phases after cross-process claim arbitration (Phase C) is proven.

---

## 6. Transaction Boundary

The transaction abstraction is defined in `quorum/db/transaction.py`:
- **Context Manager**: `async with transaction(isolation="read_committed", pool=None) as tx_conn:`
- **Commit / Rollback**: Automatically commits on successful exit of the block; automatically rolls back if an exception is raised, ensuring zero partial state remains.
- **Ambient Connection Propagation**: Uses `contextvars.ContextVar` to propagate the active transaction connection down the asyncio task call stack. Repositories resolve connections via `resolve_connection()`, automatically reusing the ambient transaction connection without opening a redundant connection.
- **Nested Transactions & Savepoints**: When `transaction()` is entered while an ambient transaction is already active, it automatically nests using an asyncpg transaction savepoint (`SAVEPOINT`).
- **Row-Level Locking**: `claim_repo.get_active_claim_for_task(task_id, for_update=True)` executes `SELECT ... FOR UPDATE`, holding an exclusive row lock for the duration of the transaction. Independent connections attempting concurrent row locks wait or raise `LockNotAvailableError` if `NOWAIT` is requested.

---

## 7. Connection Lifecycle

PostgreSQL connection management is implemented in `quorum/db/connection.py`:
- **Driver**: `asyncpg>=0.29.0`.
- **Pooling**: Global async connection pool managed via `init_pool()` and `close_pool()`.
- **Configuration**:
  - Configurable pool limits (default: `min_size=2`, `max_size=10`).
  - Connection attempt timeout (default: `5.0s`).
  - Command timeout (default: `60.0s`).
- **Fail-Closed Behavior**:
  If PostgreSQL is unreachable or connection pool checkout fails, the layer raises `DatabaseUnavailableError`.
  Under this failure mode:
  - NO authoritative claim is created.
  - NO ownership is approved.
  - NO `CoordinationToken` is granted.
  - NO external work is permitted.
  The system fails closed and never converts database failure into claim acceptance.
- **Cleanup**: `close_pool()` safely terminates pool connections and clears references deterministically.

---

## 8. Migration Strategy

Schema migrations are managed by `quorum/db/migrations/runner.py`:
- **Deterministic Discovery**: Scans `quorum/db/migrations/` for files matching `^(\d+)_(.+)\.sql$` and sorts them numerically in ascending order.
- **Tracking Table**: `schema_migrations` records `(version, name, applied_at)`. Only pending migrations are executed.
- **Transactional Application**: Each SQL migration file executes within an isolated database transaction, recording its version in `schema_migrations` upon commit.
- **Idempotency**: Executing `apply_migrations()` against an up-to-date database is a no-op and returns an empty list.
- **Concurrent Migration Safety**: To prevent race conditions when multiple independent application or worker processes boot simultaneously, the runner acquires a PostgreSQL session-level advisory lock (`pg_advisory_lock(82749182)`). Only one process can execute migration checks and apply DDL at a time; all other processes wait and subsequently detect that migrations have already been applied.

---

## 9. Current Limitations

Phase B establishes strictly the persistence foundation. In compliance with strict phase boundaries, the following capabilities are deliberately **not** implemented in this phase:

```text
Cross-process claim atomicity:
NOT IMPLEMENTED

Distributed lease arbitration:
NOT IMPLEMENTED

Fencing enforcement:
NOT IMPLEMENTED

Durable queue:
NOT IMPLEMENTED

Worker crash recovery:
NOT IMPLEMENTED
```

These capabilities belong to subsequent phases:
- **Phase C**: Cross-process atomic claim acquisition and tiebreak arbitration.
- **Phase D**: Distributed lease heartbeating and fencing enforcement (`LeaseFencedError`).
- **Phase E**: Durable task queueing and delivery.
- **Phase F**: Worker crash recovery and Reaper integration with PostgreSQL.
