# Quorum Stage 2 Architecture Design: Durable Cross-Process Coordination

## 1. Executive Summary & Problem Statement

Stage 1 established an in-process coordination contract (`SENSE → CLAIM → CLAIM VALIDATION → TIEBREAK → EXTERNAL_ALLOWED → EXPENSIVE_EXTERNAL_WORK → PUBLISH_FINDING`) and proved:
- Exactly-one ownership under 100 concurrent in-process agents.
- Zero unauthorized calls through the external work firewall (`UnapprovedExternalWorkError`).
- Hard fail-closed transitions in `StepHarness`.

However, Stage 1's ownership and mutual exclusion relied exclusively on in-memory mechanisms:
- `_claims_lock` (an `asyncio.Lock` local to a single Python event loop).
- In-memory `ChromaDB EphemeralClient`.
- In-memory `ContextVar` coordination token tracking.
- In-memory Reaper loop scanning volatile Chroma collections.

If two or more independent OS worker processes run concurrently:
1. `asyncio.Lock` does not synchronize across process boundaries.
2. Independent processes accessing isolated in-memory Chroma instances cannot detect each other's claims.
3. A process crash immediately destroys in-flight task leases and claims, leaving tasks unrecoverable.
4. Duplicate queue messages or concurrent worker deliveries can cause split-brain ownership.

Stage 2 elevates Quorum from an in-process prototype to a **durable, cross-process coordination platform**.

---

## 2. State Boundaries & Separation of Concerns

The foundational architectural principle of Stage 2 is the separation of **semantic retrieval** from **transactional authority**:

> **The Semantic Vector Store (Chroma) answers:** *"What existing work or claims are semantically related?"*  
> **The Transactional Relational Database answers:** *"Who authoritative owns this work, for what duration, and under what version?"*

```
                       ┌──────────────────────────────────────┐
                       │           Worker Process             │
                       └──────────────────┬───────────────────┘
                                          │
                         1. Semantic Sense│
                                          ▼
                       ┌──────────────────────────────────────┐
                       │        Chroma (Semantic Index)       │
                       │   Returns candidate related claims   │
                       └──────────────────┬───────────────────┘
                                          │
                                          │ 2. Candidates & Work Fingerprint
                                          ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    Authoritative Relational Database (WAL Mode)                 │
│                                                                                 │
│   BEGIN IMMEDIATE Transaction:                                                  │
│   ├── Check active claims / candidate leases (status = 'active', expires_at > t)│
│   ├── Evaluate tiebreak & semantic collision                                    │
│   ├── Insert/Update claim + issue durable claim lease with fencing version v    │
│   └── Commit (Atomic & durable across all OS processes)                         │
└─────────────────────────────────────────────────────────────────────────────────┘
                                          │
                        3. Outbox Event   │
                                          ▼
                       ┌──────────────────────────────────────┐
                       │     Outbox Sync Worker / Retries     │
                       │  Indexes new claim/finding into      │
                       │  Chroma for future sense queries     │
                       └──────────────────────────────────────┘
```

---

## 3. Explicit State Definitions

### 3.1. Authoritative Task State
- **Storage**: Transactional Database (`tasks` table).
- **Identifier**: `id` (UUIDv4) and `idempotency_key` (Unique text).
- **Lifecycle States**:
  - `QUEUED`: Waiting in durable queue for worker pick-up.
  - `RUNNING`: Leased by an active worker.
  - `RETRYING`: Worker crashed or lease expired; scheduled for retry.
  - `COMPLETED`: Work completed and authoritative finding published.
  - `FAILED`: Execution terminated with fatal error.
  - `CANCELLED`: Explicitly aborted.
  - `DEAD_LETTERED`: Exceeded `max_attempts`; quarantined for manual inspection.
- **Invariants**:
  - Exactly one task record exists per `idempotency_key`.
  - Transitions are atomic and durable under database transaction.

### 3.2. Authoritative Claim State
- **Storage**: Transactional Database (`claims` table).
- **Identifier**: `id` (UUIDv4) and `work_key` / `task_fingerprint` (Normalized semantic hash or task key).
- **Lifecycle States**:
  - `ACTIVE`: Currently owned by an active worker under a valid lease.
  - `SUPERSEDED`: Replaced by a higher-priority claim (human or tiebreak winner within epsilon window).
  - `REAPED`: Lease expired due to worker heartbeat failure; task liberated.
  - `COMPLETED`: Final finding published; claim archived as successfully completed.
- **Invariants**:
  - At most one `ACTIVE` claim exists for a given `work_key` or active task.
  - Any mutation requires holding the database write transaction.

### 3.3. Lease State & Fencing
- **Storage**: Transactional Database (`claim_leases` table).
- **Attributes**:
  - `claim_id` (Foreign key to `claims.id`).
  - `owner_id` (Participant ID, e.g. `agent-1`).
  - `worker_id` (Unique worker process UUID).
  - `version` (Monotonically increasing integer).
  - `attempt` (Integer attempt counter).
  - `created_at` (ISO-8601 UTC).
  - `heartbeat_at` (ISO-8601 UTC).
  - `expires_at` (ISO-8601 UTC).
  - `state` (`ACTIVE`, `EXPIRED`, `FENCED`, `RELEASED`).
- **Fencing Invariants**:
  - Every external call and completion verification requires `(claim_id, version, worker_id)`.
  - If a worker dies and its lease expires, a new worker increments the lease `version` to `v + 1`.
  - When the zombie worker returns, any attempt to heartbeat, complete, or publish work with version `v` is rejected with `LeaseFencedError`.

### 3.4. Semantic Index State
- **Storage**: ChromaDB (configured with durable storage directory `CHROMA_PERSIST_DIR`).
- **Role**: Read-heavy advisory index for fast approximate nearest-neighbor search.
- **Invariants**:
  - Never authoritative for ownership.
  - If Chroma is unavailable or stale, the transactional database guarantees safety (no duplicate claims).
  - Updates are fed asynchronously via transactional outbox or safe write-through.

### 3.5. Finding State
- **Storage**: Transactional Database (`finding_metadata` table) + ChromaDB (`findings` namespace).
- **Attributes**: `id`, `run_id`, `task_id`, `claim_id`, `participant_id`, `title`, `content`, `sources`, `created_at`.
- **Invariants**:
  - Finding publication is atomic with task state transition `RUNNING → COMPLETED`.
  - Duplicate completion requests return the existing finding without re-publishing.

### 3.6. Retry State
- **Storage**: Transactional Database (`task_attempts` table).
- **Attributes**: `id`, `task_id`, `attempt_number`, `worker_id`, `started_at`, `ended_at`, `status`, `error_message`.
- **Invariants**:
  - Tracks every attempt history for auditing, backoff calculation, and debugging.

### 3.7. Worker State
- **Storage**: Transactional Database (`workers` table).
- **Attributes**: `id` (UUID), `hostname`, `pid`, `state`, `heartbeat_at`, `registered_at`.
- **Lifecycle States**:
  - `STARTING`: Initializing resources and registering.
  - `READY`: Polling queue for work.
  - `BUSY`: Actively executing a task lease.
  - `DRAINING`: Completing current task before graceful shutdown; no new tasks accepted.
  - `STOPPED`: Clean shutdown completed.
  - `FAILED`: Declared dead by Reaper due to missed heartbeats.

### 3.8. Audit State
- **Storage**: Transactional Database (`audit_events` table).
- **Attributes**: `id`, `run_id`, `task_id`, `event_type`, `actor_id`, `details`, `timestamp`.
- **Invariants**:
  - Append-only log of critical lifecycle transitions (claim granted, lease fenced, worker reaped, duplicate absorbed).

---

## 4. Concurrency & Cross-Process Mutual Exclusion

### 4.1. The SQLite WAL Cross-Process Mutual Exclusion Model
In local and edge environments, Quorum runs on SQLite with Write-Ahead Logging (`PRAGMA journal_mode=WAL;`) and `PRAGMA busy_timeout = 5000;`.
- **OS-Level Locking**: SQLite uses OS file locks (`fcntl` on Linux/POSIX) to synchronize across independent OS processes.
- **Immediate Write Transactions**: By initiating write operations with `BEGIN IMMEDIATE`, SQLite acquires a reserved lock immediately.
- Only one process on the system can execute a claim transaction at any given millisecond.
- Competing processes transparently wait up to 5,000ms.
- This guarantees **strict serializability** across independent OS processes without needing an external distributed broker daemon.

### 4.2. Exactly-Once vs At-Least-Once Semantics
| Subsystem | Semantics Guaranteed | Mechanism |
| :--- | :--- | :--- |
| **Task Queue Delivery** | At-Least-Once | Worker lease timeouts and retry requeuing guarantee tasks are never lost on worker crash. |
| **Claim Ownership** | Exactly-Once | Atomic DB transaction with unique constraints and immediate write locks ensures at most one active claim exists per work identity. |
| **External Work Execution** | At-Most-Once per Claim | Fenced `CoordinationToken` bound to `(claim_id, version)` prevents duplicate external calls. |
| **Task Completion & Findings** | Exactly-Once | Idempotent completion check; duplicate publishes return existing finding record. |

---

## 5. Failure Recovery Workflows

### 5.1. Worker Crash During External Work
1. Worker A acquires task lease (claim version 1) and calls external API.
2. Worker A's host/process crashes (kill -9).
3. Reaper background process scans `claim_leases` and `workers`.
4. Reaper observes `Worker A` heartbeat has timed out (`now - heartbeat_at > TTL`).
5. Reaper marks `Worker A` as `FAILED`, marks claim lease as `EXPIRED`, increments `task.attempt_count`, and resets `task.state` to `RETRYING`.
6. Worker B receives the task from the queue, acquires a new lease with **version 2**.
7. If Worker A recovers or sends a late request, the lease check compares `token.version (1) == lease.version (2)`. Because version 1 < 2, the request is rejected with `LeaseFencedError`.
8. Worker B completes the task authoritatively.

### 5.2. Semantic Index (Chroma) Outage
1. Worker completes claim transaction in the transactional database.
2. Indexing into Chroma fails (e.g. disk error or temporary lock).
3. The claim remains authoritative in the database.
4. An outbox entry is marked as `PENDING` with retry count.
5. The task proceeds to external work safely because ownership is verified via the database.
6. The outbox background worker retries indexing into Chroma until successful.
7. Result: Zero task loss, zero split-brain ownership.
