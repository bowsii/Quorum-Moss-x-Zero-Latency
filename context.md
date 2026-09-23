# Quorum — Comprehensive Build Context & System State

> **Last Updated:** 2026-09-23T11:30:00+05:30  
> **Repository:** Quorum (`Moss x Zero latency`)  
> **Status:** Stage 1 Fully Verified & Passed; Stage 2 (Phase B: Authoritative Database & Phase C: Distributed Claim Arbitration) Implemented  
> **Architecture Mode:** Multi-Agent Semantic Coordination with Authoritative Transactional State

---

## 1. Executive Summary & Core Thesis

**Quorum** is an advanced multi-agent coordination architecture designed to eliminate redundant, conflicting, and uncoordinated work in high-concurrency LLM agent swarms.

### Core Thesis
**Semantic check-before-act (`SENSE → CLAIM → VALIDATE/TIEBREAK → EXTERNAL_ALLOWED → ACT → PUBLISH`) is cheap enough to execute on every single step of agent execution.**

### The Coordination Invariant Pipeline
```text
┌─────────┐      ┌─────────┐      ┌─────────────────────────┐      ┌────────────────────────┐      ┌─────────────┐      ┌─────────────────┐
│ SENSE   │ ───► │  CLAIM  │ ───► │ CLAIM VALIDATION /      │ ───► │ EXTERNAL_WORK_ALLOWED  │ ───► │ EXPENSIVE   │ ───► │ PUBLISH FINDING │
│ (Chroma)│      │  INTENT │      │ TIEBREAK ARBITRATION    │      │ (CoordinationToken)    │      │ WORK (LLM,  │      │ (Chroma + DB)   │
└─────────┘      └─────────┘      └─────────────────────────┘      └────────────────────────┘      │ Tavily API) │      └─────────────────┘
                                               │                                                   └─────────────┘
                                               ▼
                                  ┌─────────────────────────┐
                                  │ DUPLICATE / COMPLETED   │
                                  │ ABSORPTION              │
                                  │ (No Token / Firewalled) │
                                  └─────────────────────────┘
```

1. **Sense:** Agent queries the shared semantic board for similar active claims or completed findings (sub-millisecond in-process vector retrieval).
2. **Claim:** Agent asserts intent to perform a specific subtask before invoking external tools.
3. **Arbitrate / Tiebreak:** The system evaluates conflicts. If another agent already claimed the task outside the 50ms race window or completed it, the contender is absorbed. If within 50ms, deterministic tiebreak decides the sole winner.
4. **Firewall Gate (`CoordinationToken`):** Only the approved winner receives an unrevoked, active `CoordinationToken`. Any call to external LLMs (Groq/OpenRouter/HuggingFace) or web search (Tavily) without an active bound token fails closed via `UnapprovedExternalWorkError`.
5. **Publish Finding:** Once work completes, the finding is posted to the authoritative database and semantic board, preventing downstream duplication.

---

## 2. Stage 1 vs. Stage 2 Architectural Separation

Stage 1 established in-process coordination (`_claims_lock`, `StepHarness`, `ClaimDecisionEngine`) within a single Python event loop. Stage 2 extends this foundation to **independent operating system processes** without sacrificing single-process zero-latency advantages.

```text
                               ┌──────────────────────────────────────────────┐
                               │           PostgreSQL (Stage 2)               │
                               │   AUTHORITATIVE TRANSACTIONAL STATE          │
                               │                                              │
                               │  • runs                 • workers            │
                               │  • tasks (unique key)   • findings metadata  │
                               │  • claims (row locks)   • idempotency        │
                               │  • audit_events (log)                        │
                               └──────────────────────┬───────────────────────┘
                                                      │
                         Authoritative State & Locks  │  FOR UPDATE / Transactions
                                                      ▼
┌────────────────────────┐                    ┌────────────────────────┐
│   Process A (Worker)   │                    │   Process B (Worker)   │
│  • BaseAgent / Harness │                    │  • BaseAgent / Harness │
│  • CoordinationToken   │                    │  • CoordinationToken   │
└───────────┬────────────┘                    └───────────┬────────────┘
            │                                             │
            │           Candidate Queries                 │
            ▼           (Advisory Only)                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       ChromaDB / Moss (In-Process)                          │
│                         SEMANTIC INDEX ONLY                                 │
│                                                                             │
│  • EphemeralClient: collections `quorum_claims` & `quorum_findings`         │
│  • FastFeatureEmbeddingFunction: 384-dim sub-millisecond local embeddings   │
│  • Answers: "What existing claims or findings might be semantically related?"│
│  • CRITICAL: Chroma NEVER decides ownership or issues capabilities!         │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ Data-channel streaming & Voice WebRTC
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           LiveKit Transport Edge                            │
│  • Rooms: `quorum-{run_id}`                                                 │
│  • Data Channels: Real-time board state sync to agents and Web UI           │
│  • Voice Bridge: Transcribe via Whisper -> Immediate audio zero/deletion    │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SQLite (quourm.db)                                │
│                     TRANSITIONAL STAGE 1 & COMPLIANCE                       │
│  • REST API runs CRUD (`POST /runs`, `GET /runs/{id}`)                       │
│  • `consent_events` & `replay_logs`                                         │
│  • Right-to-Erasure GDPR cascading purge (`DELETE /runs/{id}`)              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Safety & Authority Invariants
1. **Chroma is NEVER authoritative:** Chroma provides approximate nearest-neighbor candidate discovery. All claim creation, lease validation, ownership grants, and row locks execute in PostgreSQL.
2. **PostgreSQL Fails Closed:** If PostgreSQL is unreachable or connection pools fail, the system raises `DatabaseUnavailableError`. No claim is granted, no token is issued, and no external execution occurs.
3. **Dual Persistence Boundary:** SQLite handles legacy Stage 1 API sessions, replay audit logs, and GDPR cascading erasure. PostgreSQL handles distributed task arbitration and authoritative state.

---

## 3. Technology Stack (100% Free Tier, Zero-Budget)

| Layer | Choice | Rationale & Architectural Constraints |
|---|---|---|
| **Language / Runtime** | Python 3.12, asyncio | Native async coroutines for agents, Reaper, and Adjudicator |
| **Semantic Index** | Moss / ChromaDB (in-process `EphemeralClient`) | Zero HTTP hops, in-memory sub-millisecond similarity queries |
| **Embedding Engine** | `FastFeatureEmbeddingFunction` (384-dim) | Deterministic MD5 feature hashing with L2-normalization; sub-millisecond, CPU-local |
| **Authoritative DB** | PostgreSQL via `asyncpg>=0.29.0` | ACID transactions, row-level locks (`SELECT ... FOR UPDATE`), partial unique indices |
| **Transitional DB** | SQLite via `aiosqlite>=0.20.0` | Ephemeral run metadata, replay logs, consent records, GDPR right-to-erasure |
| **Primary Inference** | Groq (`llama-3.1-8b-instant`) | Fastest free-tier inference (2/3 of step execution budget) |
| **Inference Failover** | OpenRouter (`meta-llama/llama-3.1-8b-instruct:free`) → Hugging Face | 3-deep rotation on HTTP 429/5xx, sticky failover |
| **External Grounding** | Tavily (`tavily-python>=0.3.0`) | Verified external web grounding; gated behind coordination firewall |
| **Realtime Transport** | LiveKit (`livekit>=0.11.0`, `@livekit/components-react`) | WebRTC data channels for board streaming; audio tracks for voice UI |
| **API Framework** | FastAPI (`fastapi>=0.111.0`, `uvicorn>=0.29.0`) | Async-native lifespan, Pydantic v2 first-class validation, OpenAPI docs |
| **Structured Output** | Instructor (`instructor>=1.3.0`) wrapping Groq | Automatic schema extraction with 1-repair retry on validation failure |
| **Validation** | Pydantic v2 (`strict=True`, `extra="forbid"`) | Strict schemas preventing field injections or corrupted payloads |
| **Auth** | OAuth2 password flow, JWT (HS256) + refresh tokens | Self-contained zero-cost auth with short-lived access tokens |
| **Observability** | OpenTelemetry SDK + In-process `RingBuffer` | Dedicated, separate code paths; ring buffer eliminates exporter batch distortion |
| **Profiling** | `py-spy` / `tracemalloc` | Sampling profiling for memory leaks and GC correlation checks |
| **Frontend** | Next.js 14, React 18, Tailwind CSS, Lucide Icons | Swarm monitoring workspace, real-time board state, conflict tray, voice bridge |
| **CI / CD** | GitHub Actions | Automated latency gating, regression tests, and collision-window verification |

---

## 4. Phase-by-Phase Implementation Status

```text
Phase 0: Scaffolding & Configuration ────────► [COMPLETED]
Phase 1: Day 1 Latency Gate Protocol ────────► [COMPLETED - ALL 4 TESTS PASSED]
Phase 2: Semantic Bus, Harness, Reaper ──────► [COMPLETED]
Phase 3: Baseline Arm & Comparative Benchmark► [COMPLETED]
Phase 4: Workspace Frontend & REST API ──────► [COMPLETED]
Stage 2 - Phase B: Authoritative PostgreSQL ──► [COMPLETED]
Stage 2 - Phase C: Distributed Claims & Locks ─► [COMPLETED]
Stage 2 - Phase D: Distributed Lease Heartbeats [NEXT]
Stage 2 - Phase E: Durable Task Queue ────────► [PLANNED]
Stage 2 - Phase F: Worker Crash Recovery ─────► [PLANNED]
```

### ✅ Phase 0 — Scaffolding & Environment
- [x] Project layout established under `quorum/`.
- [x] `requirements.txt` with locked dependencies.
- [x] `config/settings.py` reading from environment / `.env`, with production secret validation.

### ✅ Phase 1 — Day 1 Latency Gate (Passed on Real Hardware)
- [x] `benchmark/day1_latency_gate.py`: Standalone, inference-free gate script.
- [x] **Test 1 (Baseline):** Sense p50=0.820ms, p99=1.944ms (<10ms target); Write p50=6.474ms, p99=12.805ms (<15ms target).
- [x] **Test 2 (Contention):** 5 tasks × 200 writes, concurrent p99=1.737ms (<20ms target), 0.0% degradation (<30% target).
- [x] **Test 3 (Index Growth):** 20 docs: 1.970ms; 40 docs: 1.873ms; 100 docs: 2.669ms; 1,000 docs: 2.044ms (all <10ms target).
- [x] **Test 4 (GC / Memory):** Peak 0.03MB, delta 0.02MB, GC counts (104, 0, 0), zero pauses correlated with p99.
- [x] Output artifact saved at `benchmark/day1_results.json`.

### ✅ Phase 2 — Bus, Harness, Reaper, Agents & Transport
- [x] **Schemas (`bus/schemas.py`):** `Claim`, `Finding`, `Conflict`, `SenseResult` with `strict=True, extra="forbid"`.
- [x] **Namespaces (`bus/namespaces.py`):** In-process `EphemeralClient` with `FastFeatureEmbeddingFunction`.
- [x] **Tiebreaking (`bus/tiebreak.py`):** Priority hierarchy (`human (0) > adjudicator (1) > agent (2) > reaper (3)`), 50ms human-priority epsilon window, monotonic timestamp comparison, deterministic participant ID lexicographical fallback.
- [x] **Versioning (`bus/versioning.py`):** Forward supersedes-chain traversal with cycle-protection (`max_depth=50`) resolving terminal active heads.
- [x] **Harness & Firewall (`agents/harness.py`, `agents/coordination_token.py`):**
  - State machine: `INIT → SENSED → CLAIMED → EXTERNAL_ALLOWED → DONE`.
  - Violations raise `StepOrderViolation` across development, test, and production.
  - External services (`inference/router.py`, `search/tavily_client.py`) enforce `verify_coordination_capability()`, rejecting unauthorized calls via `UnapprovedExternalWorkError`.
- [x] **Reaper (`reaper/reaper.py`):** 1s scan loop, 15s TTL, reaps expired claims to `status="reaped"` with `reap_reason="heartbeat_timeout"`. Runs in a dedicated background coroutine.
- [x] **Adjudicator (`adjudicator/adjudicator.py`, `conflict_tray.py`):** Scans findings, detects cosine distance < 0.15 (similarity > 0.85), requests descriptive LLM verdict, adds to `ConflictTray`. **Never auto-resolves.**
- [x] **Inference Router (`inference/router.py`, `instructor_client.py`):** Groq -> OpenRouter -> HF rotation on 429/5xx. Sticky routing. Instructor wrapping with 1-repair retry.
- [x] **Transport (`transport/livekit_room.py`, `voice_bridge.py`):** Scoped room tokens (`room_record=True` for agents, `False` for observers). Board state streaming over data channels. Whisper transcription with **immediate memory zeroing and deletion of audio bytes**.
- [x] **Domain Agents (`agents/domain_agents/`):**
  - `LiteratureResearcher`: Prior art search & synthesis.
  - `DataAnalyst`: Quantitative analysis & empirical datasets.
  - `CritiqueResearcher`: Counterargument search & tension analysis (2s startup yield).
  - `SynthesisBuilder`: Multi-finding cross-reconciliation & consensus synthesis.

### ✅ Phase 3 — Baseline Arm & Comparative Benchmark
- [x] `benchmark/relay_arm.py`: Fixed-graph queue-based agent pipeline control arm.
- [x] `benchmark/board_arm_runner.py`: 10-iteration comparative suite logging raw JSON runs.
- [x] **Board Arm Latency:** 76.43 ms ± 11.62 ms.
- [x] **Relay Arm Latency:** 81.19 ms ± 0.19 ms.
- [x] **Collision-Window Integrity:** 100.0% (zero ordering violations across all runs).
- [x] **Redundancy Eliminated:** 36 redundant subtask executions prevented across 10 iterations.

### ✅ Phase 4 — Polish, REST API & Frontend Workspace
- [x] FastAPI REST API (`api/main.py`) with full lifespan managing DB, OTel, Reaper, and Adjudicator tasks.
- [x] Security middleware (`RateLimitMiddleware`, `SecurityHeadersMiddleware`, CORS).
- [x] Endpoints for runs (`POST /runs`, `GET /runs/{id}`, `DELETE /runs/{id}` full erasure), bus (`/bus/sense`, `/bus/claim`, `/bus/finding`, `/bus/conflicts`).
- [x] Next.js 14 frontend workspace (`frontend/`) with LiveKit integration, Participant presence, Board state viewer, Conflict tray, and Voice controls.

### ✅ Stage 2 - Phase B — Authoritative PostgreSQL Foundation
- [x] **Async Connection Pool (`db/connection.py`):** Managed pool lifecycle with fail-closed semantics (`DatabaseUnavailableError`).
- [x] **Transaction Boundary (`db/transaction.py`):** `async with transaction()` context manager, ambient propagation via `contextvars.ContextVar`, nested savepoints, row-level locks (`SELECT ... FOR UPDATE`).
- [x] **Migration Runner (`db/migrations/runner.py`):** Session-level PostgreSQL advisory lock (`pg_advisory_lock(82749182)`) preventing concurrent worker migration races.
- [x] **DDL Schema (`db/migrations/001_initial_authoritative_state.sql`):** 7 core tables (`runs`, `tasks`, `claims`, `workers`, `findings`, `idempotency_records`, `audit_events`).
- [x] **Repositories (`db/repositories/`):** Decoupled repositories (`run_repo`, `task_repo`, `claim_repo`, `finding_repo`, `worker_repo`, `idempotency_repo`, `audit_repo`) supporting ambient connection reuse and explicit locking.
- [x] **Validation Test Suite (`tests/test_phase_b_database.py`):** 12 comprehensive integration tests validating schema idempotency, transaction rollback, persistence across independent connections, fail-closed behavior, advisory locks, and row-level locking.

### ✅ Stage 2 - Phase C — Distributed Claims & Cross-Process Arbitration
- [x] **Authoritative Arbitration Engine (`db/arbitration.py`):**
  - Multi-process race arbitration serializing contenders via `SELECT ... FOR UPDATE` on task rows.
  - Generates immutable `AuthoritativeClaimDecision` containing outcome category and capability requirement.
  - Distinguishes outcomes: `OWNER_NEW`, `OWNER_ALREADY_HELD`, `DUPLICATE_ABSORBED`, `EXPIRED_RECLAIMED`, `TIEBREAK_WON`, `TIEBREAK_LOST`, `COMPLETED_FINDING_ABSORBED`.
- [x] **Database Defense-in-Depth (`db/migrations/002_active_claim_unique_constraint.sql`):**
  - Partial unique index `uq_claims_active_task ON claims(task_id) WHERE status = 'active'`.
  - Rejects duplicate active claims at SQL level if concurrent transactions slip past row locks.
  - Handles unique violations gracefully via savepoints, absorbing late contenders.
- [x] **Coordination Capability Issuance (`bus/arbitration.py`):**
  - Issues `CoordinationToken` ONLY after the database transaction commits and `capability_issuance_required=True`.
  - Non-winners receive `None`, failing closed at the external work firewall.
- [x] **Cross-Process Multi-Processing Test Suite (`tests/test_phase_c_distributed_claims.py`):**
  - Validates genuine independent OS processes (`multiprocessing.Process` with fork context and barrier synchronization).
  - Multi-process race acceptance tests:
    - **2 processes:** 1 owner, 1 non-owner, 1 capability, 1 external work, 1 firewall block.
    - **10 processes:** 1 owner, 9 non-owners, 1 capability, 1 external work, 9 firewall blocks.
    - **50 processes:** 1 owner, 49 non-owners, 1 capability, 1 external work, 49 firewall blocks.
    - **100 processes:** 1 owner, 99 non-owners, 1 capability, 1 external work, 99 firewall blocks.
  - Lifecycle tests: Active claim absorption, expired lease reclamation, completed finding absorption, idempotent retry.
  - Failure tests: Transaction rollback leaving zero claims, database failure failing closed.
  - Locking tests: Cross-connection `FOR UPDATE NOWAIT` raising `LockNotAvailableError`.

---

## 5. Complete File Catalog & System Blueprint

```text
/home/bowsiii/Documents/Moss x Zero latency/
├── context.md                                   # Master architecture, progress tracker & system state
├── implementation.md                            # Original zero-budget build specification & PRD contracts
├── quorum/
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py                          # Centralized Pydantic-settings config (env-driven, no secrets)
│   ├── bus/
│   │   ├── __init__.py
│   │   ├── schemas.py                           # Strict Pydantic v2 models: Claim, Finding, Conflict, SenseResult
│   │   ├── namespaces.py                        # In-process ChromaDB EphemeralClient & FastFeatureEmbeddingFunction
│   │   ├── moss_client.py                       # In-process async bus client (sense, claim, write, heartbeat)
│   │   ├── tiebreak.py                          # 50ms human-epsilon tiebreak & priority comparator
│   │   ├── versioning.py                        # Supersedes forward chain-head resolver with cycle guard
│   │   ├── candidate_retrieval.py               # Chroma semantic candidate discovery boundary (non-authoritative)
│   │   ├── claim_engine.py                      # Stage 1 in-process ClaimDecisionEngine
│   │   └── arbitration.py                       # Stage 2 coordination-facing claim arbitration & token issuance
│   ├── agents/
│   │   ├── __init__.py
│   │   ├── coordination_token.py                # Typed capability token & external work firewall
│   │   ├── harness.py                           # StepHarness state machine (sense -> claim -> gate -> act)
│   │   ├── base_agent.py                        # Base class for domain agents (heartbeat, step loop, claim helpers)
│   │   └── domain_agents/
│   │       ├── __init__.py
│   │       ├── research_agent_1.py              # Literature & Prior Art Researcher
│   │       ├── research_agent_2.py              # Quantitative Data & Statistics Analyst
│   │       ├── research_agent_3.py              # Counterargument & Critique Specialist (2s yield)
│   │       └── research_agent_4.py              # Cross-Finding Synthesis & Consensus Builder
│   ├── reaper/
│   │   ├── __init__.py
│   │   └── reaper.py                            # Async heartbeat TTL scanner (15s TTL, heartbeat_timeout reap)
│   ├── adjudicator/
│   │   ├── __init__.py
│   │   ├── adjudicator.py                       # Semantic conflict scanner (distance < 0.15) & LLM verdict generator
│   │   └── conflict_tray.py                     # Thread-safe in-memory tray (never auto-resolves)
│   ├── inference/
│   │   ├── __init__.py
│   │   ├── router.py                            # Groq -> OpenRouter -> HF 3-deep rotation with sticky failover
│   │   └── instructor_client.py                 # Instructor structured output client with 1-repair retry
│   ├── search/
│   │   ├── __init__.py
│   │   └── tavily_client.py                     # Tavily web search client behind coordination firewall
│   ├── transport/
│   │   ├── __init__.py
│   │   ├── livekit_room.py                      # LiveKit room creation, scoped token minting, data-channel broadcast
│   │   └── voice_bridge.py                      # Voice transcription (Whisper) with immediate audio memory zeroing
│   ├── api/
│   │   ├── __init__.py
│   │   ├── main.py                              # FastAPI application entrypoint with lifespan manager
│   │   ├── auth.py                              # OAuth2 password flow, JWT issuance & token verification
│   │   ├── routes/
│   │   │   ├── __init__.py
│   │   │   ├── runs.py                          # POST /runs, GET /runs/{id}, DELETE /runs/{id} (full erasure)
│   │   │   └── bus.py                           # /bus/sense, /bus/claim, /bus/finding, /bus/conflicts
│   │   └── middleware/
│   │       ├── __init__.py
│   │       ├── rate_limit.py                    # In-process sliding-window rate limiter (100 req/min per IP)
│   │       └── security_headers.py              # Defensive HTTP headers (CSP, HSTS, X-Frame-Options, etc.)
│   ├── observability/
│   │   ├── __init__.py
│   │   ├── otel_setup.py                        # OpenTelemetry TracerProvider & span tombstone emitter
│   │   └── ring_buffer.py                       # Thread-safe bounded latency buffer for benchmarks (OTel-isolated)
│   ├── db/
│   │   ├── __init__.py
│   │   ├── connection.py                        # PostgreSQL asyncpg connection pool & fail-closed lifecycle
│   │   ├── transaction.py                       # Transaction context manager, ambient propagation & savepoints
│   │   ├── models.py                            # SQLite schema: runs, consent_events, replay_logs, tombstones
│   │   ├── erasure.py                           # Cascading 3-layer right-to-erasure purge implementation
│   │   ├── arbitration.py                       # Authoritative PostgreSQL-backed claim arbitration engine
│   │   ├── migrations/
│   │   │   ├── runner.py                        # Advisory-locked schema migration runner
│   │   │   ├── 001_initial_authoritative_state.sql # Authoritative Stage 2 tables & indices
│   │   │   └── 002_active_claim_unique_constraint.sql # Partial unique index for active claims defense
│   │   └── repositories/
│   │       ├── __init__.py
│   │       ├── base.py                          # Ambient connection resolver
│   │       ├── run_repo.py                      # Authoritative run repository
│   │       ├── task_repo.py                     # Authoritative task repository (atomic upsert & FOR UPDATE)
│   │       ├── claim_repo.py                    # Authoritative claim repository (lease versioning & status updates)
│   │       ├── finding_repo.py                  # Authoritative finding metadata repository
│   │       ├── worker_repo.py                   # Authoritative worker registry repository
│   │       ├── idempotency_repo.py              # Authoritative idempotency record repository
│   │       └── audit_repo.py                    # Authoritative append-only audit event repository
│   ├── docs/
│   │   └── architecture/
│   │       └── stage2-design.md                 # Complete Stage 2 architectural specification & invariants
│   ├── benchmark/
│   │   ├── __init__.py
│   │   ├── day1_latency_gate.py                 # Standalone 4-test latency gate protocol
│   │   ├── day1_results.json                    # Hardware benchmark results from Day 1 gate
│   │   ├── relay_arm.py                         # Message-passing baseline control arm
│   │   ├── board_arm_runner.py                  # 10-iteration comparative benchmark runner
│   │   ├── test_contention_profile.py           # Contention & concurrency profiling suite
│   │   ├── metrics/
│   │   │   ├── __init__.py
│   │   │   ├── collision_window_integrity.py    # Harness ordering integrity assertion (100% target)
│   │   │   ├── reap_rate.py                     # Reap frequency & reason breakdown metric
│   │   │   └── sense_latency_vs_index_size.py   # Sense scaling curve evaluator (10 to 1,000 docs)
│   │   └── logs/                                # Committed JSON logs for iterations 1-10 & summary
│   ├── tests/
│   │   ├── __init__.py
│   │   ├── fake_external_provider.py            # Deterministic mock external provider for firewall tests
│   │   ├── test_stage1_invariants.py            # Comprehensive Stage 1 invariant verification suite (Tests A-R)
│   │   ├── test_phase_b_database.py             # Stage 2 Phase B authoritative database verification suite
│   │   ├── test_phase_c_distributed_claims.py   # Stage 2 Phase C cross-process race & arbitration test suite
│   │   ├── test_harness.py                      # StepHarness state machine unit tests
│   │   ├── test_tiebreak.py                     # 50ms human-epsilon tiebreak boundary unit tests
│   │   ├── test_versioning.py                   # Supersedes chain resolution unit tests
│   │   ├── test_reaper_adjudicator.py           # Reaper TTL and Adjudicator tray unit tests
│   │   ├── test_router.py                       # InferenceRouter failover & rotation unit tests
│   │   ├── test_settings.py                     # Configuration validation unit tests
│   │   ├── test_ring_buffer.py                  # In-process ring buffer percentile unit tests
│   │   ├── test_erasure.py                      # Right-to-erasure 3-layer purge unit tests
│   │   └── test_api.py                          # FastAPI endpoint integration tests
│   ├── frontend/
│   │   ├── package.json                         # Next.js 14, LiveKit React SDK, Tailwind dependencies
│   │   ├── tsconfig.json                        # TypeScript configuration
│   │   ├── tailwind.config.js                   # Tailwind CSS styling tokens
│   │   ├── postcss.config.js
│   │   ├── app/
│   │   │   ├── layout.tsx                       # Root layout with dark theme
│   │   │   ├── page.tsx                         # Main Quorum workspace dashboard
│   │   │   └── globals.css                      # Global styles and Tailwind directives
│   │   └── components/
│   │       ├── BoardState.tsx                   # Live claims and findings board display
│   │       ├── ParticipantList.tsx              # Participant presence list (agents, human, reaper, adjudicator)
│   │       ├── ConflictViewer.tsx               # Adjudicator conflict tray UI with manual resolve button
│   │       └── VoiceControl.tsx                 # LiveKit audio input and voice claim submission
│   ├── requirements.txt                         # Python dependencies specification
│   └── quorum.db                                # Local SQLite database file
```

---

## 6. Authoritative Database Schema Reference

### PostgreSQL Tables (Stage 2 Authoritative State)

1. **`runs`**: Top-level coordination boundary.
   - `id` (TEXT PK), `question` (TEXT), `status` (CHECK `created`, `running`, `completed`, `failed`, `cancelled`), `metadata` (JSONB), `created_at`, `updated_at`.
2. **`tasks`**: Discrete units of work within a run.
   - `id` (TEXT PK), `run_id` (TEXT FK), `task_key` (TEXT), `description` (TEXT), `status` (CHECK `queued`, `running`, `retrying`, `completed`, `failed`, `cancelled`, `dead_lettered`), `metadata` (JSONB), `created_at`, `updated_at`.
   - **Unique Constraint:** `uq_tasks_run_task_key UNIQUE(run_id, task_key)`.
3. **`claims`**: Authoritative task ownership leases.
   - `id` (TEXT PK), `task_id` (TEXT FK), `run_id` (TEXT FK), `owner_id` (TEXT), `owner_type` (CHECK `agent`, `human`, `adjudicator`, `reaper`), `status` (CHECK `active`, `superseded`, `expired`, `released`, `completed`), `lease_id` (TEXT), `lease_version` (INT >= 1), `expires_at` (TIMESTAMPTZ), `created_at`, `updated_at`.
   - **Partial Unique Index (Defense-in-Depth):** `uq_claims_active_task UNIQUE(task_id) WHERE status = 'active'`.
4. **`workers`**: Active process / worker registry.
   - `id` (TEXT PK), `run_id` (TEXT FK nullable), `worker_type` (TEXT), `status` (CHECK `starting`, `ready`, `busy`, `draining`, `stopped`, `failed`), `last_heartbeat_at`, `created_at`, `updated_at`.
5. **`findings`**: Authoritative finding metadata & content hashes.
   - `id` (TEXT PK), `task_id` (TEXT FK nullable), `run_id` (TEXT FK), `claim_id` (TEXT FK nullable), `participant_id` (TEXT), `participant_type` (TEXT), `title` (TEXT), `content_hash` (TEXT sha256), `sources` (JSONB), `supersedes` (TEXT nullable), `created_at`, `updated_at`.
6. **`idempotency_records`**: Deduplication for client requests.
   - `id` (TEXT PK), `idempotency_key` (TEXT), `scope` (TEXT), `status` (CHECK `in_progress`, `completed`, `failed`), `payload` (JSONB), `expires_at`, `created_at`, `updated_at`.
   - **Unique Constraint:** `uq_idempotency_key_scope UNIQUE(idempotency_key, scope)`.
7. **`audit_events`**: Append-only immutable coordination audit log.
   - `id` (BIGSERIAL PK), `run_id` (TEXT FK), `actor_id` (TEXT), `event_type` (TEXT), `entity_type` (TEXT), `entity_id` (TEXT), `details` (JSONB), `created_at`.
8. **`schema_migrations`**: Migration tracking table.
   - `version` (INT PK), `name` (TEXT), `applied_at` (TIMESTAMPTZ).

---

## 7. Invariants & Non-Negotiables Verification Matrix

| Invariant / Non-Negotiable | Enforcement Mechanism & Location | Status |
|---|---|---|
| **Moss is NEVER placed behind an internal HTTP service** | Direct in-process `chromadb.EphemeralClient` calls in `bus/namespaces.py` | ✅ Verified |
| **Chroma NEVER decides ownership or issues tokens** | Ownership decided strictly by PostgreSQL `claim_task_authoritatively`; Chroma only queried in `bus/candidate_retrieval.py` | ✅ Verified |
| **CoordinationToken required for external work** | `verify_coordination_capability()` called in `inference/router.py` and `search/tavily_client.py`; raises `UnapprovedExternalWorkError` | ✅ Verified |
| **Harness ordering throws at runtime in dev/test/prod** | Enforced by `StepHarness._enforce()`; hard fail-closed in all environments | ✅ Verified |
| **Reaper and Adjudicator are separate coroutine loops** | Initialized via `asyncio.create_task` in `api/main.py` lifespan; never called synchronously in write path | ✅ Verified |
| **Strict Pydantic v2 validation everywhere** | `ConfigDict(strict=True, extra="forbid")` on all models in `bus/schemas.py`, `api/routes/runs.py`, `api/routes/bus.py` | ✅ Verified |
| **`DELETE /runs/{id}` full 3-layer right-to-erasure** | `db/erasure.py` deletes Chroma claims & findings, SQLite rows, and emits OTel tombstones; verified by `tests/test_erasure.py` | ✅ Verified |
| **Voice audio transcribed and discarded immediately** | `VoiceBridge._transcribe()` zeroes and deletes audio bytes in `finally:` block; no audio stored | ✅ Verified |
| **Ring buffer and OTel are separate code paths** | `observability/ring_buffer.py` has zero OTel dependencies; independent benchmark capture | ✅ Verified |
| **Day 1 gate script is re-runnable in CI** | `benchmark/day1_latency_gate.py` exits non-zero on failure | ✅ Verified |
| **Zero paid tier / credit card dependencies** | All components (Groq, OpenRouter, HF, Tavily, LiveKit, SQLite, PostgreSQL) run on 100% free tiers | ✅ Verified |
| **Multi-Process Exactly-One Ownership** | Row locks (`SELECT ... FOR UPDATE`) + `uq_claims_active_task` partial unique index; verified with 100 concurrent OS processes | ✅ Verified |

---

## 8. Benchmark Metrics & Verification Results

### Day 1 Latency Gate Protocol (Real Hardware Baseline)
- **Standalone Sense:** p50 = 0.820 ms, p99 = 1.944 ms (Target: < 10.0 ms) — **PASSED**
- **Standalone Write:** p50 = 6.474 ms, p99 = 12.805 ms (Target: < 15.0 ms) — **PASSED**
- **Concurrent Sense (5 writers):** p50 = 0.831 ms, p99 = 1.737 ms (Target: < 20.0 ms) — **PASSED**
- **Contention Degradation:** 0.0% (Target: < 30.0%) — **PASSED**
- **Index Growth (20 docs):** p99 = 1.970 ms (Target: < 10.0 ms) — **PASSED**
- **Index Growth (40 docs):** p99 = 1.873 ms (Target: < 10.0 ms) — **PASSED**
- **Index Growth (100 docs):** p99 = 2.669 ms (Target: < 10.0 ms) — **PASSED**
- **Index Growth (1,000 docs):** p50 = 0.430 ms, p99 = 2.044 ms (Target: < 10.0 ms) — **PASSED**
- **Memory & GC Profile:** Peak 0.03 MB, delta 0.02 MB, GC counts (104, 0, 0) — **PASSED**

### 10-Iteration Comparative Benchmark (Board Arm vs Relay Arm)
- **Board Arm Mean Latency:** 76.43 ms ± 11.62 ms
- **Relay Arm Mean Latency:** 81.19 ms ± 0.19 ms
- **Collision-Window Integrity:** 100.0% (Zero ordering violations across all steps)
- **Redundant Executions Prevented:** 36 duplicate subtasks absorbed across 10 iterations

### Stage 2 Multi-Process Contention Benchmark (Independent OS Processes)
Tested via `_run_mp_race` in `tests/test_phase_c_distributed_claims.py` under barrier synchronization:

| Contenders (N) | Owners | Non-Owners | Capabilities Issued | External Work Executions | Unauthorized Calls Blocked | Result |
|---|---|---|---|---|---|---|
| **2 OS Processes** | 1 | 1 | 1 | 1 | 1 | ✅ **PASSED** |
| **10 OS Processes** | 1 | 9 | 1 | 1 | 9 | ✅ **PASSED** |
| **50 OS Processes** | 1 | 49 | 1 | 1 | 49 | ✅ **PASSED** |
| **100 OS Processes** | 1 | 99 | 1 | 1 | 99 | ✅ **PASSED** |

---

## 9. Immediate Next Steps (Stage 2 Roadmap)

1. **Phase D — Distributed Lease Heartbeating & Fencing Enforcement:**
   - Active worker heartbeating loop updating `claims.expires_at` in PostgreSQL.
   - Fencing token check (`lease_version`) on finding submission. If an agent's claim was reaped or superseded while it was computing, the finding submission is rejected with `LeaseFencedError`.
2. **Phase E — Durable Task Queue & Delivery:**
   - Transactional task enqueueing and worker dequeueing using PostgreSQL `SKIP LOCKED`.
3. **Phase F — Worker Crash Recovery & Reaper Integration:**
   - Migrate `Reaper` scan loop to monitor `workers` and `claims` tables in PostgreSQL, auto-releasing abandoned tasks.
