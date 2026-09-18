# Quorum — Build Context & Progress Tracker

> Last updated: 2026-09-18T10:43:00+05:30

---

## Project Summary

**Quorum** is a single-process Python system where N agents + 1 human + 1 Adjudicator + 1 Reaper coordinate over a shared semantic board (Moss/ChromaDB, in-process) and a shared transport (LiveKit).

**Core thesis:** Semantic check-before-act (sense → claim → gate → act) is cheap enough to run on every single step.

**Day 1 Gate:** Passed all 4 latency/GC tests on real hardware with in-process semantic indexing.

---

## Stack Summary (100% Free Tier, Zero-Budget)

| Layer | Choice | Rationale |
|---|---|---|
| Language / Runtime | Python 3.12, asyncio | Native async coroutines in single process |
| Semantic index | Moss / ChromaDB (in-process EphemeralClient) | Zero HTTP hop, sub-1ms sense latency |
| Inference primary | Groq (Llama 3.1 8B Instant) | Fastest free-tier inference |
| Inference failover | OpenRouter → Hugging Face Inference API | 3-deep rotation on 429/5xx |
| External search | Tavily (free tier) | Real external grounding |
| Realtime transport | LiveKit (data channels + voice bridge) | Presence, board streaming, voice UI |
| API framework | FastAPI | Pydantic v2 first-class, async native |
| Structured output | Instructor wrapping InferenceRouter | 1 repair retry on schema violation |
| Validation | Pydantic v2 (strict, no-extra-fields) | Strict contracts across all models |
| Persistence | SQLite via aiosqlite | Ephemeral run metadata & replay logs |
| Auth | OAuth2 password flow, JWT + refresh tokens | Zero-cost self-contained auth |
| Observability | OpenTelemetry SDK + in-process RingBuffer | Separate timing & telemetry paths |
| Profiling | py-spy / tracemalloc | GC correlation checks |
| Frontend | Next.js 14 + LiveKit React SDK + Tailwind CSS | Real-time swarm coordination UI |
| CI | GitHub Actions | Automated gate & integrity checks on PR |

---

## Phases & Implementation Status

### ✅ Phase 0 — Repo Scaffolding
- [x] Project directory structure created (`quorum/`)
- [x] `requirements.txt` with all dependencies
- [x] `config/settings.py` (env-var only, no hardcoded secrets)
- [x] `.env.example` template created
- [x] `.github/workflows/` (day1-gate.yml, ci.yml)

### ✅ Phase 1 (Day 1) — Gate Tests (PASSED)
- [x] `benchmark/day1_latency_gate.py` — standalone, no inference in loop
  - [x] Test 1: Standalone baseline — sense p50=0.820ms, p99=1.944ms (<10ms target); write p50=6.474ms, p99=12.805ms (<15ms target)
  - [x] Test 2: Concurrent-writer latency — 5 tasks × 200, concurrent p99=1.737ms (<20ms target), degradation 0.0% (<30% target)
  - [x] Test 3: Index-growth curve — seed: 1.970ms, seed+20: 1.873ms, seed+80: 2.669ms (all <10ms target)
  - [x] Test 4: GC/memory profile — peak 0.03MB, delta 0.02MB, GC counts (104, 0, 0)
- [x] `.github/workflows/day1-gate.yml` — CI-wired, re-runnable
- [x] `benchmark/day1_results.json` committed

### ✅ Phase 2 (Days 2–4) — Bus, Harness, Reaper, Agents, Transport
1. [x] `bus/schemas.py` — Claim, Finding, Conflict, SenseResult strict Pydantic v2 models
2. [x] `bus/moss_client.py` + `bus/namespaces.py` — in-process Moss/ChromaDB with FastFeatureEmbeddingFunction
3. [x] `bus/tiebreak.py` — (participant_type_priority, ts_monotonic, participant_id), 50ms human-priority epsilon
4. [x] `bus/versioning.py` — supersedes-chain forward resolution to latest terminal head
5. [x] `agents/harness.py` — step contract enforcement (raises `StepOrderViolation` in dev/test)
6. [x] `reaper/reaper.py` — 1s scan, 15s TTL, heartbeat_timeout reason code, separate coroutine
7. [x] `adjudicator/adjudicator.py` + `conflict_tray.py` — similarity detection + LLM adjudication, never auto-resolves
8. [x] `inference/router.py` + `instructor_client.py` — Groq → OpenRouter → HF rotation with safe fallbacks
9. [x] `transport/livekit_room.py` — data-channel board state streaming & scoped room tokens
10. [x] `transport/voice_bridge.py` — voice query / claim with immediate audio zeroing
11. [x] `agents/domain_agents/` — 4 concrete research agents (Literature, Data, Critique, Synthesis)

### ✅ Phase 3 (Day 5) — Baseline Arm & Experiments
- [x] `benchmark/relay_arm.py` — conventional message-passing baseline (control arm)
- [x] `benchmark/board_arm_runner.py` — 10 iterations per arm, raw JSON logs, mean & variance
- [x] `benchmark/metrics/collision_window_integrity.py` — 100% assertion verified
- [x] `benchmark/metrics/reap_rate.py` — reap rate & reason breakdown
- [x] `benchmark/metrics/sense_latency_vs_index_size.py` — scaling curve from 10 to 1,000 docs (all <10ms)

### ✅ Phase 4 (Days 6–7) — Polish & Workspace Frontend
- [x] Comprehensive pytest test suite (`tests/` — 19/19 tests passing)
- [x] FastAPI full application with lifespan (`api/main.py`)
- [x] Next.js 14 real-time workspace frontend (`frontend/`) with LiveKit integration, ParticipantList, BoardState, ConflictViewer, and VoiceControl components

---

## Non-negotiables Verification Matrix

| Checklist Requirement | Implementation Details | Status |
|---|---|---|
| Moss is NEVER placed behind an internal HTTP service | In-process `chromadb.EphemeralClient` direct memory calls in `bus/namespaces.py` | ✅ Verified |
| `agents/harness.py` ordering guard throws at runtime in dev/test | Enforced in `StepHarness` state machine, 5 dedicated unit tests | ✅ Verified |
| Reaper and Adjudicator are separate coroutine loops | Launched via `asyncio.create_task` in `api/main.py` lifespan, never called synchronously in write path | ✅ Verified |
| Strict Pydantic v2 validation on all request/response models | `ConfigDict(strict=True, extra="forbid")` throughout `bus/schemas.py` and API routes | ✅ Verified |
| `DELETE /runs/{id}` drops Moss namespace + SQLite rows + tombstones OTel spans | Verified across all 3 layers in `tests/test_erasure.py` | ✅ Verified |
| Voice audio transcribed and discarded immediately | Audio reference zeroed and deleted in `transport/voice_bridge.py` | ✅ Verified |
| In-process ring buffer and OTel are separate code paths | `observability/ring_buffer.py` has zero OTel dependencies; separate module | ✅ Verified |
| Day 1 gate script is re-runnable in CI | Exits non-zero on failure, wired to `.github/workflows/day1-gate.yml` | ✅ Verified |
| No paid tier / credit card required | All services (Groq, OpenRouter, HF, Tavily, LiveKit, SQLite, HF Spaces, Vercel) are free tier | ✅ Verified |

---

## Actual Benchmark Results (Executed on Real Hardware)

### Day 1 Latency Gate
| Metric | Target | Actual p50 | Actual p99 | Gate Verdict |
|---|---|---|---|---|
| Standalone Sense | < 10.0 ms | 0.820 ms | 1.944 ms | ✅ PASSED |
| Standalone Write | < 15.0 ms | 6.474 ms | 12.805 ms | ✅ PASSED |
| Concurrent Sense (5 writers) | < 20.0 ms | 0.831 ms | 1.737 ms | ✅ PASSED |
| Contention Degradation | < 30.0% | - | 0.0% | ✅ PASSED |
| Index Growth (20 docs) | < 10.0 ms | - | 1.970 ms | ✅ PASSED |
| Index Growth (40 docs) | < 10.0 ms | - | 1.873 ms | ✅ PASSED |
| Index Growth (100 docs) | < 10.0 ms | - | 2.669 ms | ✅ PASSED |
| Index Growth (1,000 docs) | < 10.0 ms | 0.430 ms | 2.044 ms | ✅ PASSED |

### 10-Iteration Comparative Benchmark (Board Arm vs Relay Arm)
- **Board Arm Latency:** 76.43 ms ± 11.62 ms
- **Relay Arm Latency:** 81.19 ms ± 0.19 ms
- **Collision-Window Integrity:** 100.0% (0 harness ordering violations)
- **Redundancy Eliminated via Board Sensing:** 36 redundant subtask executions prevented across 10 runs
- **Test Suite Status:** 19/19 passing tests (`pytest`)
