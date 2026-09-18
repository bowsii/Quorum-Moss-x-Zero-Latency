# Quorum — Implementation Prompt (Zero-Budget Build Spec)

*Feed this whole document to a coding agent (Claude Code or equivalent) as the build brief. It translates the PRD into an explicit stack, repo layout, phased build order, and component-level contracts. Every choice below is free-tier; nothing here requires a paid account. Where a decision has a "why" line, keep it — it's there so the agent doesn't substitute a plausible-looking alternative that quietly breaks the thesis (e.g. putting Moss behind an HTTP service).*

---

## 0. Mission statement (do not deviate from this)

Build Quorum: a single-process Python system where N agents plus one human plus one Adjudicator plus one Reaper coordinate over a shared semantic board (Moss, in-process) and a shared transport (LiveKit). The entire value proposition rests on one falsifiable claim: **semantic check-before-act is cheap enough to do on every single step**. Day 1 output is a go/no-go on that claim, gated by the four-test protocol in Section 3. Everything after Day 1 assumes the gate passed; if it didn't, build the named fallback (phase-boundary sensing, no voice-query feature) instead of silently degrading scope elsewhere.

---

## 1. Stack — final, with rationale

| Layer | Choice | Why (and why not the obvious alternative) |
|---|---|---|
| Language / runtime | Python 3.11+, asyncio | Moss and Instructor are Python-native; asyncio lets agents, Reaper, and Adjudicator run as concurrent coroutines in one process without threads, which keeps the "one process" constraint honest. |
| Semantic index | **Moss** (in-process) | The entire thesis. Never wrapped in an internal HTTP service — that reintroduces the hop the product exists to eliminate (PRD §4). |
| Inference (primary) | **Groq** (free tier, Llama/other OSS models via Groq API) | Fastest free-tier inference available; matters because LLM call latency is 2/3 of a step's cost budget and Groq's speed is what keeps wall-clock demoable on rate-limited tiers. |
| Inference (failover) | OpenRouter free-tier models, then Hugging Face Inference API | Three-deep rotation on 429s, per PRD §16. Implement as a single `InferenceRouter` class agents call through — agents never call a provider SDK directly, so the rotation logic lives in one place. |
| External search | Tavily (free tier) | Already specified; real external grounding is what makes the duplication metric meaningful (PRD §9). |
| Realtime transport | **LiveKit** (free tier / self-hosted OSS server if free-tier minutes run out) | Data channels for board state, voice for the human participant. LiveKit Cloud has a free tier; if minutes are a risk for a multi-day build, self-host `livekit-server` in Docker at zero cost — same SDK, no code change. |
| API framework | **FastAPI** | Async-native (matches the asyncio core), Pydantic v2 is a first-class citizen, OpenAPI docs come free, plays well with OAuth2 password flow out of the box. |
| Structured output | **Instructor** wrapping the `InferenceRouter` client | One repair retry on schema violation before marking a step failed, per PRD §9. |
| Validation | **Pydantic v2** | Strict types, max lengths, no extra fields — required by PRD §13 for every request/response body, and reused for the Finding/Claim/Conflict schemas in §15. |
| Persistence (durable) | **SQLite** via `aiosqlite` | Run metadata, replay logs, consent events. Zero-cost, zero-ops, matches "run-scoped and ephemeral" (§13) — no reason to reach for Postgres on this scope. |
| Auth | OAuth2 password flow, JWT (short TTL) + rotated refresh tokens, via `python-jose` + `passlib` | Exactly as specified in §13; no external auth provider needed, keeps it zero-budget. |
| Observability | **OpenTelemetry SDK** + OTLP export to a free-tier backend (e.g. Grafana Cloud free tier, or a local Jaeger container for the build phase) + an in-process ring buffer for the benchmark path | Per §14 — the ring buffer exists *specifically* because OTel exporter batching would distort sub-10ms timings; do not try to make one buffer serve both purposes. |
| Profiling | `py-spy` (sampling profiler, no code changes needed) | For the Day-1 Test 4 GC-correlation check (§17.1) — run it against the running process, doesn't require instrumenting the codebase. |
| Frontend (workspace demo) | **Next.js** + LiveKit's React SDK (`@livekit/components-react`) | LiveKit's own SDK is the path of least resistance for rendering participant presence, data-channel board state, and voice UI together. |
| Hosting | **Hugging Face Spaces** (backend + agents, Docker SDK) + **Vercel** (Next.js frontend) | Both free, both specified in §16. Spaces free tier has a CPU sleep/cold-start behavior — pre-warm before any live demo per the Risks section (§17). |
| CI | GitHub Actions free tier (public repo) | For running the Day-1 latency suite and the benchmark harness reproducibly, and for the collision-window-integrity assertion (§10.1) to run on every PR, not just locally. |

Nothing in this stack requires a credit card. The only operational risk is free-tier rate limits and Spaces cold starts, both already named as accepted, documented risks in §17.

---

## 2. Repo structure

```
quorum/
  bus/
    moss_client.py        # thin wrapper: sense(), claim(), write_finding(), write_claim()
    namespaces.py         # claims namespace, findings namespace, config for both
    schemas.py            # Claim, Finding, Conflict Pydantic models (§8, §15)
    tiebreak.py           # (participant_type_priority, ts_monotonic, participant_id) comparator (§9.2)
    versioning.py         # supersedes-chain resolution for sense() (§9.3)
  reaper/
    reaper.py             # heartbeat TTL scan loop, reason-coded reap (§8.1)
  adjudicator/
    adjudicator.py         # candidate-pair detection + LLM adjudication loop (§9)
    conflict_tray.py
  agents/
    harness.py             # enforces sense() -> claim() -> gate -> external calls ordering (§9.1)
    base_agent.py           # domain agent base class, heartbeat emission, step loop
    domain_agents/          # the 4 concrete research agents for the demo
  inference/
    router.py               # Groq -> OpenRouter -> HF rotation on 429 (§16)
    instructor_client.py     # Instructor-wrapped structured-output calls, 1 repair retry (§9)
  search/
    tavily_client.py         # external grounding calls, publishes board events (§9)
  transport/
    livekit_room.py          # room/token minting, data-channel board-state streaming (§5, §13)
    voice_bridge.py          # voice query -> sense() -> spoken response; voice claim -> claim() (§5)
  api/
    main.py                  # FastAPI app
    auth.py                  # OAuth2 password flow, JWT issuance/rotation (§13)
    routes/
      runs.py                # POST /runs, DELETE /runs/{id} full erasure (§13)
      bus.py                 # sense/claim/write/conflict HTTP surface (§12)
    middleware/
      rate_limit.py
      security_headers.py
  observability/
    otel_setup.py             # span attributes per §14
    ring_buffer.py             # benchmark-only latency capture, separate from OTel path
  benchmark/
    day1_latency_gate.py       # Tests 1-4 from §17.1, CI-runnable, exits non-zero on gate failure
    relay_arm.py                # conventional message-passing baseline for comparison
    board_arm_runner.py         # ten-iteration harness, raw JSON logs, variance reporting (§11)
    metrics/
      reap_rate.py
      collision_window_integrity.py
      sense_latency_vs_index_size.py
  frontend/                     # Next.js workspace demo
  db/
    models.py                   # SQLite schema: runs, consent events, replay logs
    erasure.py                   # full-erasure implementation for DELETE /runs/{id}
  config/
    settings.py                  # all config from env vars, no secrets in code (§13)
  tests/
  .github/workflows/
    day1-gate.yml
    ci.yml
```

---

## 3. Phase 1 (Day 1) — the gate, build this first and only this

Do not write agent, LiveKit, or API code before this passes. Implement `benchmark/day1_latency_gate.py` as a standalone script with no inference in the loop, and run all four tests against a real Moss instance with the seed corpus loaded.

- **Test 1 — Standalone baseline.** Single-threaded sense/write loop, N=1000+ iterations, report p50/p99. Targets: sense < 10ms, write < 15ms.
- **Test 2 — Concurrent-writer latency.** Spin up 5 asyncio tasks (matching 4 agents + Adjudicator) writing concurrently to the same claims namespace. Report p99 under contention. Gate: < 20ms, and < ~30% degradation vs. Test 1's p99.
- **Test 3 — Index-growth curve.** Run sense() at three index sizes: seed-corpus-only, seed+20 synthetic findings, seed+80 synthetic findings. Plot/report the curve. Gate: sense stays under 10ms at all three points.
- **Test 4 — GC/memory profile.** Wrap Test 1 and Test 2 with `py-spy record` (or `py-spy top` live). Report any GC pause correlated with a p99 outlier — informational, not a hard gate, but must be in the Day-1 report.

Wire this script into `.github/workflows/day1-gate.yml` so it's re-runnable, not a one-time manual check — the gate should be able to fail a later PR too if a schema or dependency change regresses it.

**If any gating test fails:** implement the named fallback before proceeding — phase-boundary sensing (claim() called only at subtask boundaries, not every step), same Claim/Finding schemas, and drop `transport/voice_bridge.py`'s mid-run query feature from the build plan entirely. Do not attempt to "optimize around" a failed gate; the fallback is the correct response per §17.1.

---

## 4. Phase 2 (Days 2–4) — bus, harness, Reaper, agents, transport

Build in this order; each step depends on the previous one working:

1. **`bus/schemas.py`** — Claim and Finding Pydantic models exactly as specified in §8/§15, including `last_heartbeat`, `supersedes`, `participant_type`, `status` with the three-plus-reap-reason states.
2. **`bus/moss_client.py` + `bus/namespaces.py`** — two namespaces, sense()/claim()/write() as async functions wrapping Moss calls directly, same process, no HTTP hop.
3. **`bus/tiebreak.py`** — implement `(participant_type_priority, ts_monotonic, participant_id)` with the 50ms human-priority epsilon exactly as in §9.2. Unit-test the epsilon boundary explicitly (49ms vs 51ms cases).
4. **`bus/versioning.py`** — chain-head resolution for `supersedes` per §9.3. Unit test: a 3-link chain returns only the head from sense().
5. **`agents/harness.py`** — this is the enforcement point for §9.1. Implement the step contract as a state machine or decorator that raises in dev/test mode if any external client (`inference/router.py`, `search/tavily_client.py`) is called before `claim()` resolves. This should be a real runtime assertion, not a comment.
6. **`reaper/reaper.py`** — 1-second scan loop, 15s TTL, reason-coded reap (`heartbeat_timeout`), re-opens the claim. Write this as its own coroutine/participant loop from day one, not bolted on later — it needs to run concurrently with everything else during the Day 2-4 agent testing, or heartbeat bugs won't surface until the full benchmark run.
7. **`adjudicator/adjudicator.py`** — separate loop watching findings namespace, fast similarity detection, slow LLM adjudication via `inference/router.py`, writes to conflict tray, never auto-resolves.
8. **`inference/router.py`** — Groq primary, OpenRouter failover, HF third, rotate on 429/5xx. Wrap with `instructor_client.py` for structured output, one repair retry.
9. **`transport/livekit_room.py`** — board state over data channels (replaces SSE entirely per §5), server-side scoped room tokens (§13).
10. **`transport/voice_bridge.py`** — only if Day 1 gate passed. Voice query → sense() → TTS response; voice claim → claim() with human `participant_type`.
11. Four domain agents in `agents/domain_agents/` implementing `base_agent.py`'s step loop against the enforced harness.

---

## 5. Phase 3 (Day 5) — baseline arm and experiments

- `benchmark/relay_arm.py` — a conventional fixed-graph message-passing implementation of the *same* four-agent research task, same models, same tools, same question. This is the control.
- `benchmark/board_arm_runner.py` — ten iterations per arm, same fixed question, raw JSON logs committed per iteration, variance reported alongside means (§11). Threshold fixed before this runs, not tuned after.
- Metrics scripts (`benchmark/metrics/`) compute: retrieval latency p50/p99, redundancy eliminated (tokens + search calls), collision rate at native and +200ms-injected write latency, reap rate, collision-window integrity (should read 100%; anything else means the harness guard fired — investigate before reporting), sense-latency-vs-index-size curve, and the answer-quality guardrail score for both arms.

---

## 6. Phase 4 (Days 6–7) — polish and ship

- Architecture diagram (single process box containing agents + bus + Reaper + Adjudicator + Moss index; LiveKit as the external transport edge; API/auth/observability as surrounding services).
- Final document assembled from the PRD plus the actual Day-1 and Day-5 numbers (do not pre-write the numbers — report what the gate and benchmark actually produced, including if token savings came out lower than Moss's published 70–90% figure, per §10 and §17).
- Demo video: primary path is a replayed stored run (`db/models.py` replay support) with a pre-warmed Spaces instance; live run only as backup, per the "demo fails live" risk mitigation.
- Deployment: HF Spaces (Docker SDK, backend+agents), Vercel (Next.js frontend), both configured entirely from env vars, no secrets committed.

---

## 7. Non-negotiables checklist (the agent building this should self-check against this before considering any phase "done")

- [ ] Moss is never placed behind an internal HTTP service, at any point in the stack.
- [ ] `agents/harness.py`'s ordering guard actually throws in dev/test mode on a violation — not just documented, tested.
- [ ] Reaper and Adjudicator are separate coroutine loops, not called synchronously inside the bus write path.
- [ ] Every request/response body validated by a strict, no-extra-fields Pydantic v2 model.
- [ ] `DELETE /runs/{id}` actually drops the Moss namespace, deletes SQLite rows, and tombstones telemetry spans — verify this with a test that checks all three, not just one.
- [ ] Voice audio is transcribed and discarded — verify no audio bytes persist past the transcription step.
- [ ] The in-process ring buffer and the OpenTelemetry path are genuinely separate code paths, not the same buffer feeding both.
- [ ] The Day-1 gate script is re-runnable in CI, not a one-off notebook.
- [ ] No paid tier, credit card, or non-free service appears anywhere in `config/settings.py` or the deployment manifests.

---

Build in the order given. Do not start Phase 2 before Phase 1's gate has produced a real pass/fail result on real hardware.