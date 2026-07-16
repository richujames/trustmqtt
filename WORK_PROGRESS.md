# TrustMQTT — Work Progress

A single, indexed source of truth for **what is done** and **what remains**. Status is tied to the build phases in [`trustmqtt/docs/SPEC.md §10`](./trustmqtt/docs/SPEC.md) and to the audit in the [main README §12](./README.md#12-audit-findings--improvements).

**Status legend:** ✅ done & verified · 🟡 partial / needs live-infra validation · ⬜ not started

**Last updated:** 2026-07-16

---

## Index

1. [Snapshot](#1-snapshot)
2. [Completed work](#2-completed-work)
   - 2.1 [Build phases (SPEC §10)](#21-build-phases-spec-10)
   - 2.2 [Component checklist](#22-component-checklist)
   - 2.3 [This pass — audit, CI & improvements](#23-this-pass--audit-ci--improvements)
3. [Work remaining](#3-work-remaining)
   - 3.1 [High priority (robustness / correctness)](#31-high-priority-robustness--correctness)
   - 3.2 [Medium priority](#32-medium-priority)
   - 3.3 [Low priority](#33-low-priority)
   - 3.4 [Validation & benchmarking (needs live infra)](#34-validation--benchmarking-needs-live-infra)
4. [Traceability](#4-traceability)

---

## 1. Snapshot

| Area | Status |
|---|---|
| C plugin (event capture, cache, enforcement) | ✅ implemented, unit-tested |
| Python worker (ingest → features → FSM → drift → fleet → policy → verdicts) | ✅ implemented, unit-tested |
| Incidents + redaction + LLM reporting | ✅ implemented, unit-tested |
| Grafana dashboards + Postgres schema/migrations | ✅ provisioned |
| Benchmark / eval harness | 🟡 implemented; not yet run end-to-end on public datasets |
| Automated tests | ✅ 79 Python + 4 C suites passing |
| CI | ✅ GitHub Actions (this pass) |
| Production hardening (event-loop, crash-recovery, memory) | ⬜ see [§3](#3-work-remaining) |

**Test status:** 79 Python tests + 4 C test suites (`ring`, `verdict_cache`, `token_bucket`, `enforce`) all green, locally and in CI.

---

## 2. Completed work

### 2.1 Build phases (SPEC §10)

| Phase | Scope | Status |
|---|---|---|
| **0 — Environment** | `docker-compose.yml` (mosquitto 2.1.x built from source, redis, postgres, grafana, worker), CI build+test | ✅ (CI added this pass) |
| **1 — Event capture (C)** | Callbacks for connect/disconnect/offline/message/sub/unsub/auth/acl/tick, ring buffer, emitter thread, `ka_gap` logic, `enforce`/`monitor`/`fingerprint` modes | ✅ |
| **2 — Ingest + features + storage** | `ingest.py` (consumer group + dead-letter), `features.py` (60 s tumbling windows, 21 features), `storage.py` + Alembic migrations | ✅ |
| **3 — Behavioral Contract Engine** | `fsm.py` first-order Markov contract, topic-class normalizer, learning lifecycle, FSM-diff export | ✅ |
| **4 — Drift scorer + trainer** | `drift.py` (IsolationForest + OneClassSVM, robust scaler), `train.py` per-cohort trainer, hot-reload via `tmq:models:meta` | ✅ |
| **5 — Policy + verdicts + enforcement** | `policy.py` (fusion + hysteresis + KICK gating), `verdicts.py` (packed+hash MULTI), plugin verdict cache + ACL enforcement + token bucket + KICK | ✅ |
| **6 — Fleet drift + fingerprint mode** | `fleet.py` (Welford baseline, coordinated alarm), `--fingerprint-only` + fingerprint docs, plugin `fingerprint` passthrough | ✅ |
| **7 — Incidents, redaction, LLM, Grafana** | `incidents.py`, `redact.py` (unit-tested), `llm.py` (NVIDIA NIM + deterministic fallback), 3 Grafana dashboards | ✅ |
| **8 — Benchmark harness** | `replay.py` + YAML scenarios (hijack, scope-expansion, slow-rate, coordinated-30) + `make eval` | 🟡 implemented; end-to-end run on MQTTset / MQTT-IoT-IDS2020 still pending ([§3.4](#34-validation--benchmarking-needs-live-infra)) |

### 2.2 Component checklist

**C plugin (`trustmqtt/plugin/`)** — ✅
- Event taps + JSON serialization (cJSON), non-blocking callbacks
- Lock-protected ring buffer + background emitter thread (`XADD`, exponential backoff, fail-open)
- Keepalive-conformance `ka_gap` synthetic events
- rwlock verdict cache, TICK-driven refresh, TTL decay
- ACL enforcement: THROTTLE (token bucket), QUARANTINE, KICK
- Unit tests: `test_ring`, `test_verdict_cache`, `test_token_bucket`, `test_enforce`

**Python worker (`trustmqtt/tmq_worker/`)** — ✅
- `ingest` (pydantic validation, dead-letter), `features`, `fsm`, `drift`, `fleet`, `policy`, `verdicts`
- `incidents` + `redact` + `llm`, `storage` (SQLAlchemy), `train`, `replay`, `config`
- `__main__` asyncio supervisor (ingest / sweep / fingerprint-export loops)

**Ops** — ✅
- Alembic migrations (`0001_initial`), Grafana provisioning (fleet-overview / client-drilldown / system-health), traffic simulator (devices + attack scripts)

### 2.3 This pass — audit, CI & improvements

All verified (see [main README §12.1](./README.md#121-improvements-made-verified)):

- ✅ **CI** — GitHub Actions (`.github/workflows/ci.yml`): C tests, Python tests, compose lint
- ✅ **Build fix** — `plugin/tests/Makefile` `-D_GNU_SOURCE` (C tests couldn't compile on modern glibc)
- ✅ **Efficiency** — incremental FSM vocabulary (hot path); batched `XACK`; incident report regenerated only on escalation
- ✅ **Security** — `train.py` cohort→filename sanitization (path-traversal → pickle-RCE closed)
- ✅ **Novelty** — opt-in evidence-availability–aware trust fusion (`policy.py`, off by default)
- ✅ **Docs** — comprehensive root README with full audit section
- ✅ **Cleanup** — removed stale `PROJECT_PROGRESS_NOTE.md` (described a non-existent old layout) and the superseded `SYSTEM_ARCHITECTURE.md` stub

---

## 3. Work remaining

Sourced from the audit ([main README §12.2](./README.md#122-findings-recommended-for-a-follow-up-pass-not-yet-changed)) and SPEC phase DoDs that need live infrastructure.

### 3.1 High priority (robustness / correctness)

- ⬜ **Offload scoring from the asyncio event loop.** `on_event → _handle_closed_window` (DB commits, verdict writes, LLM calls) runs on the event-loop thread; one slow window stalls all ingestion. → run scoring in a worker thread; queue report generation.
- ⬜ **Crash recovery for Redis Streams.** Use a stable consumer name + `XAUTOCLAIM`/`XPENDING` so entries un-ACKed at crash time are reclaimed (today they orphan forever).
- ⬜ **Exception handling around Redis/DB I/O** in `ingest_loop` and `_handle_closed_window` — a transient disconnect currently terminates the worker. → try/except + backoff, fail-open per window.

### 3.2 Medium priority

- ⬜ **Export/ingest thread race.** `fingerprint_export_loop` mutates shared state (`bce._fsms`, `_feature_history`, `_daily_snapshots`, SQLAlchemy session) concurrently with ingest. → single-thread or lock.
- ⬜ **Redaction defense-in-depth.** Route `reason` and `window_stats` through `redact_incident_summary` so nothing reaching `build_prompt` is unredacted.
- ⬜ **Per-client memory eviction.** `_client_username`, `_feature_history`, `_daily_snapshots`, `_known_topics` never evict. → evict on disconnect / long-idle (note: `_known_topics` must survive the short idle sweep).
- ⬜ **Incident rehydration on restart.** `_open_incident_ids` is in-memory only → reload open (`closed_ts IS NULL`) incidents at startup to avoid duplicates.

### 3.3 Low priority

- ⬜ **Reject default `REDACTION_SECRET` in `enforce` mode**; widen HMAC pseudonym beyond 4 hex (collision risk at fleet scale).
- ⬜ **`train.py`**: stream `feature_windows` (`yield_per`) and close the session; make the Postgres + `tmq:models:meta` writes ordered/atomic.
- ⬜ **`replay.py`**: index events by client_id to remove the O(attacks × clients × events) relabeling.

### 3.4 Validation & benchmarking (needs live infra)

- ⬜ **End-to-end stack run** (`make up`) validated against the SPEC DoDs (broker up with plugin, events flowing, verdicts enforced).
- ⬜ **Load test:** 500 concurrent clients / 2,000 msg/s; confirm < 50 µs plugin hot-path overhead and drop-counter = 0.
- ⬜ **Detection benchmark:** run `replay.py` adapters on **MQTTset** and **MQTT-IoT-IDS2020**; target AUC > 0.9; publish detection + enforcement metric tables to `eval/results/`.
- ⬜ **Grafana dashboards** validated against live data.
- ⬜ **CI extension (optional):** add an integration job that boots the compose stack and runs a smoke scenario.

---

## 4. Traceability

- **Architecture & contracts:** [`trustmqtt/docs/SPEC.md`](./trustmqtt/docs/SPEC.md)
- **Practical guide + audit:** [main README](./README.md)
- **Test status:** `cd trustmqtt && make test` (or see CI runs)
- Each remaining item above maps 1:1 to an audit finding or a SPEC §10 phase DoD, so this file stays in sync as items are closed.
