# TrustMQTT

[![CI](https://github.com/richujames/trustmqtt/actions/workflows/ci.yml/badge.svg)](https://github.com/richujames/trustmqtt/actions/workflows/ci.yml)

> **Zero-trust, continuous behavioral-identity verification for MQTT brokers.**
> MQTT 5's authentication answers *"is this the right device?"*. TrustMQTT answers *"is this device still behaving like itself?"* — and applies graduated, automatic countermeasures the moment it isn't, **inside the broker**, in microseconds.

TrustMQTT hooks a custom C plugin directly into Eclipse Mosquitto 2.1.x, streams resolved-semantic events (never raw payloads) to a Python scoring worker, learns a per-client behavioral contract, fuses three anomaly signals into a single trust score, and enforces a graduated response — `ALLOW → WATCH → THROTTLE → QUARANTINE → KICK` — back on the broker's own authorization path.

> 📁 **The project lives in the [`trustmqtt/`](./trustmqtt/) directory.** Run every command below from there (`cd trustmqtt` first). The repository root holds only this README, the CI workflow (`.github/`), and the project folder.

- **Full technical spec** (schemas, thresholds, build phases, data contracts): [`trustmqtt/docs/SPEC.md`](./trustmqtt/docs/SPEC.md)
- **This README** is the complete practical guide: what it is, how it works, how to run it, and a documented **audit + the efficiency / security / novelty improvements made in this pass** (see [§12](#12-audit-findings--improvements)).
- **Project status** — completed work and what remains, indexed: [`WORK_PROGRESS.md`](./WORK_PROGRESS.md)

---

## Table of contents

1. [The idea](#1-the-idea)
2. [Threat model](#2-threat-model)
3. [Architecture](#3-architecture)
4. [How detection works](#4-how-detection-works)
5. [How enforcement works](#5-how-enforcement-works)
6. [Incident reporting, redaction & the LLM](#6-incident-reporting-redaction--the-llm)
7. [Repository layout](#7-repository-layout)
8. [Configuration reference](#8-configuration-reference)
9. [Setup & execution](#9-setup--execution)
10. [Data contracts (quick reference)](#10-data-contracts-quick-reference)
11. [Technical novelty](#11-technical-novelty)
12. [Audit findings & improvements](#12-audit-findings--improvements)
13. [Testing](#13-testing)
14. [Operational modes](#14-operational-modes)
15. [Limitations & roadmap](#15-limitations--roadmap)

---

## 1. The idea

A stolen credential, a compromised-but-still-authenticated device, or a slow-drift attacker all pass MQTT authentication perfectly — because the credential *is* valid. Static ACLs don't help either: the attacker operates within the permissions the device already has.

TrustMQTT closes that gap by treating **behavior as a continuous second factor**. Every authenticated client has a learned *behavioral contract* — the topics it publishes to, the order in which it does things, its message rate, its keepalive rhythm, its QoS mix. When live behavior stops fitting that contract, trust decays and the broker automatically throttles, quarantines, or kicks the client — with hysteresis so a single noisy minute never causes a false kick, and with a full audit trail explaining every decision.

Two properties make this more than "another IDS":

- **It acts, not just alerts.** Detection and *in-broker enforcement* are one system. The differentiator is time-to-mitigation, not just F1 score.
- **It never inspects payloads.** All analysis is *resolved-semantic-event analysis* — topic, QoS, retain, session state, keepalive, MQTT 5 properties. Privacy-preserving by construction, and honest about what a Mosquitto plugin can and cannot observe.

---

## 2. Threat model

**Detects:**

| Threat | Signal that catches it |
|---|---|
| Compromised-but-authenticated device (valid creds, new topics/rates/sequences) | FSM contract violation + statistical drift |
| Credential theft / device impersonation (right creds, wrong fingerprint) | FSM + drift diverge from the learned per-client baseline |
| Slow-drift abuse (gradual escalation designed to stay under static ACLs) | Drift scorer over tumbling windows + slow-adapting contract |
| Coordinated fleet compromise (many devices shifting at once, botnet-style) | Fleet-level drift (synchronized same-signed z-scores) |

**Explicitly out of scope (v2):** payload-content inspection, wire-level frame analysis (DUP flag, packet IDs, PUBACK/PUBREC reason codes, PINGREQ timing, topic-alias usage — *none of these are exposed by the Mosquitto plugin API*, and this is documented verbatim in `plugin.c` and `features.py`), TLS-termination attacks, and broker-host compromise.

**Non-functional guarantees:**

| Requirement | Target |
|---|---|
| Broker overhead per event (plugin hot path) | < 50 µs added latency, **zero blocking I/O in callbacks** |
| Event delivery to worker | ≤ 1 s (Redis Stream) |
| Verdict propagation (worker → plugin cache) | ≤ 1 s (TICK-driven refresh) |
| Availability | **Plugin failure must never crash the broker** — every callback wraps errors, degrades to `ALLOW` + log |
| Fail posture | **Fail-open**: if Redis is down the broker keeps serving; events buffered/dropped with a counter; verdicts freeze at last-known value, then TTL-decay one level per 60 s to `ALLOW` |

---

## 3. Architecture

```
                         ┌──────────────────────────────────────────────┐
                         │              Mosquitto 2.1.x                  │
  MQTT clients ────────► │  ┌──────────── trustmqtt_plugin.so ─────────┐ │
                         │  │  Event taps: CONNECT / DISCONNECT /       │ │
                         │  │  CLIENT_OFFLINE / MESSAGE_IN / SUBSCRIBE /│ │
                         │  │  UNSUBSCRIBE / BASIC_AUTH / ACL_CHECK /   │ │
                         │  │  TICK                                     │ │
                         │  │                                           │ │
                         │  │  [emitter thread] ──JSON──► Redis Stream  │ │
                         │  │  [verdict cache]  ◄──packed str── Redis   │ │
                         │  │  [enforcer] ACL_CHECK → ALLOW / WATCH /   │ │
                         │  │             THROTTLE / QUARANTINE / KICK  │ │
                         │  └───────────────────────────────────────────┘ │
                         └───────────────────────┬────────────────────────┘
                                                 │
                                          ┌──────▼──────┐
                                          │    Redis    │  Streams + KV/hashes
                                          └──────┬──────┘
                                                 │ consumer group `tmqw`
        ┌────────────────────────────────────────▼────────────────────────────────┐
        │                       Python scoring worker (tmq-worker)                 │
        │  ingest → features (60 s windows) → ┌ FSM (Behavioral Contract Engine) ┐ │
        │                                     ├ Drift (IsolationForest + OCSVM) ─┤ │
        │                                     └ Fleet baseline (coordinated) ────┘ │
        │                            → Policy (trust fusion + hysteresis) ──verdicts──► Redis
        │                            → Incidents (redact → NVIDIA NIM) ──► Postgres │
        └──────────────────────────────────────┬───────────────────────────────────┘
                                                │
                                         ┌──────▼──────┐
                                         │   Grafana   │  (Postgres + Redis datasources)
                                         └─────────────┘
```

**Two decoupled paths, by design:**

- **Out-path (asynchronous, never blocks the broker):** callbacks serialize an event with cJSON, push it onto a lock-protected ring buffer, and return. A dedicated emitter thread drains the ring every ~100 ms and pipelines `XADD` to Redis. On the worker side, `ingest → features → FSM/drift/fleet → policy` runs per client, per 60 s window.
- **In-path (synchronous, off a local cache):** the worker writes verdicts to Redis; the plugin's `TICK` handler refreshes an in-memory verdict cache (rwlock-guarded) on a timer; the `ACL_CHECK` callback consults that cache and enforces **without any network I/O on the hot path**.

**Deployment:** one `docker-compose.yml`, five services on a single bridge network `tmqnet`:

| Service | Image / build | Role |
|---|---|---|
| `mosquitto` | built from source (2.1.x + plugin) | broker + enforcement |
| `redis` | `redis:7-alpine` | event stream + verdict KV |
| `postgres` | `postgres:16-alpine` | audit store (source of truth) |
| `tmq-worker` | Python 3.11 (built) | scoring, policy, incidents |
| `grafana` | `grafana/grafana:10.4.2` | dashboards |

---

## 4. How detection works

The worker scores each client over **tumbling 60 s windows** (`window_s`, configurable). Three independent signals feed one trust score.

### 4.1 Behavioral Contract Engine (FSM) — `fsm.py`

- **Alphabet:** each event maps to a symbol — `CONNECT`, `DISCONNECT`, `OFFLINE`, `KA_GAP`, `SUB(topic-class)`, `UNSUB(topic-class)`, `PUB(topic-class, qos)`. Topics are normalized to a *class* by replacing numeric-suffixed identifier segments (`line2`, `sensor7`) with `+`, so `plant-a/line2/temp` → `plant-a/+/temp`. This keeps the state space finite without collapsing genuinely distinct topics.
- **Model:** a per-client, first-order Markov transition matrix `P(symₜ | symₜ₋₁)` with **Laplace smoothing**, learned during a per-client learning period (first 2,000 events *or* 24 h, whichever comes first). After learning, counts keep updating with exponential decay (`α = 0.01`) so legitimate slow drift adapts the contract instead of alarming forever.
- **Violation score:** per transition, `viol = min(1, −log P / −log 1e-4)`; aggregated per window as the **p95** of violations. A never-before-seen transition scores near 1.0; a habitual one scores near 0.
- **Interpretability (FSM-diff):** the set of transitions observed this window whose learned probability is below `0.01` is exported to Postgres and rendered in Grafana — so an operator sees exactly *which* novel behavior triggered an escalation.

### 4.2 Statistical Drift Scorer — `drift.py`

- **Feature vector (21 features):** message/byte rate, inter-arrival mean/std, unique-topic count, new-topic ratio, QoS mix, retain ratio, subscription churn, topic entropy, payload-length stats, keepalive conformance, silent-but-alive ratio, connect/disconnect counts, and the FSM violation score joined in.
- **Models:** `IsolationForest(n_estimators=200, contamination=0.02)` + `OneClassSVM(kernel="rbf", nu=0.05)`, each normalized to `[0,1]` over training scores and **fused 50/50**. Features are standardized with a robust (median/IQR) scaler fit at training time.
- **Cohorts:** one model per cohort (default = `username`; `--per-client` optional). Models are trained offline (`make train`) and **hot-reloaded** by the running worker when the `tmq:models:meta` hash changes — no restart.
- **Cold start:** until a cohort model exists, `drift = 0` and only FSM + fleet act. *(This cold-start dilution is exactly what the new [adaptive-fusion](#11-technical-novelty) option addresses.)*

### 4.3 Fleet-level drift — `fleet.py`

- Maintains a **fleet-wide** running mean/std per feature (Welford's online algorithm — O(1) memory, numerically stable).
- **Coordinated-drift alarm:** if ≥ 20 % (`coordinated_fraction`) of active clients show a **same-signed** z-score above 2.0 on the same feature in the same window, it raises a fleet incident and boosts every affected client's trust score. This catches botnet-style synchronized compromise that per-client scoring alone misses.

### 4.4 Trust fusion & policy — `policy.py`

```
T = w_fsm·fsm_violation + w_drift·drift + w_fleet·fleet_component
defaults:  w_fsm = 0.45,  w_drift = 0.35,  w_fleet = 0.20   (all YAML-configurable)
```

Thresholds map `T` onto a verdict level, with **hysteresis**:

| Trust `T` | Verdict | Notes |
|---|---|---|
| `< 0.30` | **ALLOW** | normal ACLs decide |
| `0.30–0.50` | **WATCH** | flagged in dashboard, no enforcement |
| `0.50–0.70` | **THROTTLE** | token bucket at `R = max(1, baseline_rate · (1−T))` msg/s |
| `0.70–0.85` | **QUARANTINE** | publish only to `tmq/quarantine/#`; deny new subscribes |
| `≥ 0.85` | **KICK** | disconnect on next TICK, then demote to QUARANTINE |

- **Escalation is immediate; de-escalation is slow** — a level only drops after `N = 3` consecutive windows below the lower threshold (minus a `0.05` margin).
- **KICK gating:** a single window ≥ 0.95 kicks immediately; a window in `[0.85, 0.95)` needs **two consecutive** such windows — protecting against a single-window false kick.
- **New clients are capped at WATCH** while their contract is still learning (unless trust blows past the 0.95 hard ceiling).

---

## 5. How enforcement works

Enforcement lives entirely in the plugin's `ACL_CHECK` handler, reading the local verdict cache (`enforce.c`):

- **ALLOW / WATCH** → `MOSQ_ERR_PLUGIN_DEFER` (let the broker's normal ACLs decide).
- **THROTTLE** → token bucket on publishes: refill `R` tokens/s, burst cap `2R`; a publish with no token available is denied. Reads/subscribes pass through.
- **QUARANTINE** → deny all publishes except to `tmq/quarantine/#`; deny all new subscribes.
- **KICK** → the `TICK` loop calls `mosquitto_kick_client_by_clientid()` (no Will), emits an `enforcement` event, then locally demotes the client to QUARANTINE to contain reconnect storms.

The verdict cache is refreshed on `TICK` by pipelining `GET tmq:verdictp:<client_id>` for connected clients and parsing the packed `L|S|E|R` string. If Redis is unreachable, an exponential backoff kicks in and verdicts **decay one level per 60 s toward ALLOW** — fail-open, but not instantly.

---

## 6. Incident reporting, redaction & the LLM

When a client reaches `THROTTLE` or above, or a fleet alarm fires, the **incident service** (`incidents.py`):

1. Persists the incident to Postgres **first** (source of truth).
2. Builds a summary struct (scores, level history, FSM-diff transition names, window stats) — **never raw events**.
3. Runs it through the **mandatory redaction layer** (`redact.py`): IPs masked to `/24`, client IDs / usernames pseudonymized via HMAC-SHA256, secret topic segments (e.g. `+/credentials/#`) replaced with `⟦redacted⟧`, payload hashes dropped.
4. Asks **NVIDIA NIM** (OpenAI-compatible chat-completions API) for a plain-language operator report — with a **deterministic template fallback** if the LLM is disabled, slow, or unreachable. The report generation **never gates scoring** (10 s timeout, and after this pass, only regenerated on escalation — see [§12](#12-audit-findings--improvements)).

Redaction is architecturally enforced: `redact.py` is the only sanctioned bridge to `llm.py`, and `llm.py` never imports from `ingest.py` or touches the raw event stream.

---

## 7. Repository layout

From the repository root, everything lives under [`trustmqtt/`](./trustmqtt/) (plus `.github/workflows/ci.yml` for CI and the project proposal doc):

```
trustmqtt/
├── docker-compose.yml            # 5 services on bridge net tmqnet
├── Makefile                      # build / up / down / test / train / eval / fingerprint
├── docs/SPEC.md                  # full technical specification (source of truth)
├── config/tmq.yaml               # all thresholds, weights, windows (no magic numbers in code)
├── docker/mosquitto/             # Dockerfile builds Mosquitto 2.1.x from source + plugin
├── plugin/                       # Member A — C plugin
│   ├── src/{plugin,emitter,verdict_cache,enforce,ring}.c
│   ├── include/*.h
│   └── tests/                    # unit tests: ring, verdict cache, token bucket, enforce
├── tmq_worker/                   # Member B — Python service
│   ├── __main__.py               # asyncio supervisor (ingest → score → verdict/incident)
│   ├── config.py ingest.py features.py fsm.py drift.py fleet.py
│   ├── policy.py verdicts.py incidents.py redact.py llm.py storage.py
│   ├── train.py                  # offline per-cohort drift trainer CLI
│   ├── replay.py                 # benchmark / eval harness driver
│   └── tests/                    # 79 pytest cases
├── traffic-simulator/            # normal + adversarial MQTT traffic generators
│   ├── devices/                  # temperature/motion/door-lock device models
│   └── attacks/                  # flood, spoof, replay, recon, credential-replay, drift, ...
├── eval/scenarios/               # YAML attack scenarios (hijack, scope-expansion, coordinated drift)
├── grafana/provisioning/         # fleet-overview, client-drilldown, system-health dashboards
└── migrations/                   # Alembic (Postgres schema)
```

---

## 8. Configuration reference

All tunables live in [`trustmqtt/config/tmq.yaml`](./trustmqtt/config/tmq.yaml) (mounted into the worker). Secrets and connection strings come from the environment (12-factor).

```yaml
window_s: 60                                    # tumbling feature-window length
learning: {max_events: 2000, max_hours: 24}     # per-client FSM learning period
weights: {fsm: 0.45, drift: 0.35, fleet: 0.20}  # trust-score fusion weights
thresholds: {watch: 0.30, throttle: 0.50, quarantine: 0.70, kick: 0.85, kick_single: 0.95}
hysteresis: {deescalate_windows: 3, margin: 0.05}
fleet: {coordinated_fraction: 0.20, z_trigger: 2.0}
topic_class: {numeric_suffix_regex: "^[a-z]*\\d+$"}
redaction: {secret_topic_patterns: ["+/credentials/#", "+/keys/#"]}
llm: {model: "meta/llama-3.1-8b-instruct", timeout_s: 10, enabled: true}   # NVIDIA NIM
mode: enforce                                    # enforce | monitor | fingerprint
adaptive_fusion: false                           # NEW: evidence-availability-aware fusion (§11)
```

**Environment** (`.env`, copy from `.env.example`):

| Variable | Purpose |
|---|---|
| `POSTGRES_DB/USER/PASSWORD` | Postgres credentials |
| `DATABASE_URL` | SQLAlchemy URL (must match the Postgres vars) |
| `REDIS_HOST/PORT` | Redis connection |
| `NVIDIA_API_KEY` | NVIDIA NIM key (optional — falls back to deterministic report) |
| `NVIDIA_BASE_URL` | NIM endpoint (default `https://integrate.api.nvidia.com/v1`) |
| `REDACTION_SECRET` | HMAC key for pseudonymizing client IDs — **change in any real deployment** |
| `GRAFANA_ADMIN_PASSWORD` | Grafana admin login |

**Plugin options** (in `mosquitto.conf`): `redis_host`, `redis_port`, `emit_batch_ms` (100), `verdict_refresh_ms` (500), `mode` (enforce/monitor/fingerprint), `payload_hash` (off/sha256).

---

## 9. Setup & execution

> **All commands below run from the `trustmqtt/` project directory:** `cd trustmqtt` first.

### Requirements
- Docker + Docker Compose
- (Optional) a free NVIDIA NIM API key from [build.nvidia.com](https://build.nvidia.com) for LLM-written incident reports
- For running tests locally without Docker: Python 3.11+, a C toolchain (`gcc`, `make`)

### 1. Configure
```bash
cd trustmqtt
cp .env.example .env      # then fill in NVIDIA_API_KEY and change REDACTION_SECRET if desired
```

### 2. Build & run the whole stack
```bash
make up                   # docker compose up -d --build
```
Brings up Mosquitto (2.1.x + plugin), Redis, Postgres, the scoring worker, and Grafana (`http://localhost:3000`, admin/admin by default).

### 3. Generate traffic and train a baseline
```bash
python traffic-simulator/run_simulation.py --duration 120   # normal traffic → FSM/feature baseline
make train                                                  # fit per-cohort drift models
```

### 4. Run the benchmark / eval harness
```bash
make eval                                                    # default scenario: credential-reuse hijack
make eval SCENARIO=eval/scenarios/coordinated_drift_30_clients.yaml
```
Reports **detection metrics** (precision/recall/F1, ROC-AUC vs. ground-truth labels) *and* the differentiator, **enforcement metrics** (time-to-mitigation, false-quarantine rate, benign-throughput retention). Results land in `eval/results/`.

### 5. Fingerprint-only mode (novel, enforcement-free)
```bash
make fingerprint          # exports per-client behavioral fingerprint docs, no enforcement
```

### 6. Tests
```bash
make test                 # C plugin unit tests + Python worker suite (see §13)
```

---

## 10. Data contracts (quick reference)

Full schemas in [`trustmqtt/docs/SPEC.md §4`](./trustmqtt/docs/SPEC.md). Highlights:

**Event JSON** (plugin → `tmq:events` stream): `{v, ts, event, client_id, username?, ip?, topic?, qos?, retain?, payload_len?, props?, sub_count?, gap_s?, ...}`. Topics are broker-**resolved** (aliases already resolved). Absent fields are omitted, not sent as null.

**Verdict** (worker → Redis, two representations written in one `MULTI`):
- Packed string `tmq:verdictp:<cid>` = `"L|S|E|R"` (level, score, expires-at, rate) — read by the plugin hot path, TTL 120 s.
- Hash `tmq:verdict:<cid>` — human/Grafana readable, same TTL.

**Redis keys:** `tmq:events` (stream), `tmq:events:dead` (malformed), `tmq:verdictp:*` / `tmq:verdict:*`, `tmq:fsm:*`, `tmq:feat:*`, `tmq:fleet:baseline` / `tmq:fleet:zbuf`, `tmq:models:meta`, `tmq:incidents`, `tmq:stats:plugin`.

**Postgres tables:** `clients`, `sessions`, `feature_windows`, `verdict_history`, `incidents`, `fingerprints`, `model_versions`. Every enforcement decision is reconstructible from `verdict_history` + `feature_windows` + `fsm_diff` — nothing enforces without a row that explains it.

---

## 11. Technical novelty

What distinguishes TrustMQTT from off-the-shelf MQTT IDS work:

1. **Broker-native behavioral contracts.** A per-client first-order Markov *contract* over a resolved-semantic-event alphabet, learned and slowly adapted inside the pipeline — not a post-hoc classifier over exported PCAPs. It produces a human-auditable FSM-diff, matching the interpretability of process-mining approaches while running live.
2. **Detection *and* graduated in-broker enforcement as one system.** Most IDS work stops at an alert. TrustMQTT's differentiator is a five-level automatic response (`WATCH→THROTTLE→QUARANTINE→KICK`) executed on the broker's own ACL path in microseconds, benchmarked on **time-to-mitigation**, not just F1.
3. **Fleet-level coordinated-drift detection.** Synchronized same-signed z-scores across the active fleet catch botnet-style compromise that per-client scoring structurally cannot.
4. **Standalone behavioral-fingerprinting mode.** A separable, independently benchmarkable contribution: export per-client behavioral fingerprints (FSM + feature baseline + a JS-divergence *stability* score) with enforcement fully disabled.
5. **Privacy-by-construction with a redacted LLM narrator.** Zero payload inspection, plus a mandatory, unit-tested redaction layer between incident data and the LLM — the report reads like a human wrote it without any identifying data ever leaving the process.

### 11.1 New in this pass — evidence-availability–aware trust fusion *(opt-in)*

The static fusion `T = w_fsm·fsm + w_drift·drift + w_fleet·fleet` has a subtle blind spot: whenever a detector has **nothing to say yet** — most importantly a cold-start cohort whose drift model is unfitted (`drift ≡ 0` by design) — that zero silently subtracts `w_drift = 0.35` of headroom from *every* score, delaying the first `WATCH`/`THROTTLE` for exactly the clients we know least about.

This pass adds **`renormalize_weights()`** in `policy.py`: trust becomes the confidence-weighted mean over only the *active* detectors, so an unavailable signal neither accuses nor exonerates a client. With all signals active it is mathematically identical to the static formula, so it is a **strict, opt-in generalization** (enabled via `adaptive_fusion: true`; **off by default** so the frozen §5.4 scoring semantics are preserved unless you choose otherwise). New unit tests (`test_policy.py`) prove both the equivalence and the cold-start improvement.

---

## 12. Audit findings & improvements

This repository was audited across the C plugin and the full Python worker. Below is exactly what was changed (all verified — see [§13](#13-testing)), followed by findings deliberately left as documented recommendations because they need integration-level work or would change scoring semantics.

### 12.1 Improvements made (verified)

| # | Area | Change | File(s) | Why it matters | Verified by |
|---|---|---|---|---|---|
| 1 | **Correctness / build** | The documented `make test` **could not compile** the C tests on modern glibc — `-std=c11` hides POSIX `pthread_rwlock_t` without a feature-test macro. Added `-D_GNU_SOURCE`. | `plugin/tests/Makefile` | The whole C test target was broken; now all 4 suites build and pass. | `make test` → 4/4 C suites OK |
| 2 | **Efficiency (hot path)** | FSM `_vocab_size()` rebuilt a set over the *entire* transition matrix on **every** `_prob()` call — i.e. per event and per top-k/novel scan (O(states²)-ish per window). Now maintained **incrementally** in O(1). | `tmq_worker/fsm.py` | The single hottest per-event computation in the scoring loop. | Proven identical to a full rebuild across 5,000 randomized events; existing FSM tests unchanged |
| 3 | **Efficiency (I/O)** | Ingest issued **one `XACK` per stream entry** (up to 512 Redis round-trips per batch). Now a single batched `XACK … id1 id2 …`. | `tmq_worker/ingest.py` | Removes per-event Redis round-trips from the ingest loop. | Full suite green |
| 4 | **Efficiency / robustness** | The incident report (a **blocking LLM call, up to 10 s**) was regenerated on *every* window an incident stayed open. Now only on **open or escalation**. | `tmq_worker/incidents.py` | A long-running incident could otherwise stall ingestion repeatedly and burn LLM budget. | `test_incidents.py` green |
| 5 | **Security** | Drift-model filenames were built from the **untrusted cohort label** (`client_id`/`username`): a cohort like `../../etc/cron.d/x` escaped `models_dir` on save and was re-loaded via **pickle** by every worker — an arbitrary-write + code-exec sink. Now sanitized to a safe slug + content hash. | `tmq_worker/train.py` | Closes a path-traversal → RCE vector. | Direct test: `../../etc/cron.d/x` → safe basename; distinct cohorts never collide |
| 6 | **Novelty** | Added opt-in **evidence-availability–aware fusion** ([§11.1](#111-new-in-this-pass--evidence-availabilityaware-trust-fusion-opt-in)). | `policy.py`, `config.py`, `__main__.py` | Fixes cold-start under-scoring without changing default behavior. | 2 new `test_policy.py` cases |

### 12.2 Findings recommended for a follow-up pass (not yet changed)

These are real and worth fixing, but each needs integration infrastructure to verify safely or would alter the frozen scoring semantics, so they are documented rather than silently changed:

- **[High] Blocking work on the asyncio event loop.** Only `read_batch` is offloaded to a thread; the whole `on_event → _handle_closed_window` chain (SQLAlchemy commits, verdict writes, incident/LLM calls) runs *on* the event-loop thread. One slow window stalls all ingestion. *Fix:* run scoring in a worker thread and defer report generation to a queue. (Improvement #4 above already removes the worst repeated offender.)
- **[High] No pending-entry (PEL) recovery + unstable consumer name.** `read_batch` only reads new messages (`>`), and the consumer name is `worker-<pid>`; a crash mid-batch orphans un-ACKed entries forever. *Fix:* stable consumer name + an `XAUTOCLAIM`/`XPENDING` reclaim path.
- **[High] No exception handling around Redis/DB I/O in the loops.** A transient Redis disconnect propagates out of `ingest_loop` and terminates the process. *Fix:* wrap reads and per-window processing in try/except with backoff (fail-open, skip the window).
- **[Med] Export thread races the ingest thread** over shared, non-thread-safe state (`bce._fsms`, `_feature_history`, `_daily_snapshots`, the SQLAlchemy session). *Fix:* run export on the scoring thread or guard shared state with a lock.
- **[Med] Redaction defense-in-depth.** `reason` and `window_stats` reach the LLM prompt without passing through `redact_incident_summary` (today both are safe internal strings, but a future policy `reason` embedding a topic/client-id would leak). *Fix:* let `redact.py` own every field that flows into `build_prompt`.
- **[Med] Unbounded per-client memory.** `_client_username`, `_feature_history`, `_daily_snapshots`, and `FeatureWindowManager._known_topics` are keyed by `client_id` and never evicted (note: `_known_topics` *must* persist across the short idle sweep — a test depends on it — so eviction should be tied to a genuine disconnect or a long TTL). *Fix:* evict on disconnect/long-idle.
- **[Med] Incident-service restart duplicates incidents.** `_open_incident_ids` is in-memory only; a restart opens a second incident for a client that already has an open one. *Fix:* rehydrate from `closed_ts IS NULL` at startup.
- **[Low] `redaction_secret` defaults to a public literal** and `pseudonymize` truncates HMAC to 4 hex (65k space → collisions at fleet scale). *Fix:* refuse the default secret in `enforce` mode; widen the pseudonym.
- **[Low] `train.py` loads the whole `feature_windows` table** and never closes its session; **`replay.py`** relabeling is quadratic in (attacks × clients × events). Offline paths, low impact.

---

## 13. Testing

```bash
cd trustmqtt
make test          # runs both suites
make test-plugin   # C unit tests only
make test-worker   # Python suite only
```

Continuous integration runs the same suites on every push and pull request via [`.github/workflows/ci.yml`](./.github/workflows/ci.yml) (C plugin tests, Python worker tests, and a `docker compose config` lint) — that's the badge at the top.

**Current status (this pass):**

- **C plugin:** `test_ring`, `test_verdict_cache`, `test_token_bucket`, `test_enforce` — **all pass** (build fix #1 was required first).
- **Python worker:** **79 passed** (77 pre-existing + 2 new for adaptive fusion), covering ingest/validation, feature windowing, FSM contracts & FSM-diff, drift scoring, fleet coordination, policy fusion & hysteresis, verdict writing, redaction, incident lifecycle, LLM fallback, storage, replay, and the end-to-end scoring context.

> Running the Python suite outside Docker needs the worker deps: `pip install -r tmq_worker/requirements.txt -r tmq_worker/requirements-dev.txt`, then `PYTHONPATH=. pytest tmq_worker/tests -q`.

---

## 14. Operational modes

| Mode | Plugin behavior | Worker behavior |
|---|---|---|
| **enforce** (default) | Full: emit events, fetch verdicts, enforce on `ACL_CHECK`. | Full scoring + verdict writes + incidents. |
| **monitor** | Emit events, fetch verdicts, **log** would-be denials (`TMQ-WOULD: …`), never deny. | Full scoring; verdicts written for observation. |
| **fingerprint** | Emit events only — no verdict fetch, no enforcement. | `--fingerprint-only`: ingest + features + FSM; exports fingerprint docs; **skips** drift/policy/verdicts. |

---

## 15. Limitations & roadmap

**Honest limitations (by design):**
- No payload inspection and no wire-level frame analysis — the Mosquitto plugin API does not expose DUP, packet IDs, ack reason codes, PINGREQ timing, or topic-alias usage. This is stated verbatim in `plugin.c` and `features.py`.
- Detection quality depends on a clean learning period; an attacker present *during* learning can poison a contract (mitigated by cohort drift models and fleet detection, not eliminated).

**Roadmap:** the [§12.2](#122-findings-recommended-for-a-follow-up-pass-not-yet-changed) hardening items (event-loop offloading, PEL recovery, memory eviction, incident rehydration), plus richer benchmark coverage against MQTTset and MQTT-IoT-IDS2020.

---

*Architecture, data contracts, and build phases in full: [`trustmqtt/docs/SPEC.md`](./trustmqtt/docs/SPEC.md). Project status — what's done and what remains: [`WORK_PROGRESS.md`](./WORK_PROGRESS.md).*
