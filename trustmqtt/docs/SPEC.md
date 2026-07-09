# TrustMQTT — Full Technical Specification (v2, Mosquitto 2.1.x)

**Purpose of this document:** Complete, coder-ready specification for the TrustMQTT zero-trust MQTT broker security system. Hand this to the implementer(s) as the single source of truth. All schemas, key layouts, interfaces, thresholds, and build phases are defined here.

**Baseline decisions (locked):**

- Broker: **Eclipse Mosquitto 2.1.x** (required — the spec depends on `MOSQ_EVT_CONNECT` and `MOSQ_EVT_CLIENT_OFFLINE`, added in 2.1.0)
- Pipeline: **C plugin → Redis (Streams) → Python scoring worker → policy engine → enforcement back in plugin**
- Scoring is **asynchronous** (never blocks the broker's event loop); enforcement is **synchronous** against a locally cached verdict
- No raw payload capture by default — **resolved-semantic-event analysis** (topic, QoS, retain, session state, identity, MQTT 5 properties). Do NOT describe this as "wire-level" or "protocol-header-level" analysis anywhere in code comments or docs.
- Team split: **Member A** owns everything in C (plugin, enforcement path). **Member B** owns everything in Python (worker, ML, policy, reporting, dashboard).

**Implementation note (2026-07-07):** every "Gemini" reference below has been superseded — the incident-report LLM is **NVIDIA NIM** (OpenAI-compatible chat-completions API) instead, in `tmq_worker/llm.py` (not `gemini.py`), configured via `config/tmq.yaml`'s `llm:` block (not `gemini:`) and the `NVIDIA_API_KEY`/`NVIDIA_BASE_URL` env vars (not `GEMINI_API_KEY`). Everything else in this spec (redaction being mandatory and provider-agnostic, fallback-on-failure, never blocking incident creation) is unchanged — only the specific provider differs from what's written below.

---

## 0. Adopted improvements (delta from v1 design)

These are incorporated throughout the spec; listed here so the coder knows what is NEW vs. carried over.

| # | Improvement | Why | Where in spec |
|---|-------------|-----|---------------|
| I1 | Upgrade to Mosquitto 2.1.x; anchor FSM on `MOSQ_EVT_CONNECT` / `MOSQ_EVT_CLIENT_OFFLINE` | Replaces inferred connect-state with observed connect/offline events | §3.2 |
| I2 | Keep-alive conformance scoring (negotiated keepalive vs. observed activity gaps via `MOSQ_EVT_TICK`) | Recovers most of the signal lost by the missing PINGREQ hook | §3.4, §5.3 |
| I3 | Subscription-churn / topic-scope features (`MOSQ_EVT_SUBSCRIBE`/`UNSUBSCRIBE`, `mosquitto_client_sub_count()`) | Topic-scope expansion is a strong zero-trust signal, broker-native | §3.2, §5.3 |
| I4 | Standalone **fingerprint-only mode** (Behavioral Contract Engine runs with enforcement disabled, exports per-client behavioral fingerprints) | Separable novel contribution; independently benchmarkable | §5.6 |
| I5 | **Fleet-level drift** baseline (detects synchronized behavior shifts across many clients) | Catches botnet-style coordinated compromise that per-client scoring misses | §5.5 |
| I6 | Benchmark harness for **MQTTset** and **MQTT-IoT-IDS2020**, reporting detection metrics (F1/AUC) AND enforcement metrics (time-to-mitigation, false-quarantine rate) | Comparability with published work + our unique enforcement story | §9 |
| I7 | FSM-diff data exported for Grafana visualization | Matches interpretability edge of process-mining competitors | §7.2 |
| I8 | **Redaction layer** before any data leaves for the LLM API | Closes the external-data-exposure hole in the threat model | §7.1 |
| I9 | Graduated enforcement gains a THROTTLE level implemented as a token bucket inside the plugin | Smoother response curve between "watch" and "quarantine" | §6.2 |
| I10 | Terminology fix throughout: "resolved-semantic-event analysis," never "header-level" | Prevents overclaiming that a reviewer will catch | everywhere |

---

## 1. System overview

### 1.1 Goal

Continuously verify that every authenticated MQTT client *behaves like itself*, and apply graduated, automatic countermeasures when it does not. MQTT 5.0 answers "is this the right device?"; TrustMQTT answers "is this device behaving like itself?".

### 1.2 Threat model (what we detect)

- Compromised-but-authenticated device: valid credentials, changed behavior (new topics, new rates, new sequences)
- Credential theft / device impersonation: right credentials, wrong behavioral fingerprint
- Slow-drift abuse: gradual escalation designed to stay under static ACLs
- Coordinated fleet compromise: many devices shifting behavior simultaneously (fleet-level drift)

Out of scope (v2): payload-content inspection, wire-level frame analysis (DUP, packet ID, ack reason codes, PING timing — not exposed by the plugin API; documented limitation), TLS termination attacks, broker-host compromise.

### 1.3 Non-functional requirements

| Requirement | Target |
|---|---|
| Broker overhead per event (plugin hot path) | < 50 µs added latency; zero blocking I/O in callbacks |
| Event delivery to worker | ≤ 1 s end-to-end (Redis Stream) |
| Verdict propagation (worker → plugin cache) | ≤ 1 s (TICK-driven refresh) |
| Scale target (capstone) | 500 concurrent clients, 2,000 msg/s sustained |
| Broker availability | Plugin failure must never crash broker: all callbacks wrap errors, degrade to ALLOW + log |
| Fail posture | **Fail-open** (if Redis is down, broker keeps serving; events buffered/dropped with counter; verdicts frozen at last known value with TTL decay to ALLOW) |

---

## 2. Architecture

```
                        ┌──────────────────────────────────────────────┐
                        │              Mosquitto 2.1.x                 │
                        │                                              │
  MQTT clients ───────► │  ┌──────────────── trustmqtt_plugin.so ────┐ │
                        │  │  Event taps:                             │ │
                        │  │   CONNECT / CLIENT_OFFLINE / DISCONNECT  │ │
                        │  │   MESSAGE_IN / SUBSCRIBE / UNSUBSCRIBE   │ │
                        │  │   BASIC_AUTH / ACL_CHECK / TICK          │ │
                        │  │                                          │ │
                        │  │  [emitter] ──JSON──► Redis Stream        │ │
                        │  │  [verdict cache] ◄──── Redis hashes      │ │
                        │  │  [enforcer] ACL_CHECK consults cache:    │ │
                        │  │   ALLOW/WATCH/THROTTLE/QUARANTINE/KICK   │ │
                        │  └──────────────────────────────────────────┘ │
                        └──────────────────────┬───────────────────────┘
                                               │
                                        ┌──────▼──────┐
                                        │    Redis    │
                                        │  Streams +  │
                                        │  KV/hashes  │
                                        └──────┬──────┘
                                               │ consumer group
                    ┌──────────────────────────▼──────────────────────────┐
                    │              Python scoring worker (tmq-worker)     │
                    │  ┌────────────┐ ┌───────────────┐ ┌──────────────┐  │
                    │  │ Feature    │ │ Behavioral    │ │ Statistical  │  │
                    │  │ extractor  │►│ Contract      │ │ Drift Scorer │  │
                    │  │ (windows)  │ │ Engine (FSM)  │ │ IF + OCSVM   │  │
                    │  └────────────┘ └───────┬───────┘ └──────┬───────┘  │
                    │                         └───────┬────────┘          │
                    │                        ┌────────▼────────┐          │
                    │                        │  Fleet baseline │          │
                    │                        └────────┬────────┘          │
                    │                        ┌────────▼────────┐          │
                    │                        │  Policy engine  │──verdicts──► Redis
                    │                        └────────┬────────┘          │
                    │                                 │ incidents         │
                    │                        ┌────────▼────────┐          │
                    │                        │ Incident svc    │──► Postgres
                    │                        │ (+ redactor ──► NVIDIA NIM)│
                    │                        └─────────────────┘          │
                    └─────────────────────────────────────────────────────┘
                                               │
                                        ┌──────▼──────┐
                                        │   Grafana   │ (Postgres + Redis datasources)
                                        └─────────────┘
```

**Deployment:** single `docker-compose.yml` with services: `mosquitto` (custom image, 2.1.x + plugin), `redis:7`, `postgres:16`, `tmq-worker`, `grafana`. All on one bridge network `tmqnet`.

---

## 3. Component spec — Mosquitto C plugin (`trustmqtt_plugin`) — **Member A**

### 3.1 Build & load

- Language: C11. Deps: `hiredis` (with async optional; sync-with-timeout acceptable given batching design below), `cJSON` (vendored), pthreads.
- Build: `gcc -I<mosquitto/include> -fPIC -shared src/*.c -o trustmqtt_plugin.so -lhiredis -lpthread`
- `mosquitto.conf`:
  ```
  plugin /usr/lib/trustmqtt_plugin.so
  plugin_opt_redis_host redis
  plugin_opt_redis_port 6379
  plugin_opt_emit_batch_ms 100
  plugin_opt_verdict_refresh_ms 500
  plugin_opt_mode enforce        # enforce | monitor | fingerprint
  plugin_opt_payload_hash off    # off | sha256
  ```
- Implement `mosquitto_plugin_version` (return 5), `mosquitto_plugin_init` (register callbacks, spawn emitter thread), `mosquitto_plugin_cleanup`.

### 3.2 Registered events and extracted fields

Register via `mosquitto_callback_register` for:

| Event | Fired when | Fields captured into event JSON |
|---|---|---|
| `MOSQ_EVT_CONNECT` (2.1.x) | client successfully authenticated | client_id, username, ip (`mosquitto_client_address`), protocol (`mosquitto_client_protocol`), clean_session, keepalive (`mosquitto_client_keepalive`) |
| `MOSQ_EVT_DISCONNECT` | client disconnects | client_id, reason code (from event struct) |
| `MOSQ_EVT_CLIENT_OFFLINE` (2.1.x) | persistent-session client goes offline | client_id |
| `MOSQ_EVT_MESSAGE_IN` | inbound PUBLISH accepted for processing | client_id, topic, qos, retain, payload_len, selected MQTT5 properties (content-type, message-expiry, user-property COUNT only), optional payload sha256 |
| `MOSQ_EVT_SUBSCRIBE` | client subscribes | client_id, topic filter, requested qos, running `mosquitto_client_sub_count()` |
| `MOSQ_EVT_UNSUBSCRIBE` | client unsubscribes | client_id, topic filter, sub_count |
| `MOSQ_EVT_BASIC_AUTH` | auth attempt | client_id, username, ip, result passthrough (plugin defers auth: return `MOSQ_ERR_PLUGIN_DEFER`) — we only OBSERVE auth here, never decide it |
| `MOSQ_EVT_ACL_CHECK` | every pub/sub authorization | **enforcement hot path** — see §6 |
| `MOSQ_EVT_TICK` | periodic | drives emitter flush, verdict-cache refresh, keepalive-conformance bookkeeping |

**Hard rule for the coder:** no callback may perform blocking network I/O. All Redis writes go through the emitter ring buffer (§3.3); all Redis reads happen on the TICK-driven refresher.

### 3.3 Emitter (event out-path)

- Lock-protected ring buffer (fixed 8,192 slots) of serialized JSON strings.
- Callbacks: serialize with cJSON → `ring_push()` → return. On full ring: drop event, increment `tmq_dropped_events` counter (exported in a periodic `plugin_stats` event).
- Dedicated emitter pthread: every `emit_batch_ms` (default 100 ms) drains the ring and pipelines `XADD tmq:events MAXLEN ~ 1000000 * v <json>` commands over one hiredis connection. On Redis failure: exponential backoff reconnect (250 ms → 5 s cap), events continue to drop with counter (fail-open).

### 3.4 Keepalive conformance bookkeeping (I2)

- In-plugin per-client struct tracks `last_activity_ts` (updated on every MESSAGE_IN/SUBSCRIBE/UNSUBSCRIBE for that client) and negotiated `keepalive`.
- On each TICK (1 s): for every tracked connected client, if `now - last_activity_ts > 1.5 × keepalive` AND client is still connected per broker, emit a synthetic event `{"event":"ka_gap", "client_id":..., "gap_s":..., "keepalive":...}`. (We cannot see PINGREQ; a still-connected client with a large activity gap is being kept alive by pings we cannot observe — that is itself the feature: "silent-but-alive ratio".)

### 3.5 Verdict cache & refresher

- In-memory hash map `client_id → {level:u8, score:f32, expires_at:ts, tokens:f32, tokens_ts:ts}` guarded by rwlock.
- TICK refresher (every `verdict_refresh_ms`): pipeline `HGETALL tmq:verdict:<client_id>` for all connected clients (or `MGET` on packed strings — see §4.2, use the packed-string variant for cheapness). Update map.
- TTL decay: if a verdict's `expires_at` has passed and Redis is unreachable, decay level one step per 60 s until ALLOW (fail-open, but not instantly).

### 3.6 Plugin modes

- `enforce` — full behavior (default)
- `monitor` — emit events, fetch verdicts, LOG would-be enforcement, never deny
- `fingerprint` — emit events only; no verdict fetch, no enforcement (supports I4 standalone mode)

---

## 4. Data contracts

### 4.1 Event JSON schema (plugin → Redis Stream `tmq:events`)

One JSON object per stream entry, field `v` = schema version. All timestamps are UNIX float seconds (broker clock).

```json
{
  "v": 1,
  "ts": 1720000000.123,
  "event": "connect | disconnect | client_offline | publish | subscribe | unsubscribe | auth_observe | ka_gap | plugin_stats",
  "client_id": "sensor-042",
  "username": "plant-a",                 // present where known
  "ip": "10.0.3.17",                     // connect/auth events only
  "protocol": "mqtt | websockets | mqttsn",
  "clean_session": true,                  // connect only
  "keepalive": 60,                        // connect only
  "reason": 0,                            // disconnect only (broker-provided reason)
  "topic": "plant-a/line2/temp",          // publish/subscribe/unsubscribe
  "qos": 1,
  "retain": false,
  "payload_len": 118,
  "payload_sha256": null,                 // only if plugin_opt_payload_hash=sha256
  "props": {"content_type": "application/json", "message_expiry": 30, "user_prop_count": 1},
  "sub_count": 4,                         // subscribe/unsubscribe events
  "gap_s": 95.2                           // ka_gap events
}
```

Rules: absent = omit the key (do not send nulls except payload_sha256); topic strings are the broker-**resolved** topics (aliases already resolved — this is by design and must be documented as resolved-semantic-event analysis).

### 4.2 Verdict contract (worker → Redis → plugin)

Two representations, both written by the worker in the same transaction (MULTI):

1. **Packed string** (read by plugin, cheap): `SET tmq:verdictp:<client_id> "L|S|E|R"` where `L`=level int, `S`=score float 0–1, `E`=expires_at unix, `R`=rate tokens/sec for THROTTLE. Example: `2|0.71|1720000123|3.0`. TTL 120 s.
2. **Hash** (read by humans/Grafana): `HSET tmq:verdict:<client_id> level 2 score 0.71 expires_at ... rate 3.0 reason "fsm_violation:pub_seq" updated_at ...` TTL 120 s.

Verdict levels:

| Level | Name | Enforcement |
|---|---|---|
| 0 | ALLOW | none |
| 1 | WATCH | none; flagged in dashboard, sampling of events raised |
| 2 | THROTTLE | token bucket on PUBLISH ACL checks at `R` msg/s (burst 2×R); excess → deny (client sees NOT_AUTHORIZED on QoS≥1) |
| 3 | QUARANTINE | deny all PUBLISH except topics matching `tmq/quarantine/#`; deny all new SUBSCRIBE |
| 4 | KICK | plugin calls `mosquitto_kick_client_by_clientid(client_id, false)` on next TICK; verdict then drops to 3 to contain reconnects |

### 4.3 Feature vector schema (worker-internal, persisted for training)

Per client, per tumbling window `W = 60 s` (config):

```
msg_rate, byte_rate, mean_iat, std_iat, unique_topics, new_topic_ratio,
qos0_ratio, qos1_ratio, qos2_ratio, retain_ratio, sub_events, unsub_events,
sub_count_delta, topic_entropy, mean_payload_len, std_payload_len,
ka_conformance (= observed_activity_gap_p95 / keepalive), silent_alive_ratio,
connect_events, disconnect_events, fsm_violation_score (from BCE, joined in)
```

### 4.4 Redis key design (complete)

| Key | Type | Writer | Reader | TTL | Purpose |
|---|---|---|---|---|---|
| `tmq:events` | Stream (MAXLEN ~1M) | plugin | worker (group `tmqw`) | trim | protocol events |
| `tmq:verdictp:<cid>` | String | worker | plugin | 120 s | packed verdict (hot path) |
| `tmq:verdict:<cid>` | Hash | worker | Grafana/ops | 120 s | readable verdict |
| `tmq:fsm:<cid>` | String (JSON) | worker | worker | none | serialized learned FSM |
| `tmq:feat:<cid>` | Stream (MAXLEN 1440) | worker | worker/trainer | trim | recent feature windows |
| `tmq:fleet:baseline` | Hash | worker | worker | none | fleet feature means/stds (per-feature) |
| `tmq:fleet:zbuf` | Stream | worker | worker | trim | per-window fleet aggregate for coordinated-drift check |
| `tmq:models:meta` | Hash | trainer | worker | none | model version, trained_at, n_samples, path |
| `tmq:incidents` | Stream | worker | incident svc | trim | incident records pre-DB |
| `tmq:stats:plugin` | Hash | plugin | Grafana | none | dropped events, ring high-water, reconnects |

---

## 5. Component spec — Python scoring worker (`tmq-worker`) — **Member B**

Python 3.11+. Deps: `redis` (redis-py), `scikit-learn`, `numpy`, `pydantic` (schema validation of §4.1), `SQLAlchemy` + `psycopg`, `pyyaml`. Package layout:

```
tmq_worker/
  __main__.py          # entrypoint: asyncio supervisor
  config.py            # YAML config loader (thresholds, windows, weights)
  ingest.py            # Redis Stream consumer group reader
  features.py          # windowing + feature extraction (§4.3)
  fsm.py               # Behavioral Contract Engine
  drift.py             # IsolationForest + OneClassSVM ensemble
  fleet.py             # fleet baseline + coordinated drift
  policy.py            # trust score fusion + graduated verdicts + hysteresis
  verdicts.py          # Redis verdict writer (packed + hash, MULTI)
  incidents.py         # incident creation, DB persistence
  redact.py            # redaction layer before any LLM call (I8)
  llm.py               # report generation client (NVIDIA NIM — see implementation note above)
  storage.py           # SQLAlchemy models + session
  train.py             # offline/periodic trainer CLI
  replay.py            # benchmark harness driver (§9)
```

### 5.1 Ingest

- `XREADGROUP GROUP tmqw <consumer> BLOCK 1000 COUNT 512 STREAMS tmq:events >`
- Validate against pydantic model; malformed → `tmq:events:dead` stream + counter. ACK after routing. Idempotent by stream ID.

### 5.2 Behavioral Contract Engine (FSM) — `fsm.py`

- **Alphabet:** symbols = `CONNECT`, `DISCONNECT`, `OFFLINE`, `SUB(tc)`, `UNSUB(tc)`, `PUB(tc,q)`, `KA_GAP`, `IDLE` (emitted when no event for > idle_s). `tc` = topic class: topic normalized by replacing purely-numeric / UUID-shaped / >12-char-hex segments with `+` (e.g., `plant-a/line2/temp` → `plant-a/+/temp` only if `line2` matches the numeric-suffix rule `^[a-z]*\d+$`; otherwise literal). `q` = qos.
- **Model:** first-order transition matrix with Laplace smoothing per client: `P(sym_t | sym_{t-1})`, learned during a per-client **learning period** (default: first 24 h or first 2,000 events, whichever first; configurable). After learning, matrix updates continue with exponential decay (α = 0.01) so contracts adapt slowly.
- **Violation score per event:** `viol = min(1, -log(P(sym_t|sym_{t-1})) / -log(p_floor))` with `p_floor = 1e-4`. Aggregated per window: `fsm_violation_score = p95(viol in window)`.
- **Contract states for reporting:** the FSM is serialized (JSON: states, transition counts, top-k transitions) to `tmq:fsm:<cid>` after each window; the **FSM-diff** (I7) = set of transitions observed this window with learned P < 0.01, exported to Postgres for Grafana.
- **New-client policy:** unknown client_id ⇒ starts in learning mode at WATCH level (never enforced during learning unless drift score exceeds hard ceiling 0.95).

### 5.3 Statistical Drift Scorer — `drift.py`

- Features: §4.3 vector, standardized with per-feature robust scaler (median/IQR) fit at training time.
- Models: `IsolationForest(n_estimators=200, contamination=0.02)` and `OneClassSVM(kernel='rbf', nu=0.05, gamma='scale')`. Score fusion: min-max normalize each to [0,1] over training scores, then `drift = 0.5·if_score + 0.5·ocsvm_score`.
- Training: `train.py` CLI — pulls feature windows from Postgres (or `tmq:feat:*`), fits per-**cohort** models (cohort = username by default; per-client models optional flag) and writes to `models/` volume + `tmq:models:meta`. Retrain trigger: cron (daily) or manual; worker hot-reloads on meta-hash change.
- Cold start: until a cohort model exists, drift = 0 and only FSM + fleet signals act.

### 5.4 Trust score fusion — `policy.py`

```
T = w_fsm · fsm_violation_score + w_drift · drift + w_fleet · fleet_component
defaults: w_fsm = 0.45, w_drift = 0.35, w_fleet = 0.20      (YAML-configurable)
```

Thresholds (defaults): T < 0.30 → ALLOW; 0.30–0.50 → WATCH; 0.50–0.70 → THROTTLE (rate R = max(1, observed_baseline_rate × (1 − T))); 0.70–0.85 → QUARANTINE; ≥ 0.85 → KICK.

**Hysteresis:** escalation immediate; de-escalation only after `N = 3` consecutive windows below the lower threshold minus margin 0.05. KICK requires ≥ 0.85 on **2 consecutive windows** OR a single window ≥ 0.95 (protects against single-window false kicks).

### 5.5 Fleet-level drift (I5) — `fleet.py`

- Maintain fleet baseline: running mean/std of each feature across all active clients (`tmq:fleet:baseline`).
- Per window compute fleet aggregate `z̄` = mean absolute z-score across clients per feature; push to `tmq:fleet:zbuf`.
- **Coordinated-drift alarm:** if ≥ `k` (default 20 %) of active clients have same-signed z > 2 on the same feature in the same window → raise fleet incident, and add `fleet_component = min(1, fraction_affected × 2)` to every affected client's fusion. Otherwise `fleet_component = individual |z| capped: min(1, max_feature_|z|/6)`.

### 5.6 Standalone fingerprint mode (I4)

- Worker flag `--fingerprint-only`: runs ingest + features + FSM, **skips** drift/policy/verdicts.
- Exports per-client fingerprint document to Postgres + `fingerprints/` volume:

```json
{ "client_id": "...", "learned_over": {"events": 2000, "hours": 24},
  "fsm": {"states": [...], "top_transitions": [["CONNECT","SUB(plant-a/+/cmd)",0.42], ...]},
  "feature_baseline": {"msg_rate": {"med": 1.2, "iqr": 0.4}, ...},
  "stability": 0.93 }
```

- `stability` = 1 − mean JS-divergence between successive daily transition matrices. This artifact is the "broker-native per-client MQTT behavioral fingerprinting" contribution and must be generatable with the plugin in `fingerprint` mode too (no enforcement anywhere in the path).

---

## 6. Enforcement path (plugin side, hot) — **Member A**

### 6.1 ACL_CHECK handler

```
on MOSQ_EVT_ACL_CHECK(ed):
  v = verdict_cache.get(ed->client_id)            # rwlock read
  if v == NULL or v.level <= 1: return MOSQ_ERR_PLUGIN_DEFER   # normal ACLs decide
  if v.level == 2 (THROTTLE) and access is WRITE (publish):
      refill tokens: tokens += R × (now − tokens_ts); cap 2R; tokens_ts = now
      if tokens >= 1: tokens -= 1; return MOSQ_ERR_PLUGIN_DEFER
      else: return MOSQ_ERR_ACL_DENIED
  if v.level == 3 (QUARANTINE):
      if access is WRITE and topic matches "tmq/quarantine/#": return MOSQ_ERR_PLUGIN_DEFER
      if access is SUBSCRIBE: return MOSQ_ERR_ACL_DENIED
      return MOSQ_ERR_ACL_DENIED
  return MOSQ_ERR_PLUGIN_DEFER
```

### 6.2 KICK handling

- TICK loop scans cache for level 4 → `mosquitto_kick_client_by_clientid(cid, false)` (no Will sent), emit `enforcement` event, locally demote to 3.
- In `monitor` mode all denials/kicks are replaced by log lines prefixed `TMQ-WOULD:` (same code path, one branch).

---

## 7. Reporting & dashboard — **Member B**

### 7.1 Incident service + LLM redaction (I8)

- Incident created when: verdict level rises to ≥ 2, fleet alarm fires, or KICK executes. Persist to Postgres first (source of truth), then draft report.
- **Redaction (mandatory, in `redact.py`, unit-tested):** before any text goes to the LLM API: (1) drop payload hashes; (2) mask IPs to /24 (`10.0.3.x`); (3) pseudonymize client_id/username via HMAC-SHA256 with local secret → `client-7f3a`; (4) topic segments matching secret-pattern config list are replaced with `⟦redacted⟧`; (5) never send raw event stream — only the incident summary struct (scores, level history, FSM-diff transition names, window stats).
- LLM call (NVIDIA NIM, OpenAI-compatible chat-completions API): single prompt template ("Write a concise incident report for a security operator: …"), temperature 0.2, output stored as `incidents.report_md`. Failure ⇒ fall back to a deterministic template renderer (report must never block on the external API; 10 s timeout).

### 7.2 Postgres schema (SQLAlchemy models in `storage.py`)

```
clients(id PK, client_id UQ, username, first_seen, last_seen, cohort, learning_complete bool)
sessions(id PK, client_id FK, connect_ts, disconnect_ts, ip_masked, protocol, keepalive, clean_session)
feature_windows(id PK, client_id FK, window_start, window_len_s, features JSONB, fsm_violation float, drift float, fleet float, trust float)
verdict_history(id PK, client_id FK, ts, level int, score float, reason text)
incidents(id PK, client_id FK nullable, fleet bool, opened_ts, closed_ts, peak_level, peak_score, fsm_diff JSONB, summary JSONB, report_md text)
fingerprints(id PK, client_id FK, created_ts, doc JSONB, stability float)
model_versions(id PK, cohort, trained_at, n_samples, metrics JSONB, path)
```

### 7.3 Grafana

Datasources: Postgres + Redis. Dashboards (provisioned as JSON in repo `grafana/provisioning/`):

1. **Fleet overview** — active clients, verdict-level distribution (pie), trust-score heatmap (client × time), fleet z̄ timeline with alarm annotations.
2. **Client drill-down** (templated by client_id) — trust score + component breakdown stacked, feature sparklines, verdict history, **FSM-diff table** (novel transitions this window: from→to, learned P, count) (I7).
3. **System health** — `tmq:stats:plugin` (dropped events, ring high-water), stream lag (`XINFO GROUPS`), worker throughput, model versions.

---

## 8. Configuration (single `config/tmq.yaml`, mounted into worker)

```yaml
window_s: 60
learning: {max_events: 2000, max_hours: 24}
weights: {fsm: 0.45, drift: 0.35, fleet: 0.20}
thresholds: {watch: 0.30, throttle: 0.50, quarantine: 0.70, kick: 0.85, kick_single: 0.95}
hysteresis: {deescalate_windows: 3, margin: 0.05}
fleet: {coordinated_fraction: 0.20, z_trigger: 2.0}
topic_class: {numeric_suffix_regex: "^[a-z]*\\d+$"}
redaction: {secret_topic_patterns: ["+/credentials/#", "+/keys/#"]}
llm: {model: "meta/llama-3.1-8b-instruct", timeout_s: 10, enabled: true}  # NVIDIA NIM; see implementation note in §0
mode: enforce            # mirrors plugin_opt_mode; worker refuses verdict writes in fingerprint mode
```

---

## 9. Benchmark & evaluation harness (I6) — **Member B**, Member A assists

### 9.1 Replayer (`replay.py`)

- Input adapters: (a) **MQTTset** CSV/PCAP-derived logs, (b) **MQTT-IoT-IDS2020** captures, (c) our own synthetic scenario scripts (YAML: N clients, behavior phases, attack injections).
- Replays as real MQTT traffic via `paho-mqtt` against the dockerized broker, preserving inter-arrival times (scalable by factor `--speed`). Ground-truth labels carried in a sidecar file keyed by (client_id, time-range, attack_type).
- Attack scenarios to script for enforcement metrics: credential-reuse hijack (same creds, new behavior), topic-scope expansion, slow-rate escalation, coordinated 30-client drift.

### 9.2 Metrics (reported by `replay.py --report`)

- **Detection:** per-window precision/recall/F1, ROC-AUC of trust score vs. labels; comparable to published MQTTset / MQTT-IoT-IDS2020 results.
- **Enforcement (our differentiator):** time-to-mitigation (attack start → first level ≥ 2 verdict; → first denied action), false-quarantine rate (benign client-hours at level ≥ 3 / total benign client-hours), benign-throughput retention under attack.
- Output: JSON + markdown table into `eval/results/`, plus rows into Postgres for a Grafana eval dashboard.

---

## 10. Build phases & task order

**Phase 0 — Environment (both, day 1–3)**
Docker compose (mosquitto 2.1.x built from source in `docker/mosquitto/Dockerfile`, redis, postgres, grafana, worker skeleton); CI: build plugin, run pytest. Definition of done: broker up with empty plugin loaded, `XADD`/`XREAD` smoke test.

**Phase 1 — Event capture (A)**
Callbacks §3.2, ring buffer + emitter §3.3, schema §4.1, ka_gap logic §3.4, modes flag. DoD: 500-client synthetic load, zero broker latency regression > 50 µs, events visible in stream, drop counter = 0.

**Phase 2 — Ingest + features + storage (B)**
`ingest.py`, `features.py`, `storage.py`, Postgres migrations, config loader. DoD: feature_windows rows populated from live traffic; malformed events land in dead stream.

**Phase 3 — Behavioral Contract Engine (B)**
`fsm.py` per §5.2, serialization, FSM-diff export, learning-period lifecycle. DoD: unit tests incl. topic-class normalizer; violation scores sane on scripted deviation scenario.

**Phase 4 — Drift scorer + trainer (B)**
`drift.py`, `train.py`, model hot-reload, cohort logic. DoD: trained on 24 h benign replay; AUC > 0.9 on basic MQTTset attack replay.

**Phase 5 — Policy + verdicts + enforcement (B then A)**
`policy.py`, `verdicts.py` (packed+hash MULTI); plugin verdict cache §3.5 + ACL enforcement §6 + KICK. DoD: end-to-end demo — scripted hijack goes ALLOW→WATCH→THROTTLE→QUARANTINE with hysteresis correct; monitor mode logs only.

**Phase 6 — Fleet drift + fingerprint mode (B)**
`fleet.py` §5.5; `--fingerprint-only` + fingerprint docs §5.6; plugin `fingerprint` mode passthrough (A, small). DoD: coordinated 30-client scenario raises fleet incident; fingerprint stability computed over 3 synthetic days.

**Phase 7 — Incidents, redaction, LLM reporting, Grafana (B)**
`incidents.py`, `redact.py` (unit tests are DoD-blocking), `llm.py` with fallback template; three Grafana dashboards §7.3. DoD: induced incident produces redacted report; redaction tests prove no raw IP/client_id/topic-secret leaves.

**Phase 8 — Benchmark harness + evaluation (B, A assists)**
`replay.py` + adapters + scenarios §9; final metrics run; results tables for the paper. DoD: reproducible `make eval` producing detection + enforcement metrics on both public datasets.

---

## 11. Repository layout

```
trustmqtt/
  docker-compose.yml
  docker/mosquitto/Dockerfile          # builds 2.1.x + plugin
  plugin/                              # Member A (C)
    src/{plugin.c,emitter.c,verdict_cache.c,enforce.c,ring.c}  # links the broker's own cJSON, not vendored
    include/…   tests/                 # unit tests for ring, cache, token bucket, enforce
  tmq_worker/                          # Member B (§5 layout)
  config/tmq.yaml
  grafana/provisioning/
  eval/{scenarios/,results/}
  migrations/                          # alembic
  Makefile                             # build, up, test, eval targets
  docs/SPEC.md                         # this document
```

## 12. Cross-cutting rules for the coder

1. Fail-open everywhere in the broker path; fail-closed nowhere. A security capstone that crashes the broker fails its own availability story.
2. Never block a Mosquitto callback (no sync Redis in callbacks; emitter thread + TICK refresher only).
3. All thresholds/weights come from `tmq.yaml` — no magic numbers in code.
4. Terminology (I10): "resolved-semantic-event analysis." The docstring at the top of `plugin.c` and `features.py` must state the API limitation list (no DUP, packet ID, ack reason codes, PING, topic-alias usage) verbatim.
5. Every enforcement decision must be reconstructible: verdict_history + feature_windows + fsm_diff are the audit trail; nothing enforces without a row that explains it.
6. Redaction is not optional and not last: `redact.py` merges before `llm.py` in the call graph, and CI fails if `llm.py` imports anything from `ingest.py`/raw events.
