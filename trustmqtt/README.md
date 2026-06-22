# TrustMQTT — Project Build Plan

This document is the build specification for **TrustMQTT**, a continuous behavioral
identity verification system for MQTT brokers. It is written for a code assistant
agent (or a developer) to implement against directly. It defines the full repository
layout, the purpose and contents of every file, the data contracts between
components, and the build/run sequence.

**Scope for v1 (this plan): broker-side implementation only**, targeting Eclipse
Mosquitto. Gateway-side extensions (e.g. Zigbee2MQTT integration) are explicitly
out of scope for v1 and are listed at the end as future work — do not build them
unless asked.

---

## 1. High-level architecture

```
[MQTT Devices] ──MQTT/TCP──► [Mosquitto Broker + trustmqtt_plugin.so]
                                        │
                                        │ (async, non-blocking)
                                        ▼
                              [Redis: metadata queue]
                                        │
                                        ▼
                          [Python scoring worker service]
                            ├── feature_engineering
                            ├── ml_models (per-device baselines)
                            ├── drift_scoring
                            └── policy_engine (graduated response)
                                        │
                  ┌─────────────────────┼─────────────────────┐
                  ▼                     ▼                     ▼
         [Postgres/SQLite:      [Enforcement callback   [Dashboard:
          baselines, drift       to Mosquitto: ACL/      Grafana or
          history, action log]  re-auth/disconnect]      Flask UI]
```

Core principle: the broker plugin **never blocks** the publish path. It only reads
already-parsed packet fields and pushes them to Redis. All ML scoring, baseline
updates, and policy decisions happen asynchronously in a separate Python process.

---

## 2. Repository layout

```
trustmqtt/
├── README.md
├── PROJECT_PLAN.md                  # this file
├── docker-compose.yml
├── .env.example
│
├── broker-plugin/                   # C plugin loaded into Mosquitto
│   ├── CMakeLists.txt
│   ├── include/
│   │   └── trustmqtt_plugin.h
│   ├── src/
│   │   ├── plugin_main.c
│   │   ├── hooks_connect.c
│   │   ├── hooks_publish.c
│   │   ├── hooks_subscribe.c
│   │   ├── hooks_disconnect.c
│   │   ├── metadata_serialize.c
│   │   └── redis_publish.c
│   └── tests/
│       └── test_metadata_serialize.c
│
├── mosquitto-config/
│   ├── mosquitto.conf
│   ├── acl.conf
│   └── dynamic_security.json
│
├── scoring-worker/                  # Python async service
│   ├── pyproject.toml
│   ├── requirements.txt
│   ├── trustmqtt/
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── queue_consumer.py
│   │   ├── feature_engineering/
│   │   │   ├── __init__.py
│   │   │   ├── session_features.py
│   │   │   ├── message_features.py
│   │   │   └── feature_schema.py
│   │   ├── models/
│   │   │   ├── __init__.py
│   │   │   ├── baseline_store.py
│   │   │   ├── isolation_forest_model.py
│   │   │   ├── one_class_svm_model.py
│   │   │   └── adaptive_update.py
│   │   ├── scoring/
│   │   │   ├── __init__.py
│   │   │   ├── drift_scorer.py
│   │   │   └── explainability.py
│   │   ├── policy/
│   │   │   ├── __init__.py
│   │   │   ├── policy_engine.py
│   │   │   ├── thresholds.py
│   │   │   └── enforcement_client.py
│   │   ├── storage/
│   │   │   ├── __init__.py
│   │   │   ├── models_orm.py
│   │   │   └── db.py
│   │   └── main.py
│   └── tests/
│       ├── test_feature_engineering.py
│       ├── test_drift_scorer.py
│       └── test_policy_engine.py
│
├── traffic-simulator/                # Simulated devices + attack scripts
│   ├── requirements.txt
│   ├── devices/
│   │   ├── base_device.py
│   │   ├── temperature_sensor.py
│   │   ├── door_lock.py
│   │   └── motion_sensor.py
│   ├── attacks/
│   │   ├── credential_replay_attack.py
│   │   ├── different_client_lib_attack.py
│   │   ├── recon_wildcard_subscribe.py
│   │   └── gradual_drift_firmware_sim.py
│   └── run_simulation.py
│
├── evaluation/
│   ├── datasets/
│   │   └── README.md                # instructions to fetch MQTT-IoT-IDS2020
│   ├── benchmark_detection.py
│   ├── benchmark_latency.py
│   └── results/
│       └── .gitkeep
│
├── dashboard/
│   ├── grafana/
│   │   ├── provisioning/
│   │   │   ├── datasources/datasource.yml
│   │   │   └── dashboards/trustmqtt_dashboard.json
│   └── README.md
│
└── scripts/
    ├── setup_dev_env.sh
    ├── build_plugin.sh
    └── seed_test_data.py
```

---

## 3. File-by-file purpose and contents

### 3.1 `broker-plugin/` (C, Mosquitto plugin)

**`include/trustmqtt_plugin.h`**
Header declaring shared structs used across the plugin: `device_metadata_t`
(holds client_id, event_type, timestamp, protocol_version, clean_session_flag,
will_flag, keep_alive, qos, retain_flag, dup_flag, payload_size, topic,
packet_id) and function prototypes for each hook handler.

**`src/plugin_main.c`**
Entry point. Implements `mosquitto_plugin_init`, `mosquitto_plugin_cleanup`,
and `mosquitto_plugin_version` (required by the Mosquitto plugin v5 API).
Registers callbacks for `MOSQ_EVT_CONNECT`, `MOSQ_EVT_MESSAGE`,
`MOSQ_EVT_SUBSCRIBE`, `MOSQ_EVT_DISCONNECT` via `mosquitto_callback_register`.
Initializes the Redis connection (via hiredis) on load and tears it down on
cleanup.

**`src/hooks_connect.c`**
Callback for CONNECT events. Extracts: protocol version, Clean Session/Start
flag, Will flag + topic + QoS, Keep-Alive value, client_id, and (if MQTT5)
Session Expiry Interval / Receive Maximum properties. Calls
`metadata_serialize.c` then `redis_publish.c`. Must return immediately without
modifying or delaying the CONNACK.

**`src/hooks_publish.c`**
Callback for MESSAGE (PUBLISH) events. Extracts: client_id, topic string,
QoS, retain flag, dup flag, payload size (NOT payload content), packet
identifier, timestamp. Does not touch `msg->payload` contents at all.

**`src/hooks_subscribe.c`**
Callback for SUBSCRIBE events. Extracts: client_id, list of topic filters
requested, requested QoS per filter, whether any filter contains wildcards
(`+` or `#`) or targets `$SYS/`.

**`src/hooks_disconnect.c`**
Callback for DISCONNECT events. Extracts: client_id, disconnect reason code
(MQTT5) or whether it was a clean DISCONNECT vs. an abrupt socket close
detected by Mosquitto, timestamp.

**`src/metadata_serialize.c`**
Converts a populated `device_metadata_t` struct into a compact JSON string
(use a minimal C JSON lib like `cJSON` or hand-rolled snprintf — keep
dependencies light). This JSON shape is the **data contract** with the Python
worker — see Section 4.

**`src/redis_publish.c`**
Wraps hiredis. Pushes the JSON string to a Redis list (`LPUSH
trustmqtt:metadata_queue <json>`) or publishes to a Redis pub/sub channel —
pick LPUSH/list approach for v1 since it's simpler to consume reliably
(worker uses `BRPOP` / `RPOP`). Must use a non-blocking or fire-and-forget
call pattern so a slow/unavailable Redis never stalls the broker.

**`CMakeLists.txt`**
Builds `trustmqtt_plugin.so` against `mosquitto_plugin.h` and links
`hiredis`. Outputs the `.so` to a `build/` directory referenced by
`mosquitto.conf`.

**`tests/test_metadata_serialize.c`**
Unit tests asserting the JSON serializer produces well-formed JSON matching
the schema in Section 4 for sample `device_metadata_t` inputs.

---

### 3.2 `mosquitto-config/`

**`mosquitto.conf`**
Standard Mosquitto config. Must include:
`plugin /path/to/build/trustmqtt_plugin.so`
plus listener, persistence, and (for v1) `allow_anonymous false` with a
password file or the dynamic security plugin enabled for ACL-based
enforcement.

**`acl.conf`**
Static ACL file as a fallback enforcement mechanism (mainly for early dev
before dynamic security plugin integration is wired up).

**`dynamic_security.json`**
Config for Mosquitto's built-in dynamic-security plugin, which the policy
engine will programmatically update (via its control API) to revoke or
restrict a client's permissions when a "severe" drift action fires. This is
the actual enforcement mechanism for quarantine — research Mosquitto's
dynamic security plugin control topics (`$CONTROL/dynamic-security/v1`)
before implementing `enforcement_client.py`.

---

### 3.3 `scoring-worker/` (Python)

**`trustmqtt/config.py`**
Loads environment variables / `.env` (Redis host/port, DB connection string,
drift thresholds, adaptive update decay rate, Mosquitto control API
connection details).

**`trustmqtt/queue_consumer.py`**
Connects to Redis, runs a loop calling `BRPOP trustmqtt:metadata_queue` (or
equivalent), parses JSON, validates against the schema, and dispatches each
event to feature engineering. Must handle malformed JSON gracefully (log
and skip, never crash the loop).

**`trustmqtt/feature_engineering/session_features.py`**
Computes session-level features from CONNECT/SUBSCRIBE/DISCONNECT events:
CONNECT fingerprint vector (protocol version, flags, Keep-Alive), Will
behavior, subscription topology (topic count, wildcard usage, `$SYS` usage),
session resumption pattern. Maintains rolling per-device history needed to
compute "vs. baseline" deltas (e.g., Keep-Alive negotiated vs. actual
PINGREQ cadence requires tracking PINGREQ timestamps between CONNECTs).

**`trustmqtt/feature_engineering/message_features.py`**
Computes message-level features from PUBLISH events: inter-arrival timing
and jitter, payload size distribution stats, QoS/retain/dup consistency,
topic structural fingerprint (level count, matches per-device learned
topic pattern), packet ID sequencing pattern.

**`trustmqtt/feature_engineering/feature_schema.py`**
Defines the exact ordered feature vector (as a typed dataclass or named
tuple) fed into the ML models, separately for session-level and
message-level models. This is the single source of truth other modules
import from — do not let feature order drift between training and scoring
code paths.

**`trustmqtt/models/baseline_store.py`**
Persistence layer for trained per-device models: serialize/deserialize
(e.g., via `joblib`) a fitted Isolation Forest or One-Class SVM per
device_id, per model tier (session vs. message). Tracks "is this device
still in baseline-learning phase" (e.g., first N events) vs. "baseline
established, now scoring."

**`trustmqtt/models/isolation_forest_model.py`** /
**`trustmqtt/models/one_class_svm_model.py`**
Thin wrappers around scikit-learn's `IsolationForest` / `OneClassSVM`:
`fit(feature_matrix)`, `score(feature_vector) -> anomaly_score`. Keep both
implemented behind a common interface so the policy/scoring code is
model-agnostic and you can A/B them during evaluation.

**`trustmqtt/models/adaptive_update.py`**
Implements the slow baseline-update mechanism: a decaying weighted retrain
(e.g., periodically refit on a sliding window of the last K accepted
"normal" events, or an incremental/online model from `river` if going the
online-learning route). Must only incorporate events that were NOT flagged
as anomalous, to avoid baseline poisoning by an actual attacker.

**`trustmqtt/scoring/drift_scorer.py`**
Combines session-level and message-level model outputs into a normalized
per-device drift score (normalize by that device's own historical score
variance, not a global scale — see proposal Section 5). Maintains a short
rolling history per device to detect "sustained" vs. "single-spike" drift
for the moderate/severe distinction.

**`trustmqtt/scoring/explainability.py`**
For any event whose drift score crosses the "mild" threshold or higher,
computes which individual features contributed most to the anomaly score
(simple per-feature z-score deviation from baseline mean/variance is
sufficient for v1; SHAP is a stretch goal). Attaches this to the log/alert
record.

**`trustmqtt/policy/thresholds.py`**
Defines the mild/moderate/severe cutoffs as configurable values (not
hardcoded), plus the logic for what counts as "sustained" drift (e.g., N
consecutive scores above threshold) vs. a single noisy spike.

**`trustmqtt/policy/policy_engine.py`**
Given a drift score + severity classification for a device, decides the
action (log / force re-auth / quarantine) and records it. Must support a
**shadow mode** flag (config-driven) where it logs the would-be action
without calling `enforcement_client.py`.

**`trustmqtt/policy/enforcement_client.py`**
Talks to Mosquitto to actually enforce moderate/severe actions: for
re-auth, this likely means publishing a control message or using the
dynamic-security plugin API to force-disconnect the client (which makes it
reconnect and re-authenticate); for quarantine, update the dynamic security
ACL to deny that client_id, then disconnect it. Document exactly which
Mosquitto control topics/APIs are used once implemented, since this is the
trickiest integration point.

**`trustmqtt/storage/models_orm.py`** + **`db.py`**
SQLAlchemy models (works for both SQLite and Postgres) for: `Device`,
`BaselineMetadata`, `DriftScoreHistory`, `PolicyActionLog`. `db.py` handles
session/connection setup from `config.py`.

**`trustmqtt/main.py`**
Wires everything: starts `queue_consumer`, on each event runs feature
engineering → scoring → policy engine → storage write. Entry point run as
`python -m trustmqtt.main`.

**`tests/`**
Unit tests per module listed above using `pytest`; use fixture metadata
events matching the schema in Section 4, not live Redis/broker
dependencies, for fast CI runs.

---

### 3.4 `traffic-simulator/`

**`devices/base_device.py`**
Base class wrapping `paho-mqtt`: connects with configurable CONNECT
parameters (protocol version, Keep-Alive, Clean Session), and runs a
publish loop with configurable interval + jitter distribution and payload
size. Subclasses override realistic per-device-type parameters.

**`devices/temperature_sensor.py`, `door_lock.py`, `motion_sensor.py`**
Concrete simulated devices with distinct, realistic baselines (e.g.,
temperature sensor: low frequency, tiny consistent payload, QoS 0; door
lock: event-driven irregular publishing, QoS 1, occasional subscribe to a
command topic).

**`attacks/credential_replay_attack.py`**
Reuses a captured device's credentials/client_id but connects with a
different timing/CONNECT signature — validates session-level fingerprint
detection.

**`attacks/different_client_lib_attack.py`**
Deliberately uses different CONNECT flags/Keep-Alive defaults than the
target device's normal baseline (simulating an attacker using a generic
MQTT client instead of replicating embedded firmware behavior).

**`attacks/recon_wildcard_subscribe.py`**
Connects as a normally publish-only device's client_id, then issues a
wildcard or `$SYS/#` subscription — validates subscription-topology
detection.

**`attacks/gradual_drift_firmware_sim.py`**
Slowly shifts payload size/timing over many messages to simulate a
legitimate firmware update — used to validate the adaptive baseline update
logic does NOT cause sustained false escalation.

**`run_simulation.py`**
CLI entry point: spins up N simulated devices + optionally one attack
scenario, all publishing against the broker for a configurable duration.

---

### 3.5 `evaluation/`

**`datasets/README.md`**
Instructions for obtaining and preprocessing the public MQTT-IoT-IDS2020
dataset for benchmark comparison.

**`benchmark_detection.py`**
Runs the scoring pipeline against both the simulated attack suite and the
MQTT-IoT-IDS2020 dataset; computes precision/recall/F1/ROC and writes
results to `evaluation/results/`.

**`benchmark_latency.py`**
Measures end-to-end publish-to-delivery latency with the plugin enabled vs.
a vanilla Mosquitto instance, across a range of message rates, to validate
the "negligible overhead" claim from the proposal.

---

### 3.6 `dashboard/`

**`grafana/provisioning/datasources/datasource.yml`**
Points Grafana at the Postgres/SQLite DB (or a time-series DB if swapped
in later).

**`grafana/provisioning/dashboards/trustmqtt_dashboard.json`**
Pre-built panels: per-device drift score over time, alert/re-auth/quarantine
event counts, fleet-wide health overview.

---

### 3.7 Root-level files

**`docker-compose.yml`**
Services: `mosquitto` (built from `broker-plugin` + `mosquitto-config`),
`redis`, `scoring-worker`, `postgres` (or mount SQLite volume), `grafana`.
All on one bridge network so service names resolve as hostnames.

**`.env.example`**
Template for Redis host/port, DB connection string, threshold values,
shadow-mode toggle.

**`scripts/setup_dev_env.sh`**
Installs system deps (Mosquitto dev headers, hiredis, Python deps),
intended as the first command run after cloning.

**`scripts/build_plugin.sh`**
Runs CMake build for the broker plugin and copies the resulting `.so` to
the path referenced in `mosquitto.conf`.

**`scripts/seed_test_data.py`**
Convenience script to pre-populate a few device baselines in storage for
dashboard demo purposes without running the full simulator.

---

## 4. Data contract: metadata JSON schema (plugin → Redis → worker)

This is the single most important interface in the system. Every event
pushed to Redis by the C plugin, and consumed by the Python worker, MUST
conform to this shape. Treat this as a versioned schema.

```json
{
  "schema_version": "1.0",
  "event_type": "CONNECT | PUBLISH | SUBSCRIBE | DISCONNECT | PINGREQ",
  "client_id": "string",
  "timestamp_ms": 1718700000000,

  "connect": {
    "protocol_version": 4,
    "clean_session": true,
    "keep_alive_sec": 60,
    "will_flag": false,
    "will_topic": null,
    "will_qos": null,
    "session_expiry_interval": null
  },

  "publish": {
    "topic": "string",
    "qos": 0,
    "retain": false,
    "dup": false,
    "payload_size_bytes": 24,
    "packet_id": 1024
  },

  "subscribe": {
    "topics": [{ "filter": "string", "requested_qos": 0 }]
  },

  "disconnect": {
    "reason_code": 0,
    "clean": true
  }
}
```

Only the section matching `event_type` will be populated; the others are
omitted or null. Keep this schema in `feature_schema.py` as the canonical
parser/validator on the Python side and in `trustmqtt_plugin.h` as the
canonical struct on the C side — if one side changes, update both plus this
document.

---

## 5. Build and run sequence (for the coding agent to follow in order)

1. `scripts/setup_dev_env.sh` — install system + language deps.
2. Build and sanity-test the plugin in isolation:
   `scripts/build_plugin.sh`, then load it into a bare Mosquitto instance
   and confirm hook callbacks fire (simple stdout logging before wiring
   Redis) using `mosquitto_pub`/`mosquitto_sub` manually.
3. Wire `redis_publish.c` and confirm JSON lands in Redis
   (`redis-cli LRANGE trustmqtt:metadata_queue 0 -1`) while publishing test
   messages.
4. Stand up `scoring-worker` with `queue_consumer.py` only, logging
   consumed events to console/SQLite — confirm the async pipe works
   end-to-end with NO ML yet.
5. Implement `feature_engineering/` and validate feature vectors against
   sample events from step 4.
6. Implement `models/` + `scoring/drift_scorer.py`; train baseline against
   `traffic-simulator` output for a single simulated device type first.
7. Implement `policy/` in shadow mode only; confirm correct
   mild/moderate/severe classification against injected attack scripts
   from `traffic-simulator/attacks/`.
8. Implement `enforcement_client.py` and disable shadow mode; confirm real
   re-auth/quarantine actions occur against Mosquitto's dynamic security
   plugin.
9. Wire `docker-compose.yml` to run all services together reproducibly.
10. Run `evaluation/benchmark_detection.py` and
    `evaluation/benchmark_latency.py`; populate `evaluation/results/`.
11. Stand up `dashboard/` against the same Postgres/SQLite instance.

---

## 6. Explicit non-goals for v1 (do not implement unless asked)

- Gateway-layer integration (e.g., Zigbee2MQTT hooks) — future work only.
- The topic correlation / causality graph feature (Section 5.1 of the
  proposal) — phase 2 stretch goal, not core v1.
- MQTT 5–specific reason code / properties fingerprinting — only implement
  if the target broker/client fleet actually uses MQTT 5; do not block v1
  on this.
- Multi-tenancy / per-fleet baseline isolation — note as a config seam in
  `baseline_store.py` (e.g., a `fleet_id` field) but full isolation logic
  is a later iteration.

---

## 7. Future work (not built now, for awareness only)

- Gateway-side extension: hook into a translation gateway (e.g.,
  Zigbee2MQTT) to recover per-sensor radio-level behavioral features for
  non-native-MQTT devices that are otherwise indistinguishable behind a
  single gateway MQTT session.
- Topic correlation / causality graph modeling across the full pub/sub
  topology.
- SHAP-based explainability in place of simple z-score deviation.
