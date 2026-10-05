# TrustMQTT broker-to-Redis implementation and HiveMQ integration note

Implementation update: the HiveMQ adapter and MQTTX runner have since been added.
See [current setup and behavior](HIVEMQ_MQTTX.md). The inspection and proposal below are retained as the pre-implementation design record.

Prepared: 17 September 2026. Scope: the current local working tree, including uncommitted changes. “HiveMQ” is the broker interpreted from the request. The proposed target is a self-hosted HiveMQ deployment with the Extension SDK; the edition, broker release and compatible SDK/JDK versions must be pinned before implementation.

This note separates code that exists from the proposed HiveMQ implementation. It is based on source inspection, not a fresh integration test. Docker's engine was unavailable during the status check. No HiveMQ adapter currently exists in this project.

## 1. What the broker part includes

The broker side is more than an MQTT server forwarding messages to Redis. It has two connected paths:

1. **Observation:** receive MQTT activity, extract metadata, create normalized events, queue them, and write them to Redis.
2. **Enforcement:** fetch worker verdicts from Redis in the background, cache them locally, and apply restrictions inside the broker.

Redis carries TrustMQTT telemetry and decisions. It is not the MQTT message-routing engine. MQTT delivery to subscribers remains the broker's job. Behavioral model training and scoring happen after the Redis boundary in the Python worker.

```text
MQTT devices
    |
    v
Broker: MQTT sessions, authentication, static permissions, message routing
    |
    +-- broker callbacks --> TrustMQTT normalized event builder
    |                              |
    |                       bounded memory queue
    |                              |
    |                       background emitter
    |                              |
    |                              v
    |                    Redis Stream: tmq:events --> Python worker
    |                                                    |
    |                                                    v
    |                  Redis keys: tmq:verdictp:<client_id>
    |                              |
    |                       background refresh
    |                              |
    +-- access checks <---- local verdict cache
    +-- disconnect API <--- pending KICK action
```

## 2. Existing components and their responsibilities

Paths in this note are relative to the project directory containing this note's parent `docs` folder.

| Component | Source | Responsibility |
|---|---|---|
| Mosquitto adapter | `plugin/src/plugin.c` | Translate Mosquitto callbacks into normalized core calls; map access denial and disconnect actions back to Mosquitto. |
| FlashMQ adapter | `plugin/src/flashmq_adapter.cpp` | Equivalent translation using FlashMQ's C++ plugin API. Exists in the local working tree. |
| Core interface | `plugin/include/trustmqtt_core.h` | Broker-independent event structs, lifecycle, access checks, maintenance and action polling. ABI version is 1. |
| Shared runtime | `plugin/src/core.c` | Event serialization, client registry, activity-gap events, verdict refresh and periodic statistics. |
| Configuration | `plugin/src/config.c`, `plugin/include/trustmqtt_plugin.h` | Defaults, validation, modes, identifiers and common constants. |
| Queue | `plugin/src/ring.c`, `plugin/include/ring.h` | Fixed-size, mutex-protected ring holding heap-allocated JSON strings. |
| Redis emitter | `plugin/src/emitter.c` | Background connection, batch draining and Redis Stream writes. |
| Verdict cache | `plugin/src/verdict_cache.c` | Cached levels, expiry, token buckets, decay and claiming KICK actions. |
| Enforcement rules | `plugin/src/enforce.c` | Map cached verdicts to continue/deny decisions for write, read and subscribe operations. |
| Build | `plugin/CMakeLists.txt` | Build separate Mosquitto and optional FlashMQ shared libraries using the same C sources. |
| Deployment | `docker/mosquitto/`, `docker/flashmq/`, `docker-compose.yml` | Broker images, listeners, baseline ACLs, plugin loading and Redis connectivity. |
| Receiver contract | `tmq_worker/ingest.py` | Parse and validate the events delivered through Redis. |
| Return contract | `tmq_worker/verdicts.py` | Write packed verdicts consumed by the broker and readable hashes for operations. |

The C runtime is shared at source/build level. The brokers load different `.so` files; a Mosquitto plugin binary cannot simply be installed into HiveMQ.

## 3. Current deployment and configuration

Mosquitto's Dockerfile defaults to source release **2.1.0** and builds the plugin against its headers. It uses C11, CMake, pthreads, hiredis for Redis, cJSON for JSON, and OpenSSL for optional hashing. A header-free degraded build exists for local checks but disables actual Mosquitto event capture; compiling that variant does not demonstrate a working broker integration.

Mosquitto exposes MQTT on host port **1883** and WebSockets on **9001**. The optional FlashMQ Compose profile exposes host **1884** to container **1883**. Redis is `redis:6379` on the Compose network and is also published on the host.

| Setting | Current default / checked-in value |
|---|---|
| `broker` | Adapter name: `mosquitto` or `flashmq` |
| `broker_id` | `default` |
| Redis address | `redis:6379` |
| Emit interval | 100 ms |
| Verdict refresh interval | 500 ms |
| Mode | `enforce` |
| Payload hash | `off`; optional `sha256` |
| Event queue capacity | 8,192 events |
| Emitter drain batch | Up to 512 events |
| Tracked client limit | 4,096 |
| Statistics event interval | 10 seconds |

The checked-in brokers allow anonymous connections for development/replay. Baseline ACL rules give named plant users their plant namespace and clients their own `fleet/<client_id>/#` and quarantine namespaces. TrustMQTT adds restrictions on top of these grants. Its “continue” result is not a grant to bypass the ACL.

The current C Redis connection configuration exposes host and port only; Redis authentication/TLS are not implemented in these connection paths. Compose does not attach a persistent data volume to Redis. These are development deployment characteristics to revisit when selecting HiveMQ's deployment requirements.

## 4. Mosquitto event capture

| Native callback | Normalized behavior |
|---|---|
| `MOSQ_EVT_CONNECT` | Emit `connect`; register client ID, connection metadata and activity tracking. |
| `MOSQ_EVT_DISCONNECT` | Emit `disconnect` with reason and remove active registry entry. |
| `MOSQ_EVT_CLIENT_OFFLINE` | Emit `client_offline` and remove active registry entry. |
| `MOSQ_EVT_MESSAGE_IN` | Emit `publish` with topic, QoS, retain, payload length and selected MQTT 5 properties. |
| `MOSQ_EVT_SUBSCRIBE` | Emit `subscribe` with filter, requested QoS and broker subscription count. |
| `MOSQ_EVT_UNSUBSCRIBE` | Emit `unsubscribe` with filter and available subscription metadata. |
| `MOSQ_EVT_BASIC_AUTH` | Emit `auth_observe`, then defer to normal authentication. Does not verify passwords or report an authentication result. |
| `MOSQ_EVT_ACL_CHECK` | Check local verdict for write/read/subscribe; return ACL denial or defer. Does not emit an event for every ACL check. |
| `MOSQ_EVT_TICK` | Claim pending KICK actions and call the broker disconnect API. Redis I/O is now on the core maintenance thread. |

The core also generates `ka_gap`, `plugin_stats`, and `enforcement` events. Current `enforcement` emission occurs after calling the injected kick callback; it is an action-attempt record, not independently confirmed proof of a successful disconnect. Ordinary throttle/quarantine denials do not each produce this event.

### Important signal limits

Raw payloads are never placed in the Redis event. Optional hashing reads payload bytes to compute SHA-256; with hashing off, the event carries only payload length. Topic, username and IP can still appear as plain metadata at this boundary.

The current common event contract does not carry packet identifiers, DUP flags, acknowledgement timing or PING packet timing. HiveMQ may expose additional data, but adding it needs an explicit schema and feature-design change.

`ka_gap` is an **observed application-activity gap**, calculated from connect/publish/subscribe/unsubscribe activity. It triggers when the gap exceeds 1.5 times keepalive, with repeated emissions spaced by at least keepalive. Because the current path does not touch activity on PINGREQ, it is not proof of a wire-level keepalive violation. A connected idle device can continue sending pings while appearing inactive to this signal.

## 5. Serialization, queueing and delivery to Redis

The callback passes normalized values to the core. The core builds compact JSON immediately and enqueues the resulting string. A separate emitter thread drains the queue and uses pipelined Redis commands.

```text
XADD tmq:events MAXLEN ~ 1000000 * v <complete-event-JSON>
```

The stream field named `v` contains the **entire JSON string**. Inside that JSON, another field `v` is the integer schema version. This distinction is essential when writing the Java producer. Flattening event attributes into separate Stream fields would break the current parser.

Illustrative publish event:

```json
{
  "v": 1,
  "ts": 1789600000.125,
  "event": "publish",
  "broker": "mosquitto",
  "broker_id": "default",
  "client_id": "sensor-01",
  "topic": "fleet/sensor-01/temperature",
  "qos": 1,
  "retain": false,
  "payload_len": 24,
  "props": {"content_type": "application/json", "user_prop_count": 0}
}
```

| Event | Event-specific fields in addition to version, timestamp and broker metadata |
|---|---|
| `connect` | `client_id`, optional `username`, `ip`, `protocol`, `clean_session`, `keepalive` |
| `disconnect` / `client_offline` | `client_id`, optional numeric `reason` |
| `publish` | `client_id`, `topic`, `qos`, `retain`, `payload_len`; optional `payload_sha256` and `props` |
| `subscribe` / `unsubscribe` | `client_id`, `topic`; optional `qos`, `sub_count` |
| `auth_observe` | `client_id`, optional `username`, `ip` |
| `ka_gap` | `client_id`, `gap_s`, `keepalive` |
| `plugin_stats` | `dropped_events`, `ring_size`; no client ID required |
| `enforcement` | `client_id` |

`props` supports content type, message expiry and user-property count. The C serializer currently omits zero-valued expiry and only creates the properties object when one of its trigger conditions is present. Preserve the meaning of missing data rather than filling unknown fields with zero. Mosquitto's `protocol` value currently describes transport/API type such as `mqtt` or `websockets`, not the MQTT version number.

The worker reads `tmq:events` through consumer group `tmqw`. Invalid entries go to `tmq:events:dead`. The dead-letter stream is written by the worker, not the broker. Broker metadata is already accepted by the current Python model, so `broker: "hivemq"` fits the existing parser.

### Actual reliability properties

- Queue insertion uses a mutex. There is no Redis call on the callback path, but this is not a lock-free or strictly wait-free implementation. Serialization, allocation and optional payload hashing also occur there.
- A full ring drops the incoming event and increments its drop counter.
- Redis connection failures cause already-drained batches to be freed, with retry backoff from 250 ms to 5 seconds. Those connection-loss drops are not included in the ring-full counter.
- Failed delivery is not durably retried. Shutdown does not guarantee queue flushing. Approximate stream trimming also eventually removes old entries.
- The configured 100 ms emission and 500 ms refresh intervals are scheduling targets, not measured latency guarantees. The current connections set a connect timeout, but do not explicitly configure a command read timeout in these paths.

Consequently, this is best-effort telemetry that favors continued broker service, not a lossless audit channel.

## 6. Redis-to-broker verdict path

The worker writes both representations in one Redis transaction with a 120-second Redis TTL:

| Key | Representation and consumer |
|---|---|
| `tmq:verdictp:<client_id>` | Packed `level|score|expires_at|rate`, read by broker |
| `tmq:verdict:<client_id>` | Hash containing level, score, expiry, rate, reason and update time, used for readable operations data |

Example packed value: `2|0.7200|1789600120|10.0000`. Here level 2 means THROTTLE and the rate is 10 tokens per second; expiry is an absolute Unix timestamp in seconds. The broker does not calculate a new score: it uses the worker-provided level and rate.

The maintenance thread snapshots registered client IDs, pipelines one GET per client, parses returned strings and updates a cache protected by a read/write lock. Missing Redis keys do not immediately clear existing cache entries. Cache decay is handled separately.

| Level | Current behavior |
|---|---|
| 0 ALLOW | Continue normal broker authorization. |
| 1 WATCH | Continue normal broker authorization. |
| 2 THROTTLE | Apply a per-client token bucket to writes; reads and subscriptions continue. Refill rate comes from the verdict; bucket cap is twice the rate. Excess publishes are denied rather than delayed in a delivery queue. |
| 3 QUARANTINE | Permit only writes matching `tmq/quarantine/#` to continue to baseline ACL evaluation. Deny other writes, reads and subscriptions. |
| 4 KICK | Apply quarantine-like restrictions until the pending disconnect action is processed. |

The broad quarantine match does not itself grant cross-client access; the baseline ACL further restricts the permitted client namespace.

**Mode caveat found in the current code:** `monitor` turns access denials into `TMQ-WOULD` logs and continuation; `fingerprint` bypasses access restrictions and skips verdict refresh. However, `tmq_core_poll_actions()` does not check mode before processing cached KICK levels. Because monitor mode still refreshes verdicts, it can currently execute KICK. That discrepancy should be corrected and regression-tested before using monitor mode as a safe rollout gate.

**Expiry caveat:** the implementation lowers an expired verdict by one level when at least 60 seconds have elapsed since its last decay, then sets expiry to `now + 120`. Subsequent steps therefore wait roughly another 120 seconds. Existing prose describing a uniform “one level every 60 seconds” decay does not match this code. Specify and test the intended policy for Java rather than copying the prose.

KICK processing changes the cached level to QUARANTINE while claiming an action and calls broker APIs outside the cache lock. A later refresh of an unchanged Redis KICK can reintroduce the action. Connection-aware deduplication is a useful integration requirement.

## 7. What FlashMQ already demonstrates

FlashMQ shows that broker portability has already begun: both native adapters use the same serializer, queue, emitter, cache and enforcement rules. Its lifecycle and metadata are not identical to Mosquitto's:

- Anonymous connections are observed on their first ACL callback; other connections are observed through the login path.
- Keepalive and clean-session values are omitted, so synthetic keepalive-gap generation is unavailable for those clients.
- Subscription counts are adapter-maintained counters, not an authoritative inventory of unique persisted subscriptions.
- Expiry is derived from remaining time rather than the original interval.
- Publish/subscription telemetry is emitted after the adapter's TrustMQTT denial check, so TrustMQTT-denied operations are omitted from that path.

For HiveMQ, define whether telemetry describes attempted or accepted activity. A common JSON shape alone does not establish equal detection inputs across brokers.

## 8. Proposed HiveMQ implementation

Use a **Java HiveMQ extension** that implements the existing event/verdict contract. Reuse the Python worker and Redis formats. Reimplement the small broker-side runtime responsibilities in Java rather than trying to load the Mosquitto `.so` through JNI.

The self-hosted HiveMQ CE repository describes a Java broker with an extension system. Confirm the chosen edition and pinned release support the required SDK surface before building. [HiveMQ CE repository](https://github.com/hivemq/hivemq-community-edition)

```text
HiveMQ Java extension
  lifecycle listeners + packet interceptors
                  |
          normalized event DTOs
                  |
           bounded event queue
                  |
        background Redis stream writer --> tmq:events

  background verdict refresher <---------- tmq:verdictp:<client_id>
                  |
          local verdict/token state
                  |
     restrictions + disconnect action
```

Suggested new project structure, not yet created:

```text
hivemq-extension/
  pom.xml
  src/main/java/.../
    TrustMqttExtension.java
    ClientLifecycleListener.java
    EventNormalizer.java
    RedisEventEmitter.java
    VerdictRefresher.java
    VerdictCache.java
    EnforcementPolicy.java
    TrustMqttConfiguration.java
  src/main/resources/
    hivemq-extension.xml
    META-INF/services/com.hivemq.extension.sdk.api.ExtensionMain
  src/test/...
docker/hivemq/
  Dockerfile
  config.xml
```

The component names above express responsibilities, not a final SDK implementation. Register lifecycle listeners and per-client interceptors through the SDK. Capture CONNECT metadata early, but emit successful connection telemetry only at the appropriate successful lifecycle point; connection start alone is not successful authentication. [HiveMQ extension use cases](https://docs.hivemq.com/hivemq/latest/extensions/extension-sdk-usage.html)

Use background execution for Redis work and the Client Service for disconnection. Cancel scheduled work and close Redis resources when the extension stops. Copy needed metadata while callbacks are valid; do not hold broker packet objects or payload buffers in long-lived queues. [HiveMQ SDK services](https://docs.hivemq.com/hivemq/latest/extensions/services.html)

### Hook and behavior mapping

| Required behavior | HiveMQ integration approach to implement and verify |
|---|---|
| Connection metadata and successful connect | Lifecycle listener; correlate early CONNECT fields with successful authentication/connection handling. |
| Disconnect and registry cleanup | Lifecycle disconnect handling; define one canonical event policy to avoid accidental duplicates. |
| Publish telemetry | Inbound publish interceptor; select and document attempted-versus-accepted semantics. |
| Subscription telemetry | Subscribe/unsubscribe interceptors; emit one normalized record per filter and distinguish attempts from successful subscriptions. |
| THROTTLE | Local token bucket plus suitable publish prevention semantics; test all supported QoS/protocol combinations. |
| QUARANTINE publishing | Block non-quarantine traffic while retaining baseline authorization for permitted traffic. |
| QUARANTINE subscription and receive path | Reject new subscriptions and prevent outbound delivery to already-subscribed clients. |
| KICK | Client Service disconnect, with per-connection action tracking and a documented Will-message policy. |
| Queue, stats and verdict refresh | Java runtime components with bounded work, background I/O and the existing Redis envelope. |

HiveMQ supplies inbound/outbound interceptors; evaluate the appropriate publish prevention API for the chosen release. Outbound filtering is important because rejecting a new SUBSCRIBE does not stop an existing subscription from receiving data. [HiveMQ interceptor documentation](https://docs.hivemq.com/hivemq/latest/extensions/interceptors.html)

### Enforcement decisions that must be settled first

HiveMQ publish authorization failure can disconnect the client. Consequently, directly mapping every C `DENY` to `failAuthorization()` can turn THROTTLE into disconnection. Investigate interceptor-based publish prevention for soft restrictions and reserve explicit disconnection for KICK. Also use the SDK's continuation path where appropriate so TrustMQTT does not override baseline permissions. Extension ordering and configured default permissions require integration tests. [HiveMQ authorization documentation](https://docs.hivemq.com/hivemq/latest/extensions/authorization.html)

Resolve these additional details in the first integration spike:

1. Whether prevented publishes receive success or failure acknowledgements, and what the benchmark will count as delivery. A successful publisher acknowledgement is not sufficient proof of subscriber delivery.
2. Will-message authorization and emission must not consume ordinary live-publish tokens or create duplicate live telemetry accidentally.
3. Reconnects, client-ID takeover, persisted subscriptions, retained delivery and shared subscriptions need deliberate behavior.
4. Map MQTT 5 clean-start/session-expiry semantics carefully; do not label them as MQTT 3 clean-session without specifying the translation.
5. Either preserve the current activity-gap definition or introduce a versioned capability change. Do not silently make one broker's signal depend on PINGs while another's does not.
6. Validate packed verdict ranges and reject malformed/non-finite values without stopping refresh or bypassing baseline ACLs.

## 9. What can stay, what must change

| Area | Expected work |
|---|---|
| Python scoring, policy, incidents | Reuse for contract-compatible events; verify equivalent feature inputs. |
| Redis names/envelope | Preserve for the initial single-broker integration. |
| Broker plugin | Add Java implementation and HiveMQ SDK hooks. |
| Deployment | Add pinned HiveMQ image/configuration, extension packaging, health checks and optional Compose profile. |
| Makefile | Add build/run/evaluation targets once the new service exists. |
| Ingest tests | Add Java-produced fixtures for every normalized event type and missing optional fields. |
| Dashboard/replay assumptions | Check broker selectors, service hostnames, protocol behavior and delivery measurements. |
| CI | Add Java unit tests, extension packaging and a real HiveMQ/Redis smoke scenario. |

**Single-broker boundary:** existing verdict keys, worker state and database identity still use `client_id`. Adding `broker_id` to JSON does not isolate decisions. Start with one active broker per control plane. Running HiveMQ alongside Mosquitto/FlashMQ with overlapping IDs requires a coordinated `(broker_id, client_id)` migration across Redis, worker state, storage, fingerprints and dashboards.

## 10. Implementation sequence and acceptance criteria

1. Pin self-hosted HiveMQ edition, release, SDK and JDK. Prove extension startup and inspect actual hook ordering and publish-prevention behavior.
2. Write shared JSON/verdict fixtures and settle attempted-versus-accepted telemetry, mode behavior and expiry semantics.
3. Implement connection/publish/subscription telemetry, bounded queue and Redis emission. Confirm the existing parser accepts every event without dead-letter entries.
4. Add background verdict refresh and local access decisions. Test monitor mode explicitly, including a level-4 verdict that must not disconnect.
5. Implement throttle, quarantine and KICK with baseline ACL composition. Validate outbound delivery and acknowledgement behavior with an independent subscriber.
6. Add deployment and replay targets. Run the existing attack scenarios against HiveMQ and compare event coverage, detection, actual mitigation and broker latency.
7. Test Redis loss/recovery, malformed verdicts, expiry, queue overflow, repeated KICK, extension shutdown/reload and reconnects. Record loss counters and verify broker availability.

Current C unit-test targets cover ring, cache, token bucket, enforcement and configuration. They do not prove full callback mapping, Redis networking or HiveMQ parity; no new passing test claim is made by this note.

HiveMQ integration is complete when the extension builds reproducibly, the existing worker accepts its events, every verdict produces the specified client-visible behavior, normal permissions remain effective, and replay/failure results are recorded. The current local broker code is a useful reference implementation, with the mode, expiry and telemetry differences above treated as explicit design issues.

## 11. Local source references

- [Shared core interface](../plugin/include/trustmqtt_core.h)
- [Core runtime](../plugin/src/core.c)
- [Mosquitto adapter](../plugin/src/plugin.c)
- [FlashMQ adapter](../plugin/src/flashmq_adapter.cpp)
- [Redis event emitter](../plugin/src/emitter.c)
- [Queue](../plugin/src/ring.c)
- [Verdict cache and expiry](../plugin/src/verdict_cache.c)
- [Access restrictions](../plugin/src/enforce.c)
- [Defaults and options](../plugin/src/config.c)
- [Event receiver schema](../tmq_worker/ingest.py)
- [Verdict writer](../tmq_worker/verdicts.py)
- [Deployment](../docker-compose.yml)
- [Existing broker portability note](BROKER_ADAPTERS.md)
