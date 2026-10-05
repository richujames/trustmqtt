# HiveMQ and MQTTX

TrustMQTT now includes a Java extension for **HiveMQ CE 2026.5**, built against Extension SDK **4.52.0**, and a synthetic traffic runner using **MQTTX CLI 1.12.1**. The Java extension produces the existing Redis event envelope and consumes the existing packed verdicts. The Python scoring worker remains shared.

## Run the stack

Run from the `trustmqtt` directory. Use one active broker per Redis/worker deployment: verdict keys still identify clients by `client_id`, not `(broker_id, client_id)`.

```sh
# If switching from another broker, stop its service first.
docker compose stop mosquitto flashmq
docker compose --profile hivemq up -d --build hivemq redis postgres tmq-worker grafana

# Or: make up-hivemq
```

HiveMQ is available at `localhost:1885`; other Compose services reach `hivemq:1883`. Redis is `localhost:6379`; Grafana is `localhost:3000`.

The checked-in image disables HiveMQ's bundled allow-all extension. Compose explicitly enables TrustMQTT's **development authenticator** (`TMQ_DEMO_AUTH=true`), granting a client only `fleet/<client_id>/#` and `tmq/quarantine/<client_id>/#`. Client IDs with `/`, `+` or `#` are rejected in this mode. This supplies client-ID-based synthetic test scopes, not proof of device identity. It does not reproduce plant-user password authentication. For a real deployment, disable demo authentication and install/configure an authentication extension with baseline permissions. TrustMQTT itself continues to apply behavioral restrictions.

`TMQ_MODE` selects `enforce`, `monitor`, or `fingerprint`; default is `enforce`. Monitor logs would-deny operations and never kicks. Fingerprint bypasses restrictions and verdict reads. If changing modes, also align the worker's `config/tmq.yaml`; the Compose environment variable only selects the HiveMQ extension mode. `TMQ_BROKER_ID` defaults to `default`.

## Generate synthetic messages with MQTTX

```sh
# 10 simulated devices; 1,000 messages total, at 1 second per device.
docker compose --profile traffic run --rm --build mqttx

# Higher fixed-rate traffic: 20 ms per device, bounded total and runtime.
docker compose --profile traffic run --rm mqttx --scenario burst --clients 10 --messages 2000

# After 30 messages per device, change topic class and increase payload size.
docker compose --profile traffic run --rm mqttx --scenario scope-expansion --clients 10 --messages 1000

# Target the existing broker services instead.
docker compose --profile traffic run --rm mqttx --host mosquitto --port 1883
docker compose --profile traffic run --rm mqttx --host flashmq --port 1883
```

The runner creates `tmqx-0`, `tmqx-1`, etc. and publishes under each client's own namespace. Change `--prefix` when running independent fleets. Reusing the same IDs intentionally reuses learned identity state; simultaneous runs with the same IDs cause client takeover.

| Option | Default | Meaning |
|---|---|---|
| `--host`, `--port` | Container: `hivemq:1883`; local: `localhost:1885` | Broker endpoint |
| `--clients` | 10 | Connections, maximum 4,096 in this runner |
| `--messages` | 1,000 | Global total, not messages per device; must be positive |
| `--interval` | 1,000 ms | Per-device publishing interval; burst preset uses 20 ms |
| `--scenario` | `normal` | `normal`, `burst`, `scope-expansion` |
| `--warmup` | 30 | Messages per device before scope expansion |
| `--qos` | 1 | 0, 1 or 2; runner uses MQTT 5 |
| `--prefix` | `tmqx` | Client IDs are `<prefix>-<index>` |
| `--timeout` | 180 seconds | Stop a stalled/long run; timeout exit code 124 |
| `--dry-run` | off | Print resolved configuration and MQTTX arguments |

Payloads are JSON with device ID, sequence, timestamp, temperature, humidity and a synthetic marker. The scope-expansion scenario changes the topic from `telemetry/temperature` to `diagnostics/configuration` and adds 512 characters. All scenario topics remain within the static client scope so that behavioral detection can be exercised separately from static permission denial.

Burst is a fixed higher-rate run; it is not a timed rate ramp. These are synthetic MQTT messages, not malformed raw-wire packet fuzzing. Merely changing a temperature value does not demonstrate detection because TrustMQTT does not inspect payload content. Short smoke runs also do not establish a trained behavioral baseline. For meaningful model experiments, allow multiple 60-second feature windows and the configured learning lifecycle, then compare scenario outcomes with the existing replay harness.

Local use without Docker:

```sh
cd traffic-simulator/mqttx
npm ci
npm start -- --host localhost --port 1885 --scenario normal
npm test
```

The Docker runner uses Node 22. MQTTX's package declares Node 18 and therefore prints an engine warning on newer Node versions; use the project's pinned dependency and verify the included smoke test when upgrading it.

## HiveMQ behavior and compatibility

- Lifecycle events report authentication observation, successful connection and disconnect. Publish/subscribe/unsubscribe telemetry describes **attempted activity**, including TrustMQTT-prevented publishes.
- Events go to `tmq:events` as a single field `v` containing schema-v1 JSON. `broker` is `hivemq` and `broker_id` is configurable. Raw payloads and passwords are not emitted.
- Exact subscription count is omitted. The extension currently does not implement optional payload hashing or a separate `client_offline` event. MQTT 5 clean-session compatibility means clean start plus zero session expiry. Transport is reported as `mqtt` because this deployment exposes only TCP MQTT.
- The 8,192-entry queue is drained in batches of 512 by background tasks; Redis socket/connect timeouts are one second. Events are best effort, and dropped batches count toward `plugin_stats.dropped_events`. There is no disk spool or delivery retry guarantee. Java refresh uses one `MGET` for currently tracked clients.
- Redis host/port default to `redis:6379`; `TMQ_EMIT_MS` defaults to 100 and `TMQ_REFRESH_MS` to 500. Redis authentication/TLS support is not yet configured by this extension.
- THROTTLE uses a local token bucket. QUARANTINE permits quarantine-namespace writes to proceed to baseline permission checks, rejects new subscriptions, and prevents outbound delivery to existing subscribers.
- Soft publish restrictions use the SDK's delivery-prevention API with a success acknowledgement. Negative acknowledgement reasons can disconnect clients, so restricted messages are acknowledged but not delivered onward. Measure subscriber delivery, not just publisher acknowledgements, when evaluating mitigation. KICK uses Client Service and suppresses the Will. Confirmed successful disconnects produce `enforcement` events. Repeated KICK refreshes are deduplicated within the tracked connection until a lower verdict appears.
- Local expiry lowers the level once after its expiry and gives the next level another 120 seconds, following the current C implementation's broad decay behavior. Token refill uses a monotonic clock. Missing/malformed Redis verdicts retain the last valid value until decay; missing local state adds no behavioral restriction.
- `ka_gap` retains the existing application-activity-gap definition. It does not claim to measure PINGREQ timing or prove protocol keepalive failure.
- Will messages are not counted as ordinary inbound publish telemetry or charged to the live-publish token bucket. Default topic permissions still govern Will authorization. Session persistence, cluster operation and hot reload of an extension onto already-connected clients are outside the first integration's validation scope; restart the broker when installing the extension.

## Verification

```sh
# Java unit tests and packaged extension (JDK 21 and Maven 3.9+).
mvn -B -f hivemq-extension/pom.xml verify

# Live protocol/enforcement checks: run with no scoring worker so injected
# test verdicts cannot be overwritten by the worker.
docker compose stop tmq-worker
docker compose --profile hivemq up -d hivemq redis
python hivemq-extension/smoke.py

# Resume scoring after the isolated enforcement test.
docker compose up -d tmq-worker

# Existing attack replay through HiveMQ.
make eval-hivemq
```

`smoke.py` verifies MQTT 3.1.1 and MQTT 5 at QoS 0/1/2, self-delivery, namespace ACL rejection, throttle, quarantine, recovery, KICK, and Redis events accepted by the Python parser. It uses unique IDs and deletes only its own verdict keys. Java tests additionally cover malformed verdicts, Redis failure, queue overflow, mode guards and KICK deduplication.

`traffic-simulator/mqttx/smoke.cjs` verifies actual MQTTX delivery using a separate observer against an **isolated broker permitting `fleet/#`**, such as a temporary local Mosquitto. The scoped HiveMQ demo authenticator intentionally does not grant that observer broad access.

CI builds both Docker images, runs the Java tests during image creation, executes the live HiveMQ checks, and runs bounded MQTTX traffic. The existing C and Python suites remain separate CI jobs.

## References

- [HiveMQ extension sources](../hivemq-extension/src/main/java/org/trustmqtt/hivemq/)
- [MQTTX runner](../traffic-simulator/mqttx/run.cjs)
- [Broker-to-Redis design note](BROKER_TO_REDIS_HIVEMQ_NOTE.md) — pre-implementation inspection dated September 17; this document describes the added adapter.
- [HiveMQ Extension SDK](https://github.com/hivemq/hivemq-extension-sdk)
- [MQTTX CLI usage](https://mqttx.app/docs/cli/get-started)
