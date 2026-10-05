# TrustMQTT two-week update: HiveMQ, MQTTX and Windows desktop testing

## Outcome

During this two-week period, TrustMQTT gained a HiveMQ Community Edition adapter, repeatable MQTTX synthetic traffic, and a Windows MQTTX Desktop path. I also mapped the existing Python worker myself as part of the handover process. This gave me a clear understanding of how broker events move through ingestion, feature extraction, behavioral analysis, scoring, verdict generation and storage, so I can independently add the next set of features.

The desktop path lets a tester create packets in the MQTTX application on the Windows host, send them through a Docker-published MQTT port, and observe those same packets as TrustMQTT broker events before the worker aggregates and scores them.

```text
MQTTX Desktop on Windows
        |
        | localhost:1885 (HiveMQ) or localhost:1884 (FlashMQ)
        v
Docker broker container + TrustMQTT adapter
        |
        v
Redis tmq:events -> scoring worker -> feature windows / verdicts -> PostgreSQL
```

## Week 1: HiveMQ broker integration

- Added a Java extension for HiveMQ CE 2026.5 using Extension SDK 4.52.0.
- Reused the TrustMQTT Redis event envelope so all broker adapters share the existing worker.
- Added lifecycle, publish, subscribe, unsubscribe and enforcement telemetry.
- Added client-ID-scoped development permissions for synthetic testing.
- Implemented ALLOW, THROTTLE, QUARANTINE, recovery and KICK behavior.
- Validated MQTT 5 and MQTT 3.1.1 at QoS 0, 1 and 2.
- Corrected worker handling for adapters that report `sub_count` as unavailable.

The live test passed all six protocol/QoS combinations. Each combination verified normal delivery, throttling without an unwanted disconnect, quarantine, recovery, KICK and Redis-schema parsing.

## Week 2: MQTTX traffic, Python-worker mapping and integration testing

- Added an MQTTX CLI container and bounded synthetic traffic runner.
- Added normal, burst and scope-expansion scenarios.
- Used deterministic client IDs and topics under `fleet/<client-id>/#`.
- Validated three MQTTX clients sending 30 messages through FlashMQ.
- Confirmed 3 connects, 30 publishes, 21 scope-expansion publishes and 3 clean disconnects in Redis.
- Updated and rebuilt FlashMQ 1.27.1 with the native TrustMQTT adapter.
- Added live Docker test transcripts and result screenshots under `reports/`.
- Mapped the Python worker from input to output so responsibility for future worker development can be continued after the team handover.
- Traced how Redis events are validated, converted into feature windows, evaluated by the behavioral components and written as verdicts and database records.
- Identified the extension points for adding new features, scoring inputs, policy rules, storage fields and testing scenarios.
- Reviewed the worker configuration values that control feature windows, learning behavior, scoring weights and enforcement thresholds.

## Python worker mapping for the next development phase

I mapped the worker as the following processing sequence:

```text
Broker adapter
    -> Redis stream: tmq:events
    -> Event parsing and schema validation
    -> Per-client feature-window aggregation
    -> Behavioral contract/FSM analysis
    -> Drift and fleet analysis
    -> Trust-score calculation
    -> Policy and enforcement level
    -> Packed Redis verdict
    -> PostgreSQL history and incident records
```

The main extension points are:

| Area | Current responsibility | Where the next features can be added |
|---|---|---|
| Ingestion | Parses and validates broker events | New event types or optional broker fields |
| Feature extraction | Builds per-client 60-second windows | New packet, topic, rate or session features |
| Behavioral model | Tracks expected client behavior | New state transitions and protocol checks |
| Drift and fleet analysis | Detects individual and coordinated change | New comparison models or fleet signals |
| Trust scoring | Combines behavioral components | New score inputs or revised weights |
| Policy | Converts scores into enforcement levels | New thresholds, rules or enforcement actions |
| Storage | Saves sessions, windows, verdicts and incidents | New fields, reports and dashboard queries |

This mapping means future work can be added to the relevant stage without rewriting the broker adapters or the entire worker. It also provides a clean point for testing each new feature separately before running the complete Docker integration flow.

## Windows MQTTX Desktop workflow

Start the observer in PowerShell before clicking Connect in MQTTX Desktop:

```powershell
.\scripts\windows_mqttx_test.ps1 -Broker hivemq -ClientId windows-mqttx-01 -Duration 75
```

For FlashMQ:

```powershell
.\scripts\windows_mqttx_test.ps1 -Broker flashmq -ClientId windows-mqttx-01 -Duration 75
```

The script starts the selected broker, Redis, PostgreSQL and the scoring worker. It prints the exact MQTTX settings and watches only the selected client's events. Start the observer first so the connection event is included.

| Setting | HiveMQ | FlashMQ |
|---|---:|---:|
| Host | `localhost` | `localhost` |
| Port | `1885` | `1884` |
| Protocol | MQTT 5.0 | MQTT 5.0 |
| Client ID | `windows-mqttx-01` | `windows-mqttx-01` |
| Username/password | empty | empty |
| Clean Start | enabled | enabled |
| Keep Alive | 30 seconds | 30 seconds |
| Topic | `fleet/windows-mqttx-01/telemetry/temperature` | same |
| QoS | 1 | 1 |

Example payload:

```json
{"device_id":"windows-mqttx-01","temperature":24.7,"humidity":51,"synthetic":true}
```

The topic must contain the exact MQTTX client ID. The development ACL grants a client access to `fleet/<its-client-id>/#`; publishing under another client's namespace should be rejected and is an intentional ACL test.

## Test parameters to exercise from MQTTX Desktop

| Test | MQTTX action | Expected observation |
|---|---|---|
| Normal | Send steadily to `telemetry/temperature` | Connect and publish events; ordinary feature window |
| Burst | Send many messages quickly | Higher `msg_rate` in the next feature window |
| Scope expansion | Publish to `diagnostics/configuration` with a larger body | New topic class and larger average payload |
| ACL rejection | Publish to `fleet/another-device/telemetry` | Broker rejects the out-of-scope operation |
| Lifecycle | Disconnect the MQTTX connection cleanly | Disconnect event appears in Redis |

TrustMQTT uses 60-second feature windows. Keep the observer running for at least 75 seconds to allow a window to close. A newly seen client remains in learning until its configured learning limit is reached, so a short manual run proves packet capture and feature extraction but may show `pending / learning` instead of an escalated verdict. Reuse the same client ID for baseline-building tests; use a new client ID when isolation is required.

## Verified status

- HiveMQ live enforcement suite: passed.
- FlashMQ native adapter and MQTTX synthetic flow: passed.
- Redis broker identity, lifecycle and publish counts: passed.
- Windows host ports are published and ready for MQTTX Desktop.
- The PowerShell observer reports the broker adapter, event counts, topics and latest packed verdict for the chosen desktop client.
- The Python worker flow and its feature-extension points have been mapped for continued development.

## Remaining work

- Use the Python-worker map to select and implement the next behavioral features.
- Add focused tests for each new feature before repeating the complete broker-to-worker integration test.
- Perform a longer Windows MQTTX Desktop baseline run using one stable client ID.
- Capture several complete 60-second normal windows before injecting burst and scope-expansion traffic.
- Compare the feature windows and verdict history in PostgreSQL and Grafana.
- Replace development anonymous/scoped access with deployment authentication before any non-local environment is used.
