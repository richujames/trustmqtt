# Broker-neutral TrustMQTT adapters

Update: a HiveMQ CE Java extension is now available in `hivemq-extension/`.
See [HiveMQ and MQTTX](HIVEMQ_MQTTX.md) for setup and the implemented compatibility boundaries.
The research priority table below records the earlier adapter selection order.

## Decision

TrustMQTT is now split into a broker-neutral C runtime and thin broker adapters.
The reusable artifact is the C core, not a single plugin `.so`: brokers expose
different callback ABIs and some do not load native code at all.

The first additional broker is **FlashMQ 1.27.x**. It is the closest production
fit because its native plugin is an ELF shared object, its API exposes login,
disconnect, publish, subscribe, unsubscribe and ACL decisions, and it provides
a thread-safe client-removal API. The same C core is linked into both the
Mosquitto and FlashMQ adapters.

```text
                     broker-specific                  broker-neutral
MQTT client -> [Mosquitto callbacks] --\
                                        > [TrustMQTT C core] -> Redis Stream
MQTT client -> [FlashMQ C++ ABI] ------/          ^                |
                                                    |                v
                                              verdict cache <- scoring worker
                                                    |
                                      adapter maps deny/kick to broker API
```

The stable boundary is `plugin/include/trustmqtt_core.h`:

- adapters submit normalized connect, disconnect, publish, subscribe,
  unsubscribe and authentication observations;
- the core serializes the existing schema and adds `broker` and `broker_id`;
- Redis emission and verdict refresh run on core-owned background threads;
- the core returns `TMQ_ENFORCE_DEFER` or `TMQ_ENFORCE_DENY`; each adapter maps
  that to its broker's authorization result;
- kick is an injected adapter callback, claimed through `tmq_core_poll_actions`
  from the broker's periodic hook; broker APIs therefore stay on a
  broker-owned thread and the core never includes broker SDK types.

## Broker research and priority

| Broker | Deployment/maturity signal | Extension surface | TrustMQTT fit | Priority |
|---|---|---|---|---|
| FlashMQ | Active releases, commercial hosted/self-hosted support, documented multi-million-client design | Native C++ `.so`; login, disconnect, publish/subscribe ACL, unsubscribe, periodic callback, queued client removal | Full telemetry and enforcement with the shared C core; some connect fields are not exposed | **Implemented first** |
| VerneMQ | Apache-2.0 distributed broker; project lists deployments across many countries and industrial use | Native Erlang/OTP plugins; Lua and HTTP webhooks; MQTT 5 publish/subscribe authorization hooks | Full contract is possible through a local sidecar, but the Mosquitto/FlashMQ `.so` cannot be loaded | Phase 2 sidecar adapter |
| HiveMQ CE | Apache-2.0; its repository says the core is used in many large MQTT deployments | Java Extension SDK with auth, authorization, client initializers and packet interceptors | Full contract is possible with a Java adapter; JNI would add crash/GC complexity without pipeline benefit | Phase 3 native Java adapter |
| NanoMQ | MIT-licensed pure-C edge broker with active releases and MQTT 5 support | Documented webhooks currently expose connack, disconnect and publish; documented HTTP authorization is not yet feature-equivalent for publish/subscribe | Monitor-only without patching NanoMQ; cannot provide TrustMQTT quarantine/throttle parity today | Revisit when hooks mature |
| EMQX | Widely deployed and technically mature, but current releases (5.9+) use BSL 1.1, which EMQX itself describes as source-available rather than OSI open source | Erlang hooks and a gRPC ExHook cover lifecycle, publish and authorization | Strong sidecar fit, but excluded from the strict current-open-source shortlist; Apache-2.0 5.8 reached end-of-life on February 28, 2026 | License/product decision required |

Primary sources:

- [FlashMQ plugin API and threading model](https://www.flashmq.org/documentation/authentication-plugin/),
  [native API header](https://github.com/halfgaar/FlashMQ/blob/v1.27.1/flashmq_plugin.h),
  [configuration/plugin loading](https://www.flashmq.org/man/flashmq.conf.5), and
  [OSL-3 licensing statement](https://www.flashmq.org/2023/06/05/license-changed-to-osl-3/).
- [VerneMQ repository, license and deployment statement](https://github.com/vernemq/vernemq),
  [plugin architecture](https://docs.vernemq.com/plugin-development/introduction),
  [publish authorization hooks](https://docs.vernemq.com/plugin-development/publishflow), and
  [HTTP webhook behavior](https://docs.vernemq.com/plugin-development/webhookplugins).
- [HiveMQ CE repository, license and deployment statement](https://github.com/hivemq/hivemq-community-edition)
  and [Extension SDK](https://github.com/hivemq/hivemq-extension-sdk).
- [NanoMQ repository and MIT license](https://github.com/nanomq/nanomq),
  [current webhook events](https://nanomq.io/docs/en/latest/config-description/webhook.html), and
  [HTTP authorization limitations](https://nanomq.io/docs/en/latest/access-control/http.html).
- [EMQX's BSL 1.1 licensing FAQ](https://www.emqx.com/en/content/license-faq),
  [5.8 open-source end-of-life notice](https://www.emqx.com/en/news/a-notice-on-the-emqx-5-8-open-source-version),
  [current repository](https://github.com/emqx/emqx), and
  [gRPC ExHook surface](https://docs.emqx.com/en/emqx/latest/extensions/exhook.html).

## FlashMQ implementation

The adapter is in `plugin/src/flashmq_adapter.cpp`; all scoring-facing behavior
remains in `plugin/src/core.c`. Build and run it with:

```bash
make up-flashmq
make eval-flashmq
```

The host port is `1884`; inside the Compose network the worker/replay connects
to `flashmq:1883`. The standard Mosquitto workflow and its host port `1883`
remain unchanged.

FlashMQ's public API does not expose every Mosquitto field. The adapter follows
an **omit, never invent** rule:

| Normalized signal | Mosquitto | FlashMQ |
|---|---:|---:|
| Successful connect | Exact post-auth callback | Login success, or first ACL callback for allowed anonymous clients |
| IP address | Yes | Yes for authenticated login; absent for anonymous fallback |
| Keepalive / clean session | Yes | Absent |
| Inbound publish, QoS, retain, payload length | Yes | Yes |
| MQTT 5 content type / user-property count | Yes | Yes |
| Message expiry | Original interval | Approximate remaining seconds |
| Subscribe / unsubscribe | Yes | Yes |
| Exact subscription count | Broker value | Adapter-local count since connection |
| THROTTLE / QUARANTINE ACL denial | Yes | Yes |
| KICK | Mosquitto kick API | FlashMQ queued client removal |
| `ka_gap` | Yes | Disabled because keepalive is not exposed |

FlashMQ's password and ACL files take precedence over its plugin. TrustMQTT's
login callback deliberately observes and returns success; production must set
`allow_anonymous false` and configure `mosquitto_password_file`. The checked-in
configuration keeps anonymous access enabled only to preserve the existing
synthetic replay workflow.

## Adding another in-process adapter

1. Verify the broker exposes post-auth connect/disconnect, inbound publish,
   subscribe/unsubscribe, synchronous publish/subscribe authorization, a safe
   disconnect API, and a periodic or thread-safe lifecycle hook.
2. Translate SDK fields into `trustmqtt_core.h`; never include broker headers
   in `core.c` and never add broker conditionals there.
3. Map normal core continuation to the broker's equivalent of “let normal ACLs
   decide.” Map only `TMQ_ENFORCE_DENY` to a broker denial.
4. Keep network I/O out of broker callbacks. The core's background emitter and
   verdict refresher are the only Redis users. Call `tmq_core_poll_actions`
   from the broker's periodic hook for pending kick/disconnect actions.
5. Document missing fields and add an adapter capability test. Missing values
   must be omitted, not filled with zero/false unless zero is the real value.
6. Run the same replay scenario and compare accepted event types, dead-letter
   count, time-to-mitigation, false-quarantine rate and broker latency.

## Out-of-process adapters (VerneMQ, HiveMQ CE, and EMQX if licensed)

For brokers whose native runtime is not C/C++, reuse the **event/verdict
contract**, not the plugin binary:

```text
broker hook -> local adapter/sidecar -> tmq:events -> worker
broker auth hook <- local in-memory verdict mirror <- tmq:verdictp:*
```

The sidecar must be node-local, bounded-time and fail-open. Telemetry hooks may
be asynchronous. Authorization hooks must read only a local verdict mirror;
they must not make a synchronous Redis/HTTP request on every publish. VerneMQ's
HTTP webhook cache helps, but a small native Erlang adapter with an in-memory
mirror is safer at high message rates. HiveMQ should use its Java SDK and the
same normalized JSON rather than JNI-loading the C library.

## Current scaling boundary

The new adapter layer supports **swapping the broker implementation** while
keeping the pipeline unchanged. It does not yet make one worker deployment
safe for multiple brokers that reuse the same MQTT `client_id`: Redis verdict
keys and the SQL client uniqueness constraint are still keyed by `client_id`
alone.

Before running brokers concurrently in one TrustMQTT control plane, migrate to
a canonical subject key `(broker_id, client_id)` in Redis, worker state, SQL
constraints, fingerprints and Grafana variables. Until then, use one active
broker service per deployment and keep `broker_id` stable for observability.

## What to revisit as the system grows

- Move from per-client `GET` refresh to a broker-scoped verdict stream or
  pub/sub invalidation when active clients exceed the current 4,096 snapshot.
- Add adapter capability metadata so scoring can disable unsupported features
  explicitly rather than infer support from missing event fields.
- Namespace stream consumer groups and verdict keys by tenant/broker before
  operating a shared multi-cluster control plane.
- Add ABI/version conformance CI against pinned Mosquitto and FlashMQ releases;
  both broker plugin APIs can change independently of the TrustMQTT core ABI.
