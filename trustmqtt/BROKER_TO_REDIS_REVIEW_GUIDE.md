# TrustMQTT code-review script: Mosquitto broker to Redis

This guide uses simple spoken language. You can read the quoted parts directly
during the review. The bullet points tell you what each group of code lines does.

It covers only your part of the project:

```text
MQTT client
   -> Mosquitto broker
   -> Mosquitto callback adapter (`plugin/src/plugin.c`)
   -> broker-neutral TrustMQTT core (`plugin/src/core.c`)
   -> in-memory ring buffer (`plugin/src/ring.c`)
   -> background Redis emitter (`plugin/src/emitter.c`)
   -> Redis Stream `tmq:events`                    <-- handoff point
```

It also explains how the plugin receives a decision back from Redis. For example,
the decision may say to slow down or disconnect a device. This is still part of
the broker plugin. Do not explain the Python worker, machine-learning model,
database, or LLM. Those belong to your teammate.

## Simple meanings of important words

- **Broker:** The MQTT server. In this project, the broker is Mosquitto.
- **Plugin:** Extra C code loaded inside Mosquitto.
- **Callback:** A function Mosquitto automatically calls when something happens.
  For example, Mosquitto calls one function when a device connects and another
  when it publishes a message.
- **Event:** A small record describing what happened, such as connect or publish.
- **Adapter:** Code that changes Mosquitto-specific data into the common format
  understood by TrustMQTT.
- **Core:** The shared part that creates events and applies security decisions.
- **JSON:** A text format made from names and values, such as
  `{"event":"publish","client_id":"sensor-01"}`.
- **Ring buffer:** A fixed-size first-in-first-out queue. This one holds 8,192
  event strings. It reuses its array from the beginning after reaching the end.
- **Emitter:** The background thread that takes events from the queue and sends
  them to Redis.
- **Redis Stream:** The ordered list in Redis where the events are stored.
- **Verdict:** The current security decision for a device: allow, watch,
  throttle, quarantine, or kick.
- **Fail-open:** If the security-supporting system fails, MQTT traffic continues.
  The project chooses broker availability over blocking every device.
- **Defer:** TrustMQTT does not make the final allow decision. Mosquitto must
  continue checking its normal ACL rules.

## 1. Opening: say this first

> My section starts when an MQTT device sends something to Mosquitto. It ends
> when the TrustMQTT plugin stores an event in the Redis Stream named
> `tmq:events`. Mosquitto tells our plugin about actions such as connect,
> publish, subscribe, and disconnect. The plugin changes that information into
> one common format. The core turns it into JSON and puts it in a temporary
> queue in memory. A separate background thread takes events from that queue
> and sends them to Redis. My teammate's section starts after the event reaches
> Redis.

Important terminology:

- Say **Mosquitto**, not “Mosquito.”
- Say **Redis**, not “Reddish.”
- Say **resolved-semantic event analysis**, not packet sniffing or wire-level inspection. The plugin receives already-parsed information exposed by Mosquitto's public callback API.
- The plugin is an **in-process shared library**, `trustmqtt_plugin.so`, loaded by Mosquitto.

## 2. One-minute architecture explanation

> When a device does something, Mosquitto calls the matching function in
> `plugin.c`. That file collects the useful details and passes them to `core.c`.
> The core adds the time, event type, broker name, and broker ID. It converts the
> result into JSON. `ring.c` temporarily stores the JSON in a queue that can hold
> 8,192 events. `emitter.c` removes up to 512 events at a time and sends them to
> Redis. The command is `XADD tmq:events MAXLEN ~ 1000000 * v <json>`.

> The system is fail-open. This means MQTT traffic continues if Redis is down or
> the event queue is full. If the queue already contains 8,192 events and 10
> more arrive before the emitter creates space, all 10 new events are dropped.
> The existing 8,192 events stay in the queue. The number 512 is only the
> emitter's batch size; it is not the queue limit. This design keeps the broker
> available, but some security events may be lost during overload or failure.

## 3. Startup sequence: explain it in this order

1. Docker Compose starts Redis and checks that it answers.
2. It builds and starts the Mosquitto image.
3. Mosquitto reads `mosquitto.conf` and loads `/mosquitto/plugin/trustmqtt_plugin.so`.
4. Mosquitto calls `mosquitto_plugin_version()` and then `mosquitto_plugin_init()`.
5. The adapter loads its normal settings, applies the settings from the config
   file, and creates the shared core.
6. The core creates the event queue, saved security decisions, connected-device
   list, Redis-sending thread, and maintenance thread.
7. The adapter tells Mosquitto which nine functions to call for different events.
8. MQTT traffic can now be observed and, depending on mode and cached verdict, enforced.

## 4. File-by-file and line-by-line walkthrough

Line numbers below refer to the current repository version. Blank lines only separate logical blocks and need no spoken explanation.

### `docker-compose.yml`

**Simple idea:** This file starts the services and puts Mosquitto and Redis on
the same private network. Think of it as the file that connects the boxes in
the architecture diagram.

Only lines 1–53 are in the broker-to-Redis path.

- **Lines 1–3:** Define a private Docker bridge network named `tmqnet`. Mosquitto reaches Redis by the service DNS name `redis` on this network.
- **Lines 5–14:** Declare persistent volumes. These are mostly for later components; Redis itself has no volume here, so its data is ephemeral across container removal.
- **Lines 16–20:** Start the services section and define the Mosquitto service. Its build context is the repository root, while the selected Dockerfile is `docker/mosquitto/Dockerfile`.
- **Lines 21–23:** Publish MQTT TCP port 1883 and WebSocket port 9001 from the container to the host.
- **Line 24:** Attach Mosquitto to `tmqnet`.
- **Lines 25–27:** Do not start Mosquitto until the Redis health check succeeds. This improves clean startup, although runtime Redis failures are still handled by the plugin.
- **Lines 29–41:** Define optional FlashMQ support. Say that it is not part of this review.
- **Lines 43–47:** Run Redis 7 Alpine, expose port 6379, and attach it to the same network.
- **Lines 48–52:** Run `redis-cli ping` every five seconds, allow three seconds per attempt, and retry five times. A successful `PONG` marks Redis healthy.
- **Lines 54 onward:** Postgres, the Python worker, and Grafana are downstream and outside this section. The handoff has already happened at Redis.

Suggested sentence:

> Compose provides service discovery and startup ordering: the plugin uses host name `redis`, Docker resolves it on `tmqnet`, and Mosquitto starts only after Redis answers its health check.

### `docker/mosquitto/Dockerfile`

**Simple idea:** This file creates the Mosquitto container image. It first
builds Mosquitto, then builds our plugin, and finally copies only the files
needed to run them into a smaller and safer image.

- **Lines 1–7:** Explain why the image builds from source: the adapter needs Mosquitto 2.1 callbacks, especially connect and client-offline events. The repository root must be the build context because both the Dockerfile and plugin source are copied.
- **Line 9:** Sets Mosquitto 2.1.0 as an overridable build argument.
- **Lines 11–13:** Begin stage one, named `mosquitto-builder`, using Alpine 3.19.
- **Lines 14–18:** Install the compiler, CMake, Git, and Mosquitto's development dependencies.
- **Lines 20–22:** Clone only the selected Mosquitto tag and only its latest commit, reducing download size.
- **Lines 24–28:** Configure a release build with WebSockets enabled and docs/tests/static libraries disabled, compile in parallel, and install into `/opt/mosquitto`.
- **Lines 30–33:** Begin a second clean build stage for the TrustMQTT plugin.
- **Lines 35–38:** Install plugin build dependencies: hiredis for Redis, OpenSSL for SHA-256, cJSON for JSON, plus compiler tooling.
- **Lines 40–44:** Copy the Mosquitto headers and TrustMQTT source/build file into the plugin builder.
- **Lines 46–48:** Configure CMake with the exact broker include directory and build `trustmqtt_plugin.so`.
- **Lines 50–53:** Begin the small runtime image; build tools do not enter the final image.
- **Lines 55–57:** Install only runtime libraries and create an unprivileged `mosquitto` user/group.
- **Lines 59–61:** Copy the built broker, its libraries, and the plugin shared object into their runtime locations.
- **Lines 63–64:** Create Mosquitto config/data/log directories and give the broker user ownership.
- **Lines 66–71:** Copy broker configuration and ACLs, then secure the ACL file with Mosquitto ownership and `0600` permissions.
- **Lines 73–74:** Make the new shared libraries discoverable and document ports 1883 and 9001.
- **Lines 76–78:** Drop privileges to the Mosquitto user, make Mosquitto the entry point, and pass the TrustMQTT config path.

### `docker/mosquitto/mosquitto.conf`

**Simple idea:** This is Mosquitto's main settings file. It opens the MQTT
ports, enables the ACL file, loads our plugin, and passes settings to it.

- **Lines 1–2:** Identify the configuration and warn that anonymous access is development-only.
- **Line 3:** Listen for normal MQTT TCP connections on port 1883.
- **Lines 4–5:** Add a second listener on port 9001 and set that listener's protocol to WebSockets.
- **Line 7:** Permit unauthenticated clients. Explicitly call this a development setting and a production security risk.
- **Line 8:** Load the static topic rules from `acl.conf`.
- **Lines 10–11:** Persist broker state under `/mosquitto/data/`.
- **Line 12:** Send broker logs to container standard output.
- **Line 14:** Load the compiled TrustMQTT shared library.
- **Line 15:** Set the stable broker instance identifier included in each event.
- **Lines 16–17:** Tell the plugin to connect to the Redis service on port 6379.
- **Line 18:** Let the emitter collect events for 100 ms before each drain cycle.
- **Line 19:** Refresh downstream verdicts every 500 ms.
- **Line 20:** Enable actual enforcement. `monitor` would only log would-be denials; `fingerprint` only observes.
- **Line 21:** Do not hash payloads. Metadata and length are still emitted, but payload content is not sent.

### `docker/mosquitto/acl.conf`

**Simple idea:** This file contains the normal topic permissions. It decides
which topic area each device may use. TrustMQTT adds a second, changing
security decision on top of these fixed rules.

- **Lines 1–4:** Clarify layering: static ACLs establish credential/topic permissions; TrustMQTT adds dynamic behavioral decisions.
- **Lines 5–9:** Users `plant-a` and `plant-b` receive read/write access only to their respective topic trees.
- **Lines 11–15:** Allow any client to use a client-ID-specific quarantine topic. `%c` is replaced with that client's ID.
- **Lines 17–21:** Allow each device to access only `fleet/<its-own-client-id>/#`. This permits anonymous simulator clients while preventing one device from directly using another device's namespace.

Say this distinction clearly:

> The ACL answers “is this identity normally allowed on this topic?” The TrustMQTT verdict answers “is this identity's current behavior trustworthy enough to exercise that permission?” A plugin `DEFER` means continue to the static ACL; it does not mean unconditional allow.

### `plugin/CMakeLists.txt`

**Simple idea:** This is the plugin's build recipe. It lists the source files
and outside libraries needed to create `trustmqtt_plugin.so`.

- **Lines 1–5:** Require CMake 3.10 and compile the project as required C11.
- **Lines 7–12:** Locate hiredis, cJSON, OpenSSL Crypto, and POSIX threads; add their headers and library paths.
- **Lines 14–28:** Locate Mosquitto broker headers. The Docker build supplies them explicitly. If absent, the code can compile in a degraded syntax-check mode with no live event capture.
- **Lines 30–38:** Build one shared library from the adapter, core, config parser, ring, emitter, verdict cache, and enforcement modules.
- **Lines 40–45:** Link Redis, JSON, cryptography, and threading libraries.
- **Lines 47–50:** Name the output exactly `trustmqtt_plugin.so`, with no default `lib` prefix.
- **Line 52:** Define an install destination.
- **Lines 54–85:** Optional FlashMQ target. It shares all core modules but swaps the adapter. It is not part of this Mosquitto review.

### `plugin/include/trustmqtt_plugin.h`

**Simple idea:** This header stores values used by several plugin files, such
as queue size, operating modes, verdict levels, and configuration fields.

- **Lines 1–9:** State the observation boundary. The plugin sees resolved broker semantics, not raw MQTT bytes. It cannot observe DUP, packet IDs, acknowledgment reason codes, ping timing, or topic-alias use through these hooks.
- **Lines 11–12 and 53:** Header guard: prevents duplicate declarations when included by multiple files.
- **Lines 14–15:** Import fixed-size and integer types.
- **Line 17:** Ring capacity is 8,192 JSON events.
- **Line 18:** Client IDs are stored in fixed 128-byte arrays.
- **Line 19:** Emit plugin health statistics every ten seconds.
- **Lines 21–25:** Define three modes: enforce, monitor, and fingerprint.
- **Lines 27–33:** Define increasing verdict severity: allow, watch, throttle, quarantine, and kick.
- **Lines 35–46:** Define configuration fields: broker implementation, deployment ID, Redis endpoint, timing settings, mode, and optional payload hashing.
- **Lines 48–51:** Declare `tmq_now()`, which returns fractional Unix time.

### `plugin/include/trustmqtt_core.h`

**Simple idea:** This header is the agreement between a broker adapter and the
shared core. It says what information an adapter must provide and which core
functions it can call.

- **Lines 1–8:** Describe the stable broker-neutral boundary. No Mosquitto SDK type appears in this API.
- **Lines 10–15 and 99–101:** Imports, header guard, and C linkage for compatibility with the optional C++ adapter.
- **Line 17:** Sets core ABI version 1.
- **Line 19:** Makes `tmq_core_t` opaque. Callers hold a pointer but cannot alter internals.
- **Line 21:** Defines the callback used when the core requests that a broker disconnect a client.
- **Lines 23–31:** Normalized connection event: identity, network/protocol information, session flag, and keepalive.
- **Lines 33–43:** Normalized publish event: client, topic, payload pointer/length, QoS, retain, and selected MQTT 5 properties.
- **Lines 47–52:** Declare default configuration and per-option parsing.
- **Lines 57–60:** Create and destroy the core. Network unavailability does not prevent creation because the design reconnects in the background.
- **Line 62:** Read the active operating mode.
- **Lines 64–75:** Event entry points called by broker adapters.
- **Lines 80–83:** Ask the core for an ACL enforcement decision.
- **Lines 88–89:** Expose one maintenance pass for deterministic tests; production uses the background thread.
- **Lines 94–95:** Poll pending broker actions on a broker-owned thread, avoiding unsafe broker API calls from the maintenance thread.

### `plugin/src/config.c`

**Simple idea:** This file supplies safe default settings and checks values
read from `mosquitto.conf`. Bad values are rejected before the plugin starts.

- **Lines 1–6:** Include shared declarations and standard parsing/string headers.
- **Lines 8–19:** `copy_string` safely copies into fixed buffers, handles null input, reserves space for a terminator, and always writes `\0` at the end.
- **Lines 21–34:** `parse_positive_int` rejects null, empty, nonnumeric, negative, zero, overflowing, and partially parsed values; only a complete positive integer succeeds.
- **Lines 36–51:** `tmq_config_defaults` zeroes the whole structure and selects defaults: broker name supplied by the adapter, broker ID `default`, Redis at `redis:6379`, 100 ms emit interval, 500 ms verdict refresh, enforce mode, and payload hashing off.
- **Lines 53–92:** `tmq_config_set` validates and applies known options. Ports must also be at most 65535. Mode accepts only `enforce`, `monitor`, or `fingerprint`; payload hash accepts only `sha256` or `off`. Return `1` means applied, `0` means unknown and ignored, and `-1` means invalid.

### `plugin/src/plugin.c`: the Mosquitto adapter

**Simple idea:** This is the Mosquitto-specific file. It listens to Mosquitto
callbacks, copies the useful information into common TrustMQTT structures, and
passes those structures to the core. It does not send events to Redis itself.

- **Lines 1–6:** File contract: this module translates Mosquitto APIs only; shared behavior belongs in the core.
- **Lines 7–13:** Include the shared API and standard C types/utilities.
- **Lines 15–36:** With real Mosquitto headers, compile against its API. Without them, define minimal placeholders for a degraded local build.
- **Lines 38–45:** Adapter state contains the core pointer and, in a real build, Mosquitto's plugin identifier. `g_adapter` is the one process-wide instance.
- **Lines 47–57:** `protocol_name` converts Mosquitto protocol enum values into stable event strings.
- **Lines 59–76:** `count_user_properties` repeatedly reads MQTT 5 user-property pairs, counts them, and frees temporary strings allocated by Mosquitto.
- **Lines 78–97:** `handle_connect` ignores unused callback parameters, gets the client ID, returns harmlessly if missing, maps Mosquitto fields into `tmq_connect_event_t`, sends it to the core, and always lets connection processing continue.
- **Lines 99–107:** `handle_disconnect` captures client ID and reason, then emits a normal disconnect event.
- **Lines 109–116:** `handle_client_offline` emits a separate offline event. It has no reason code, so `has_reason` is false.
- **Lines 118–151:** `handle_message_in` reads the client ID and selected MQTT 5 properties, constructs a normalized publish event, calls the core synchronously, frees the temporary content-type string, and returns success. It passes a payload pointer only for optional local hashing; the JSON path does not transmit payload content.
- **Lines 153–165:** `handle_subscribe` extracts filter, requested QoS from the low two option bits, and current subscription count.
- **Lines 167–179:** `handle_unsubscribe` performs the same translation for unsubscribe events.
- **Lines 181–191:** `handle_basic_auth` emits an observation containing client, username, and address, then returns `MOSQ_ERR_PLUGIN_DEFER`. This is crucial: TrustMQTT observes authentication but leaves the actual authentication decision to Mosquitto's normal chain.
- **Lines 193–212:** `handle_acl_check` translates Mosquitto access flags into read/write/subscribe, asks the core, maps a denial to `MOSQ_ERR_ACL_DENIED`, and otherwise returns `MOSQ_ERR_PLUGIN_DEFER` so the static ACL still runs.
- **Lines 214–223:** `handle_tick` polls pending non-network broker actions on Mosquitto's own thread.
- **Lines 225–229:** `kick_client` maps a generic core kick request to `mosquitto_kick_client_by_clientid`.
- **Lines 231–253:** `unregister_callbacks` reverses every registration during shutdown so no callback can access destroyed state.
- **Lines 254–260:** Degraded builds provide a no-op kick function.
- **Lines 262–267:** `mosquitto_plugin_version` declares plugin API version 5.
- **Lines 269–289:** `mosquitto_plugin_init` clears global state, loads Mosquitto defaults, applies every option, rejects invalid values, creates the core, and fails cleanly if allocation/thread initialization fails.
- **Lines 291–313:** Register connect, disconnect, offline, inbound message, subscribe, unsubscribe, authentication, ACL, and periodic tick callbacks.
- **Lines 314–317:** Log effective startup mode, broker ID, and Redis endpoint, then return success.
- **Lines 319–331:** Cleanup unregisters callbacks, destroys core-owned threads and structures, clears the global adapter, and returns success.

### `plugin/src/core.c`: normalization, JSON, and orchestration

**Simple idea:** This is the main shared logic. It remembers active clients,
creates JSON events, places them in the queue, refreshes security decisions,
and starts or stops the background threads.

- **Lines 1–7:** State the core's rule: callbacks only use memory; Redis work belongs to background threads.
- **Lines 8–24:** Include internal modules plus cJSON, hiredis, OpenSSL, pthreads, time, and memory/string utilities.
- **Lines 26–29:** Set limits: 4,096 tracked clients, 2,048 registry buckets, and Redis reconnect backoff from 250 ms up to 5 seconds.
- **Lines 31–47:** Define per-client activity/keepalive state, chained hash nodes, and a mutex-protected client registry.
- **Lines 49–66:** Define all owned core state: config, ring, emitter, verdict cache, registry, a dedicated verdict Redis connection, backoff/timestamps, kick callback, maintenance thread/lock, and reusable snapshot storage.
- **Lines 68–73:** `tmq_now` converts `CLOCK_REALTIME` seconds and nanoseconds into a fractional Unix timestamp.
- **Lines 75–83:** DJB2 string hash used to distribute client IDs.
- **Lines 85–107:** Initialize and destroy the client registry, freeing every chained node under the mutex.
- **Lines 109–119:** Find a client in the correct hash bucket. The `_locked` suffix documents that the caller must already hold the mutex.
- **Lines 121–146:** On connect, find or allocate a client, cap tracking at 4,096, store a bounded client ID, set keepalive and activity time, and reset keepalive-gap emission state.
- **Lines 148–156:** Touch a known client's last-activity timestamp.
- **Lines 158–174:** Remove and free a disconnected client through a pointer-to-pointer linked-list walk.
- **Lines 176–190:** Copy current client IDs into a stable snapshot so Redis I/O can occur after releasing the registry lock.
- **Lines 192–204:** `new_event` creates the common JSON envelope: schema `v:1`, timestamp, event type, broker implementation, and broker deployment ID.
- **Lines 206–216:** `emit_event` prints compact JSON, deletes the cJSON tree, then transfers ownership of the allocated string to the ring.
- **Lines 218–238:** Optionally compute a SHA-256 digest with OpenSSL and render its 32 bytes as 64 lowercase hexadecimal characters.
- **Lines 240–243:** Return current mode, safely defaulting to monitor if core is null.
- **Lines 245–262:** Connect handling validates input, registers the client, builds the envelope, conditionally adds username/IP/protocol/session data, adds keepalive, and queues the event.
- **Lines 264–276:** Disconnect handling removes client state, chooses `disconnect` or `client_offline`, optionally adds reason, and queues it.
- **Lines 278–306:** Publish handling updates activity and emits topic, QoS, retain, and payload length. If configured, it emits only a SHA-256 hash, not the payload. It groups selected MQTT 5 metadata under `props`.
- **Lines 308–335:** Shared subscription helper emits either subscribe or unsubscribe data; the public wrappers choose the event name.
- **Lines 337–349:** Authentication observation emits identity and address metadata without making the authentication decision.
- **Lines 351–367:** Access checking bypasses enforcement in fingerprint mode, calls generic policy logic otherwise, converts a monitor-mode denial into a logged `TMQ-WOULD` plus `DEFER`, and returns real denials only in enforce mode.
- **Lines 369–390:** Scan active clients for silence longer than 1.5 times keepalive and emit rate-limited `ka_gap` events. This approximates liveness gaps from observable activity; it is not raw PINGREQ timing.
- **Lines 392–403:** Execute the adapter's kick callback and emit an enforcement event.
- **Lines 405–411:** Safely close and null the verdict-side Redis connection.
- **Lines 413–464:** Snapshot clients; connect to Redis with a one-second timeout and exponential retry; pipeline `GET tmq:verdictp:<client_id>` for all active clients; parse worker-produced packed values `level|score|expires_at|rate`; update the local verdict cache; and reconnect later if the pipeline breaks.
- **Lines 466–493:** One maintenance pass scans keepalive gaps, decays expired verdicts, refreshes verdicts at the configured interval except in fingerprint mode, and emits plugin statistics every ten seconds. A try-lock prevents overlapping passes.
- **Lines 495–501:** Claim and process pending kick actions.
- **Lines 503–514:** Maintenance thread sleeps between 25 and 100 ms, then runs one maintenance pass until shutdown.
- **Lines 516–557:** Constructor allocates core and snapshot memory, copies configuration/callbacks, initializes all in-memory structures, starts the emitter, starts maintenance, and carefully unwinds resources if maintenance-thread creation fails. If only the emitter fails, the core remains alive and events eventually fill/drop from the ring.
- **Lines 560–575:** Destructor stops and joins maintenance, stops emitter, closes Redis, destroys locks/caches/registry/ring, and frees both allocations in safe ownership order.

### `plugin/include/ring.h` and `plugin/src/ring.c`

**Simple idea:** These files implement the waiting line between the broker and
Redis. New events join at `head`; the emitter removes the oldest event from
`tail`. The queue holds 8,192 events, not 512.

Header:

- **Header lines 1–3:** Fixed-size, mutex-protected queue of heap-owned JSON strings; full means drop instead of blocking.
- **Header lines 9–19:** Structure holds 8,192 pointers, next-write index `head`, next-read index `tail`, count, mutex, and cumulative drop counter.
- **Header lines 21–35:** Declare lifecycle, ownership-transferring push, batch pop, and statistics functions.

Implementation:

- **Lines 4–14:** Initialize indices/counters, null every slot, and initialize the mutex.
- **Lines 16–27:** Under the lock, free any undrained strings, then destroy the mutex.
- **Lines 29–43:** `ring_push` locks; if full, increments `dropped`, unlocks, frees the incoming JSON, and reports failure. Otherwise it stores at `head`, wraps with modulo, increments count, unlocks, and reports success.
- **Lines 45–58:** `ring_pop_batch` removes up to the requested number from `tail`, nulls vacated slots, wraps the index, decrements count, and transfers each pointer to the caller.
- **Lines 60–67:** Read the drop count under the mutex. The const cast exists only because pthread's lock API requires a mutable mutex pointer.
- **Lines 69–75:** Return current queue depth under the mutex.

Suggested sentence:

> Strictly speaking, the callback can wait briefly for this mutex, so “non-blocking” here means no network or unbounded wait in the callback path; queue work is constant-time and bounded.

Use this simpler explanation of why the queue is circular:

> The queue uses one fixed array instead of creating and moving a new list for
> every event. `head` shows where the next event will be added. `tail` shows
> where the oldest event will be removed. When either position reaches slot
> 8,191, the next position wraps back to slot zero. Empty slots at the start of
> the array can therefore be reused. Adding and removing an event takes the
> same small amount of work even when the queue contains thousands of events.

```text
Add here                              Remove here
   head                                   tail
     v                                      v
[empty][newer event][ ... ][oldest event][event]

After reaching the last slot, an index goes back to the first slot.
```

### `plugin/include/emitter.h` and `plugin/src/emitter.c`

**Simple idea:** The emitter is the delivery worker. It repeatedly takes up to
512 waiting events and sends them to Redis. A batch of 512 is used for
efficiency; the ring itself can still hold 8,192 events.

Header:

- **Lines 1–4:** This is the sole blocking Redis event-output path; broker callbacks never write to the network.
- **Lines 9–14:** Keep emitter internals opaque and expose only start/stop.

Implementation:

- **Lines 1–6:** Import emitter API, hiredis, threads, memory/string, and sleep support.
- **Lines 8–10:** Drain at most 512 events per batch and back off from 250 ms to 5 seconds after connection failures.
- **Lines 12–19:** Emitter state stores the ring reference, Redis endpoint, batch timing, thread handle, and running flag.
- **Lines 21–32:** `try_connect` opens a hiredis connection with a one-second timeout and cleans up failed contexts.
- **Lines 34–40:** The thread owns one Redis connection, its backoff value, and a stack array of 512 JSON pointers.
- **Lines 41–45:** While running, sleep for the configured batch interval and repeatedly drain available batches.
- **Lines 46–60:** Connect lazily. If Redis is unavailable, free the already-drained events, sleep with exponential backoff, and continue later. This is where connection-loss telemetry is intentionally dropped.
- **Lines 61–65:** Reset backoff after connection and append one pipelined command per event: `XADD tmq:events MAXLEN ~ 1000000 * v <json>`.
- **Lines 66–79:** Read one reply per appended command, free replies and JSON strings, and discard the context if any reply indicates a broken connection.
- **Lines 81–87:** On thread exit, close any surviving Redis connection.
- **Lines 89–108:** `emitter_start` allocates state, copies endpoint/settings, defaults invalid batch timing to 100 ms, marks it running, starts the thread, and gives ownership back through `out`.
- **Lines 110–118:** `emitter_stop` handles null safely, clears the flag, joins the thread, and frees state.

This is the exact handoff statement:

> At emitter line 64, ownership crosses into Redis. The destination is the stream `tmq:events`; Redis generates the entry ID because the command uses `*`; the stream is approximately trimmed to one million entries using `MAXLEN ~ 1000000`; and the field name is `v`, whose value is the complete compact JSON event. My teammate starts with consuming this stream.

### `plugin/include/verdict_cache.h`, `plugin/src/verdict_cache.c`, and `plugin/src/enforce.c`

**Simple idea:** These files handle decisions coming back from the worker
through Redis. The cache remembers each device's latest decision so the ACL
callback can check it quickly without contacting Redis for every message.

This is the return path from Redis. Cover it briefly because it explains why the code is an IDS/response plugin rather than only a logger.

Verdict cache:

- **Header lines 1–8:** Map `client_id` to a worker verdict and protect it with a reader/writer lock.
- **Header lines 14–27:** Each entry stores identity, severity, anomaly score, expiration, throttle rate/token state, decay time, and linked-list pointer.
- **Header lines 29–32:** Use 4,096 hash buckets and one read/write lock.
- **Implementation lines 5–18:** DJB2 hashes client IDs to buckets.
- **Lines 20–30:** Find an entry while caller holds the lock.
- **Lines 32–52:** Initialize buckets/lock and later free all chains safely.
- **Lines 54–64:** Copy a verdict under a read lock; missing means fail-open allow/defer.
- **Lines 66–91:** Insert or refresh a verdict. New throttle clients begin with a two-second burst capacity, `2 * rate`.
- **Lines 93–119:** Refill tokens according to elapsed time, cap at `2 * rate`, consume one for a write, and deny when empty.
- **Lines 121–147:** After expiry, lower severity one level per minute; remove the entry once it reaches allow.
- **Lines 149–185:** Collect kick IDs under lock, demote them to quarantine to prevent repeated kicks, release the lock, then invoke broker callbacks to avoid deadlock/re-entry.

Enforcement:

- **Lines 4–10:** Define quarantine filter and a bounded topic-segment representation.
- **Lines 12–27:** Split filters/topics on `/` without allocating new strings.
- **Lines 29–55:** Match MQTT topic filters with `+` as one segment and `#` as all remaining segments.
- **Lines 57–63:** Missing, allow, or watch verdicts defer to normal ACLs.
- **Lines 65–70:** Throttle affects writes only and uses one token per publish.
- **Lines 72–77:** Quarantine and kick deny all access except writes into `tmq/quarantine/#`; kick is treated as quarantine until the broker tick disconnects the client.

## 5. Event examples to show

Connect:

```json
{"v":1,"ts":1788460000.125,"event":"connect","broker":"mosquitto","broker_id":"default","client_id":"sensor-01","username":"plant-a","ip":"172.20.0.5","protocol":"mqtt","clean_session":true,"keepalive":60}
```

Publish with payload hashing off:

```json
{"v":1,"ts":1788460001.250,"event":"publish","broker":"mosquitto","broker_id":"default","client_id":"sensor-01","topic":"fleet/sensor-01/temperature","qos":1,"retain":false,"payload_len":24}
```

Actual Redis shape:

```text
XADD tmq:events MAXLEN ~ 1000000 * v '{...JSON above...}'
```

Redis stores something conceptually like:

```text
1788460001250-0
  v => {"v":1,"ts":...,"event":"publish",...}
```

## 6. Test files: what each proves

- `plugin/tests/test_core_config.c`: verifies defaults, accepted mode/hash options, and rejection of malformed ports or invalid values.
- `plugin/tests/test_ring.c`: verifies FIFO ordering, batch pop, capacity behavior, ownership/drop counting, and queue size.
- `plugin/tests/test_token_bucket.c`: verifies throttle burst capacity, depletion, time-based refill, and fail-open behavior for unknown clients.
- `plugin/tests/test_enforce.c`: verifies MQTT topic matching and allow/watch/throttle/quarantine/kick decisions.
- `plugin/tests/test_verdict_cache.c`: verifies insert/update, expiry decay, eviction at allow, and one-time kick processing.
- `plugin/tests/Makefile`: compiles core units with strict warnings and runs the standalone C test executables without needing a live broker.

If asked why `plugin.c` and `emitter.c` are not fully unit-tested here, say:

> These tests concentrate on deterministic broker-neutral logic. The Mosquitto adapter and live Redis transport require integration tests because they depend on broker callbacks, threads, and network behavior.

## 7. Design decisions and trade-offs reviewers may challenge

### Why C?

> Mosquitto provides a C interface for plugins, so the adapter is written in C.
> It runs inside the broker and can receive events immediately. This is fast,
> but a serious C bug could also crash Mosquitto. For that reason, the
> Mosquitto-specific part is kept small and the reusable logic is tested
> separately.

### Why a ring buffer?

> Redis may be slow or unavailable. If a Mosquitto callback waited for Redis,
> one slow Redis operation could delay MQTT clients. The ring lets the callback
> save the event quickly and return. It is fixed at 8,192 events so memory use
> cannot grow forever.

### Why drop instead of blocking?

> Security-event collection should not bring down the MQTT service. When the
> ring is full, each new event is dropped and counted. When Redis cannot be
> reached after a batch was removed, that batch is also dropped. MQTT traffic
> continues, but the security system temporarily has less information.

### Why a Redis Stream?

> A Redis Stream stores events in order and gives every event an ID. The Python
> worker can read the stream as a consumer. The command also keeps the stream
> near a limit of one million entries so Redis memory does not grow forever.

### Why put the whole JSON in field `v`?

> Every stream entry has one field named `v`. That field contains the whole JSON
> event. This makes reading simple. Inside the JSON, another `v` stores the event
> format version, currently version 1.

### Why no raw payload?

> By default, the plugin does not store the message body. It stores safer and
> smaller details such as topic, length, QoS, and retain flag. SHA-256 hashing
> can be enabled when the system needs to recognize matching payloads without
> saving their readable contents.

### Why two Redis connections?

> One Redis connection only sends events. Another connection only reads device
> verdicts. Separate connections allow the two background jobs to work without
> trying to use the same hiredis connection at the same time.

### What does fail-open mean here?

> If there is no usable verdict, the plugin does not automatically block the
> device. Old cached verdicts slowly move back toward allow after they expire.
> The project chooses to keep MQTT working when the supporting security system
> has a problem.

## 8. Honest limitations to state

- Anonymous access is enabled in the supplied development configuration and must be disabled for production.
- Redis traffic is unencrypted and unauthenticated inside the Docker bridge network.
- A full ring drops events; a Redis outage also drops batches already removed from the ring.
- Ring-full drops are counted, but connection-loss drops are not currently represented by `ring_dropped_count`.
- `volatile int` flags are used between threads; a stricter portable C implementation would use C11 atomics or mutex-protected state.
- Payload hashing can add CPU cost on the broker callback path because hashing occurs before enqueueing.
- Client tracking is capped at 4,096, and long IDs are truncated to fixed storage.
- Keepalive-gap detection is inferred from observed semantic activity, not MQTT PING packet timing.
- Callback registration return values are not checked individually, so partial registration failure is not surfaced during initialization.
- The Redis event stream is bounded approximately, but Redis has no persistent Docker volume in this Compose file.

## 9. Likely questions and exact answers

**Q: Does the plugin decide whether authentication succeeds?**

> No. The basic-auth callback records an observation and returns `MOSQ_ERR_PLUGIN_DEFER`, so Mosquitto's normal authentication chain decides. Dynamic enforcement happens in the ACL callback using cached behavioral verdicts.

**Q: Does `DEFER` mean allow?**

> No. It means TrustMQTT has no additional denial and Mosquitto should continue evaluating its normal ACL plugins and ACL file.

**Q: Can Redis downtime stop MQTT publishing?**

> By design, no. Event transport reconnects in the background with exponential backoff and drops affected telemetry rather than blocking the broker. Cached enforcement state also decays toward allow.

**Q: Is every MQTT packet inspected?**

> No. The code handles semantic broker events made available by Mosquitto callbacks. It does not parse network frames and cannot see several raw protocol details listed in `trustmqtt_plugin.h`.

**Q: Where exactly does the Python worker take over?**

> Immediately after the emitter executes `XADD` into `tmq:events`. Consumer-group reads, validation, feature engineering, scoring, verdict generation, and database storage are the worker section.

**Q: How is ordering handled?**

> Within this plugin instance, events enter the FIFO ring and are appended in batch order. Redis assigns monotonically ordered stream IDs. With multiple broker instances, Redis provides stream insertion order, not a global causal order across independent clocks.

**Q: What happens during shutdown?**

> Mosquitto unregisters callbacks first. The core then stops and joins the maintenance thread, stops and joins the emitter, closes Redis, frees verdict and client entries, drains remaining ring allocations, and frees the core.

**Q: Is the ring lock-free?**

> No. It is a simple mutex-protected circular buffer. Each push/pop operation is bounded and does no network I/O. Calling it non-blocking refers to the architecture's avoidance of Redis waits in broker callbacks, not to a lock-free algorithm.

## 10. Closing handoff

> To summarize my section: Mosquitto loads the TrustMQTT C plugin and exposes already-parsed client lifecycle, publish, subscription, authentication, ACL, and tick events. The adapter converts Mosquitto data into a broker-neutral API. The core adds metadata and builds compact JSON without including raw payload by default. The JSON is queued in a bounded in-memory ring so broker callbacks avoid network I/O. A dedicated emitter thread batches and pipelines `XADD` commands to the Redis Stream `tmq:events`. That Redis stream is the end of my section and the input to the Python worker, which my teammate will explain next.
