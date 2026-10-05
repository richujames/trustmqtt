package org.trustmqtt.hivemq;

import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.XAddParams;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

final class BrokerRuntime implements AutoCloseable {
    static final class Client {
        Verdict verdict;
        double activity = now(), gapAt;
        int keepalive;
    }
    final Configuration config;
    final ConcurrentMap<String, Client> clients = new ConcurrentHashMap<>();
    final ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(8192);
    final AtomicLong dropped = new AtomicLong();
    final ObjectMapper json = new ObjectMapper();
    private final List<ScheduledFuture<?>> tasks = new ArrayList<>();
    private double lastStats;
    BrokerRuntime(Configuration config) { this.config = config; }
    static double now() { return System.currentTimeMillis() / 1000.0; }
    static double monotonic() { return System.nanoTime() / 1e9; }
    Map<String,Object> event(String kind, String id) {
        Map<String,Object> e = new LinkedHashMap<>();
        e.put("v", 1); e.put("ts", now()); e.put("event", kind);
        e.put("broker", "hivemq"); e.put("broker_id", config.brokerId);
        if (id != null) e.put("client_id", id);
        return e;
    }
    void emit(Map<String,Object> event) {
        try { if (!queue.offer(json.writeValueAsString(event))) dropped.incrementAndGet(); }
        catch (Exception ex) { dropped.incrementAndGet(); }
    }
    void touch(String id) {
        Client c = clients.get(id);
        if (c != null) synchronized(c) { c.activity = now(); }
    }
    boolean deny(String id, String topic, boolean write) {
        Client c = clients.get(id);
        if (c == null || config.mode.equals("fingerprint")) return false;
        boolean denied;
        synchronized(c) {
            if (c.verdict == null) return false;
            c.verdict.decay(now());
            denied = write ? c.verdict.denyWrite(topic, now(), monotonic()) : c.verdict.level >= 3;
        }
        if (denied && config.mode.equals("monitor")) {
            System.out.println("TMQ-WOULD: client=" + id + " topic=" + topic);
            return false;
        }
        return denied;
    }
    void start(ScheduledExecutorService executor, Function<String, CompletableFuture<Boolean>> kick) {
        tasks.add(executor.scheduleWithFixedDelay(this::flush, config.emitMs, config.emitMs, TimeUnit.MILLISECONDS));
        tasks.add(executor.scheduleWithFixedDelay(() -> maintain(kick), config.refreshMs, config.refreshMs, TimeUnit.MILLISECONDS));
    }
    private Jedis redis() { return new Jedis(config.host, config.port, 1000, 1000); }
    void flush() {
        List<String> batch = new ArrayList<>(512);
        queue.drainTo(batch, 512);
        if (batch.isEmpty()) return;
        try (Jedis r = redis(); var pipeline = r.pipelined()) {
            for (String e : batch) pipeline.xadd("tmq:events", XAddParams.xAddParams().maxLen(1000000).approximateTrimming(), Map.of("v", e));
            pipeline.sync();
        } catch (Exception ex) { dropped.addAndGet(batch.size()); }
    }
    void maintain(Function<String, CompletableFuture<Boolean>> kick) {
        try {
            List<Map.Entry<String,Client>> snapshot = new ArrayList<>(clients.entrySet());
            if (!config.mode.equals("fingerprint") && !snapshot.isEmpty()) {
                try (Jedis r = redis()) {
                    String[] keys = snapshot.stream().map(e -> "tmq:verdictp:" + e.getKey()).toArray(String[]::new);
                    List<String> values = r.mget(keys);
                    for (int i = 0; i < values.size(); i++) {
                        if (values.get(i) == null) continue;
                        try {
                            Verdict next = Verdict.parse(values.get(i), monotonic());
                            Client c = snapshot.get(i).getValue();
                            synchronized(c) { if (c.verdict == null) c.verdict = next; else c.verdict.update(next); }
                        } catch (IllegalArgumentException ignored) { /* retain last valid decision */ }
                    }
                } catch (Exception ignored) { /* local expiry still runs during a Redis outage */ }
            }
            for (var entry : snapshot) {
                String id = entry.getKey(); Client c = entry.getValue(); boolean claim = false;
                synchronized(c) {
                    double time = now();
                    if (c.keepalive > 0 && time - c.activity > 1.5 * c.keepalive && time - c.gapAt >= c.keepalive) {
                        c.gapAt = time; Map<String,Object> e = event("ka_gap", id);
                        e.put("gap_s", time - c.activity); e.put("keepalive", c.keepalive); emit(e);
                    }
                    if (c.verdict != null) {
                        c.verdict.decay(time);
                        if (config.mode.equals("enforce") && c.verdict.level == 4 && !c.verdict.kickClaimed) {
                            c.verdict.kickClaimed = true; claim = true;
                        }
                    }
                }
                if (claim && clients.get(id) == c) kick.apply(id).whenComplete((done, error) -> {
                    if (error == null && Boolean.TRUE.equals(done)) emit(event("enforcement", id));
                    else synchronized(c) { if (c.verdict != null) c.verdict.kickClaimed = false; }
                });
            }
            if (now() - lastStats >= 10) {
                lastStats = now(); Map<String,Object> e = event("plugin_stats", null);
                e.put("dropped_events", dropped.get()); e.put("ring_size", queue.size()); emit(e);
            }
        } catch (Exception ex) { System.err.println("TrustMQTT maintenance failed: " + ex.getClass().getSimpleName()); }
    }
    public void close() { tasks.forEach(t -> t.cancel(false)); queue.clear(); clients.clear(); }
}
