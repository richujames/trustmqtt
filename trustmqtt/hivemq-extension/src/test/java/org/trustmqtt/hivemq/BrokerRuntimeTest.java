package org.trustmqtt.hivemq;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import redis.clients.jedis.Jedis;

class BrokerRuntimeTest {
    BrokerRuntime runtime(String mode) { return new BrokerRuntime(new Configuration(Map.of("TMQ_MODE", mode))); }
    BrokerRuntime.Client client(BrokerRuntime r, int level, double rate) {
        BrokerRuntime.Client c = new BrokerRuntime.Client();
        c.verdict = Verdict.parse(level + "|0.9|" + (BrokerRuntime.now()+120) + "|" + rate, BrokerRuntime.monotonic());
        r.clients.put("device", c); return c;
    }
    @Test void throttleConsumesTokensAndDoesNotBlockReads() {
        Verdict v = Verdict.parse("2|0.6|1000|1", 0);
        assertFalse(v.denyWrite("fleet/device/a", 1, 0));
        assertFalse(v.denyWrite("fleet/device/a", 1, 0));
        assertTrue(v.denyWrite("fleet/device/a", 1, 0));
        assertFalse(v.denyWrite("fleet/device/a", 1, 1));
        BrokerRuntime r = runtime("enforce"); client(r, 2, 0);
        assertFalse(r.deny("device", "fleet/device/a", false));
        assertTrue(r.deny("device", "fleet/device/a", true));
    }
    @Test void quarantineBlocksExistingReceivePathAndRespectsTopicBoundary() {
        BrokerRuntime r = runtime("enforce"); client(r, 3, 10);
        assertTrue(r.deny("device", "fleet/device/a", false));
        assertTrue(r.deny("device", "tmq/quarantine-escape/a", true));
        assertFalse(r.deny("device", "tmq/quarantine/device/a", true));
        assertTrue(r.deny("device", "tmq/quarantine/device/a", false));
    }
    @Test void monitorAndFingerprintNeverDenyOrKick() {
        try (var mocked = mockConstruction(Jedis.class, (r, context) -> when(r.mget(any(String[].class))).thenReturn(List.of()))) {
            for (String mode : List.of("monitor", "fingerprint")) {
                BrokerRuntime r = runtime(mode); client(r, 4, 10);
                assertFalse(r.deny("device", "fleet/device/a", true));
                AtomicInteger kicks = new AtomicInteger();
                r.maintain(id -> { kicks.incrementAndGet(); return CompletableFuture.completedFuture(true); });
                assertEquals(0, kicks.get());
            }
        }
    }
    @Test void kickIsConfirmedAndDeduplicatedUntilVerdictChanges() {
        String packed = "4|0.9|" + (BrokerRuntime.now()+120) + "|1";
        try (var mocked = mockConstruction(Jedis.class, (r, context) -> when(r.mget(any(String[].class))).thenReturn(List.of(packed)))) {
            BrokerRuntime r = runtime("enforce"); client(r, 4, 1);
            AtomicInteger kicks = new AtomicInteger();
            for (int i=0; i<3; i++) r.maintain(id -> { kicks.incrementAndGet(); return CompletableFuture.completedFuture(true); });
            assertEquals(1, kicks.get());
            assertEquals(1, r.queue.stream().filter(e -> e.contains("\"enforcement\"")).count());
        }
    }
    @Test void redisFailureStillAllowsExpiryDecay() {
        try (var mocked = mockConstruction(Jedis.class, (r, context) -> when(r.mget(any(String[].class))).thenThrow(new RuntimeException("offline")))) {
            BrokerRuntime r = runtime("enforce"); var c = client(r, 3, 1); c.verdict.expires = 0;
            r.maintain(id -> CompletableFuture.completedFuture(true));
            assertEquals(2, c.verdict.level);
        }
    }
    @Test void malformedVerdictsAreRejected() {
        for (String p : List.of("5|0.1|100|1", "2|NaN|100|1", "2|0.5|Infinity|1", "2|0.5|100|-1", "garbage"))
            assertThrows(IllegalArgumentException.class, () -> Verdict.parse(p, 0));
    }
    @Test void refreshDoesNotRefillBucket() {
        Verdict v = Verdict.parse("2|0.6|1000|1", 0);
        v.denyWrite("a", 1, 0); v.denyWrite("a", 1, 0);
        v.update(Verdict.parse("2|0.7|1100|1", 0));
        assertTrue(v.denyWrite("a", 1, 0));
    }
    @Test void queueOverflowIsBoundedAndCounted() {
        BrokerRuntime r = runtime("enforce");
        for (int i=0; i<8200; i++) r.emit(r.event("publish", "device"));
        assertEquals(8192, r.queue.size()); assertEquals(8, r.dropped.get());
    }
    @Test void envelopeMatchesPythonReceiver() throws Exception {
        BrokerRuntime r = runtime("enforce"); r.emit(r.event("plugin_stats", null));
        var e = r.json.readTree(r.queue.remove());
        assertEquals(1, e.get("v").asInt()); assertEquals("hivemq", e.get("broker").asText());
        assertFalse(e.has("client_id"));
    }
}
