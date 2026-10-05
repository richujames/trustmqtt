package org.trustmqtt.hivemq;

import java.util.Map;

final class Configuration {
    final String host, brokerId, mode;
    final int port, emitMs, refreshMs;
    final boolean demoAuth;
    Configuration(Map<String, String> env) {
        host = env.getOrDefault("REDIS_HOST", "redis");
        brokerId = env.getOrDefault("TMQ_BROKER_ID", "default");
        mode = env.getOrDefault("TMQ_MODE", "enforce");
        if (!java.util.List.of("enforce", "monitor", "fingerprint").contains(mode)) throw new IllegalArgumentException("Invalid TMQ_MODE");
        port = positive(env, "REDIS_PORT", 6379);
        if (port > 65535) throw new IllegalArgumentException("Invalid REDIS_PORT");
        emitMs = positive(env, "TMQ_EMIT_MS", 100);
        refreshMs = positive(env, "TMQ_REFRESH_MS", 500);
        demoAuth = Boolean.parseBoolean(env.getOrDefault("TMQ_DEMO_AUTH", "false"));
        if (host.isBlank() || brokerId.isBlank()) throw new IllegalArgumentException("Empty host/broker ID");
    }
    private static int positive(Map<String,String> env, String name, int fallback) {
        int value = Integer.parseInt(env.getOrDefault(name, Integer.toString(fallback)));
        if (value <= 0) throw new IllegalArgumentException(name + " must be positive");
        return value;
    }
}
