package org.trustmqtt.hivemq;

/** Local state; callers synchronize per client, never on network operations. */
final class Verdict {
    int level;
    double expires, rate, tokens, tokenTime;
    boolean kickClaimed;
    static Verdict parse(String packed, double now) {
        String[] p = packed.split("\\|", -1);
        if (p.length != 4) throw new IllegalArgumentException("Expected four verdict fields");
        Verdict v = new Verdict();
        v.level = Integer.parseInt(p[0]);
        double score = Double.parseDouble(p[1]);
        v.expires = Double.parseDouble(p[2]);
        v.rate = Double.parseDouble(p[3]);
        if (v.level < 0 || v.level > 4 || !Double.isFinite(score) || score < 0 || score > 1 ||
            !Double.isFinite(v.expires) || v.expires < 0 || !Double.isFinite(v.rate) || v.rate < 0)
            throw new IllegalArgumentException("Invalid verdict values");
        v.tokens = v.rate * 2;
        v.tokenTime = now;
        return v;
    }
    void update(Verdict next) {
        level = next.level; expires = next.expires; rate = next.rate;
        tokens = Math.min(tokens, rate * 2);
        if (level < 4) kickClaimed = false;
    }
    void decay(double now) {
        // Match the C implementation's 120-second extensions after expiry.
        if (now > expires && level > 0) { level--; expires = now + 120; }
    }
    boolean denyWrite(String topic, double wallNow, double monotonicNow) {
        decay(wallNow);
        if (level <= 1) return false;
        if (level >= 3) return !(topic.equals("tmq/quarantine") || topic.startsWith("tmq/quarantine/"));
        tokens = Math.min(rate * 2, tokens + Math.max(0, monotonicNow - tokenTime) * rate);
        tokenTime = monotonicNow;
        if (tokens < 1) return true;
        tokens--;
        return false;
    }
}
