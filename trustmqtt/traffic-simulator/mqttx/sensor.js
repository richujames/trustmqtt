'use strict';

// MQTTX invokes the generator once per publish with the resolved client ID.
// Metadata changes (topic/rate/length), rather than payload values alone, feed TrustMQTT.
const sequences = new Map();
module.exports = {
  name: 'trustmqtt-sensor',
  generator(_faker, options) {
    const id = options.clientId;
    const sequence = (sequences.get(id) || 0) + 1;
    sequences.set(id, sequence);
    const scenario = process.env.TMQ_SCENARIO || 'normal';
    const warmup = Number(process.env.TMQ_WARMUP_MESSAGES || 30);
    const expanded = scenario === 'scope-expansion' && sequence > warmup;
    return {
      topic: `fleet/${id}/${expanded ? 'diagnostics/configuration' : 'telemetry/temperature'}`,
      message: JSON.stringify({
        device_id: id, sequence, timestamp: new Date().toISOString(),
        temperature_c: Number((22 + Math.sin(sequence / 10) * 2).toFixed(2)),
        humidity_pct: 45 + sequence % 7,
        status: 'synthetic',
        ...(expanded ? {diagnostic: 'x'.repeat(512)} : {}),
      }),
    };
  },
};
