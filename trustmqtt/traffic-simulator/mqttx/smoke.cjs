// Test MQTTX against an isolated broker that permits the observer's fleet/# subscription.
// Usage: node smoke.cjs localhost 21885 (never use an unrelated production broker).
const {createRequire} = require('node:module');
const {spawn} = require('node:child_process');
const assert = require('node:assert/strict');
const mqtt = createRequire(require.resolve('mqttx-cli/package.json'))('mqtt');
const host = process.argv[2] || 'localhost';
const port = process.argv[3] || '21885';
const observer = mqtt.connect(`mqtt://${host}:${port}`, {protocolVersion:5, clientId:'tmqx-smoke-observer', reconnectPeriod:0});
const received = [];
const timeout = setTimeout(() => { console.error('Smoke test timed out'); observer.end(true); process.exit(1); }, 20000);
observer.on('error', e => { console.error(e); process.exit(1); });
observer.on('message', (topic, payload) => received.push({topic, body:JSON.parse(payload)}));
observer.on('connect', () => {
  observer.subscribe('fleet/#', {qos:1}, (err, granted) => {
    if (err || granted.some(g => g.qos > 2)) throw err || new Error('Observer subscription denied');
    const child = spawn(process.execPath, [require.resolve('./run.cjs'), '--host',host,'--port',port,
      '--clients','2','--messages','12','--interval','50','--prefix','tmqx-smoke','--scenario','scope-expansion','--warmup','2'], {stdio:'inherit'});
    child.on('exit', code => setTimeout(() => {
      try {
        assert.equal(code, 0); assert.equal(received.length, 12);
        assert.equal(new Set(received.map(e => e.body.device_id)).size, 2);
        assert.ok(received.some(e => e.topic.endsWith('/diagnostics/configuration')));
        for (const e of received) assert.ok(e.topic.startsWith(`fleet/${e.body.device_id}/`));
        console.log('PASS: 12 MQTTX messages received from two devices, including topic expansion.');
      } catch (e) { console.error(e); process.exitCode = 1; }
      clearTimeout(timeout); observer.end();
    }, 200));
  });
});
