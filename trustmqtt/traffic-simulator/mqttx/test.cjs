const {test} = require('node:test');
const assert = require('node:assert/strict');
const {configuration, command} = require('./run.cjs');
test('finite defaults and client-scoped identities', () => {
  const o = configuration([], {});
  const args = command(o);
  assert.equal(args[args.indexOf('--limit') + 1], '1000');
  assert.equal(args[args.indexOf('--client-id') + 1], 'tmqx-%i');
  assert.throws(() => configuration(['--messages','0'], {}));
  assert.throws(() => configuration(['--prefix','bad/#'], {}));
  assert.throws(() => configuration(['--clients','4097'], {}));
});
test('payload and topic expansion are isolated per client', () => {
  process.env.TMQ_SCENARIO = 'scope-expansion'; process.env.TMQ_WARMUP_MESSAGES = '1';
  const script = require('./sensor.js');
  const first = script.generator(null, {clientId:'unit-1'});
  const second = script.generator(null, {clientId:'unit-1'});
  const other = script.generator(null, {clientId:'unit-2'});
  assert.equal(first.topic, 'fleet/unit-1/telemetry/temperature');
  assert.equal(second.topic, 'fleet/unit-1/diagnostics/configuration');
  assert.equal(JSON.parse(other.message).sequence, 1);
  assert.ok(second.message.length > first.message.length);
  delete process.env.TMQ_SCENARIO; delete process.env.TMQ_WARMUP_MESSAGES;
});
