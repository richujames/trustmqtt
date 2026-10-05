'use strict';
const {spawn} = require('node:child_process');
const path = require('node:path');

function configuration(args, env = process.env) {
  const options = {host: env.MQTT_HOST || 'localhost', port: env.MQTT_PORT || '1885',
    clients: '10', messages: '1000', interval: '1000', scenario: 'normal',
    prefix: 'tmqx', timeout: '180', qos: '1', warmup: '30'};
  for (let i = 0; i < args.length; i++) {
    const key = args[i].replace(/^--/, '');
    if (args[i] === '--dry-run') { options.dryRun = true; continue; }
    if (!(key in options) || !args[i].startsWith('--') || i + 1 === args.length) throw new Error(`Invalid option: ${args[i]}`);
    options[key] = args[++i];
  }
  for (const key of ['port', 'clients', 'messages', 'interval', 'timeout', 'warmup']) {
    if (!/^\d+$/.test(options[key]) || Number(options[key]) < 1 || !Number.isSafeInteger(Number(options[key]))) throw new Error(`${key} must be a positive integer`);
  }
  if (+options.port > 65535 || +options.clients > 4096) throw new Error('Port must be <=65535; clients must be <=4096');
  if (!['0','1','2'].includes(options.qos)) throw new Error('qos must be 0, 1 or 2');
  if (!['normal','burst','scope-expansion'].includes(options.scenario)) throw new Error('Unknown scenario');
  if (!/^[a-zA-Z0-9_-]{1,64}$/.test(options.prefix)) throw new Error('prefix must contain only letters, digits, _ and -');
  if (options.scenario === 'burst' && options.interval === '1000') options.interval = '20';
  return options;
}
function command(o) {
  return ['simulate', '--file', path.join(__dirname, 'sensor.js'),
    '--hostname', o.host, '--port', o.port, '--count', o.clients,
    '--client-id', `${o.prefix}-%i`, '--limit', o.messages,
    '--message-interval', o.interval, '--qos', o.qos, '--mqtt-version', '5.0',
    '--reconnect-period', '0', '--content-type', 'application/json'];
}
function main() {
  const o = configuration(process.argv.slice(2));
  const args = command(o);
  if (o.dryRun) { console.log(JSON.stringify({configuration: o, mqttx: args}, null, 2)); return; }
  console.log(`MQTTX: ${o.scenario}; ${o.clients} clients; ${o.messages} total messages; ${o.host}:${o.port}`);
  const child = spawn(process.execPath, [require.resolve('mqttx-cli/bin/index.js'), ...args], {
    stdio: 'inherit', env: {...process.env, TMQ_SCENARIO: o.scenario, TMQ_WARMUP_MESSAGES: o.warmup},
  });
  let timedOut = false;
  const timer = setTimeout(() => { timedOut = true; child.kill('SIGTERM'); }, +o.timeout * 1000);
  for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => child.kill(signal));
  child.on('error', error => { clearTimeout(timer); console.error(error.message); process.exitCode = 1; });
  child.on('exit', code => { clearTimeout(timer); process.exitCode = timedOut ? 124 : (code ?? 1); });
}
if (require.main === module) { try { main(); } catch (error) { console.error(error.message); process.exitCode = 1; } }
module.exports = {configuration, command};
