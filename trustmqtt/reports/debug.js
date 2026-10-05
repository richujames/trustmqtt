const { chromium } = require('playwright');
const GRAFANA = process.env.GRAFANA_URL || 'http://host.docker.internal:3000';
(async () => {
  const browser = await chromium.launch({ args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1600, height: 900 } });
  const page = await ctx.newPage();
  page.on('console', m => console.log(`CONSOLE[${m.type()}]`, m.text().slice(0, 300)));
  page.on('pageerror', e => console.log('PAGEERROR:', String(e).slice(0, 400)));
  page.on('requestfailed', r => console.log('REQFAILED:', r.url().slice(0, 160), r.failure() && r.failure().errorText));
  page.on('response', r => { if (r.status() >= 400) console.log('HTTP', r.status(), r.url().slice(0, 160)); });

  await ctx.request.post(`${GRAFANA}/login`, { data: { user: 'admin', password: 'admin' }, headers: { 'Content-Type': 'application/json' } });
  await page.goto(`${GRAFANA}/d/tmq-fleet-overview/x?kiosk&theme=dark`, { waitUntil: 'load', timeout: 60000 });
  await page.waitForTimeout(8000);
  console.log('--- TITLE:', await page.title());
  console.log('--- body snippet:', (await page.evaluate(() => document.body.innerText)).slice(0, 300).replace(/\n/g, ' | '));
  await browser.close();
})();
