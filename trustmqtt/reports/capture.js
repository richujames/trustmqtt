// Capture full-resolution screenshots of every provisioned Grafana dashboard.
// Runs inside the mcr.microsoft.com/playwright container.
//
// Two non-obvious things this handles:
//  1. Chromium must be given an explicit locale, or Grafana's frontend throws
//     "RangeError: Incorrect locale information provided" during bootstrap and
//     renders its "failed to load application files" fallback instead of the
//     dashboard.
//  2. Grafana lazy-renders panels as they scroll into view and lays them out in
//     an absolutely-positioned grid. Forcing the scroll container open with CSS
//     corrupts that layout (panels overlap), so instead we scroll to trigger
//     rendering, then grow the *viewport* to the full content height and take
//     an ordinary viewport screenshot.
const { chromium } = require('playwright');

const GRAFANA = process.env.GRAFANA_URL || 'http://host.docker.internal:3000';
const USER = process.env.GF_USER || 'admin';
const PASS = process.env.GF_PASS || 'admin';
const OUT = process.env.OUT_DIR || '/out';
const LABEL = process.env.LABEL || 'before';
const RANGE = process.env.RANGE || 'from=now-24h&to=now';
const ONLY = process.env.ONLY_UID || '';
const WIDTH = parseInt(process.env.WIDTH || '1920', 10);
const MAX_H = parseInt(process.env.MAX_HEIGHT || '7000', 10);

(async () => {
  const browser = await chromium.launch({
    args: ['--no-sandbox', '--lang=en-US', '--enable-unsafe-swiftshader', '--hide-scrollbars'],
  });
  const ctx = await browser.newContext({
    viewport: { width: WIDTH, height: 1080 },
    deviceScaleFactor: 2,
    colorScheme: 'dark',
    locale: 'en-US',
    timezoneId: 'UTC',
  });
  const page = await ctx.newPage();

  const login = await ctx.request.post(`${GRAFANA}/login`, {
    data: { user: USER, password: PASS },
    headers: { 'Content-Type': 'application/json' },
  });
  if (!login.ok()) throw new Error(`login failed: ${login.status()}`);
  console.log('logged in');

  const res = await ctx.request.get(`${GRAFANA}/api/search?type=dash-db`);
  let dashboards = await res.json();
  if (ONLY) dashboards = dashboards.filter(d => ONLY.split(',').includes(d.uid));
  console.log(`capturing ${dashboards.length} dashboard(s)`);

  for (const d of dashboards) {
    const url = `${GRAFANA}${d.url}?${RANGE}&kiosk&theme=dark`;
    console.log(`- ${d.title}`);
    await page.setViewportSize({ width: WIDTH, height: 1080 });
    await page.goto(url, { waitUntil: 'networkidle', timeout: 90000 });
    await page.waitForTimeout(5000);

    // Walk the page so every lazy-mounted panel renders and runs its query.
    let prev = -1;
    for (let i = 0; i < 60; i++) {
      const pos = await page.evaluate(() => {
        const el = document.querySelector('.scrollbar-view') || document.scrollingElement;
        el.scrollTop += 600;
        return el.scrollTop;
      });
      await page.waitForTimeout(500);
      if (pos === prev) break;
      prev = pos;
    }

    // Measure full content height, then grow the viewport to fit it exactly.
    const contentH = await page.evaluate(() => {
      const el = document.querySelector('.scrollbar-view') || document.scrollingElement;
      const inner = el.firstElementChild;
      return Math.ceil(Math.max(el.scrollHeight, inner ? inner.scrollHeight : 0));
    });
    const height = Math.min(contentH + 60, MAX_H);
    console.log(`  content ${contentH}px -> viewport ${WIDTH}x${height}`);

    await page.evaluate(() => {
      const el = document.querySelector('.scrollbar-view') || document.scrollingElement;
      el.scrollTop = 0;
    });
    await page.setViewportSize({ width: WIDTH, height });
    // Let the grid re-layout and every panel repaint at the new size.
    await page.waitForTimeout(6000);

    await page.screenshot({ path: `${OUT}/${LABEL}__${d.uid}.png` });
    console.log(`  saved ${LABEL}__${d.uid}.png`);
  }

  await browser.close();
  console.log('done');
})().catch(e => { console.error('CAPTURE FAILED:', e); process.exit(1); });
