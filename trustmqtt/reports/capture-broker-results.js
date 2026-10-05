const { chromium } = require('playwright');
const path = require('node:path');
(async()=>{
  const browser=await chromium.launch({headless:true});
  const page=await browser.newPage({viewport:{width:1440,height:900},deviceScaleFactor:1});
  const source='file:///'+path.resolve(__dirname,'broker-results.html').replaceAll('\\','/');
  for(const broker of ['hivemq','flashmq']){
    await page.goto(`${source}?broker=${broker}`);
    await page.screenshot({path:path.resolve(__dirname,`trustmqtt-${broker}-results.png`),fullPage:false});
  }
  await browser.close();
})();
