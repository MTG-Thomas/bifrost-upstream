import { chromium } from "playwright";
import http from "node:http";
import fs from "node:fs";
import path from "node:path";
const dist = process.argv[2];
const server = http.createServer((req, res) => {
  const file = path.join(dist, req.url === "/" ? "index.html" : req.url.split("?")[0]);
  if (!fs.existsSync(file)) { res.writeHead(404); return res.end(); }
  res.writeHead(200, { "content-type": file.endsWith(".js") ? "text/javascript" : file.endsWith(".css") ? "text/css" : "text/html" });
  fs.createReadStream(file).pipe(res);
}).listen(0);
const port = server.address().port;
const browser = await chromium.launch();
const results = [];
for (const [base, appends] of [[200, 100], [2000, 100], [5000, 50]]) {
  for (const renderer of ["react", "foldkit", "react", "foldkit", "react", "foldkit"]) {
    const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
    const cdp = await page.context().newCDPSession(page);
    await cdp.send("Performance.enable");
    await page.goto(`http://127.0.0.1:${port}/`);
    const metric = async () => Object.fromEntries((await cdp.send("Performance.getMetrics")).metrics.map((m) => [m.name, m.value]));
    const before = await metric();
    const r = await page.evaluate(([renderer, base, appends]) => window.runBench(renderer, base, appends), [renderer, base, appends]);
    const after = await metric();
    r.scriptMs = +((after.ScriptDuration - before.ScriptDuration) * 1000).toFixed(0);
    r.taskMs = +((after.TaskDuration - before.TaskDuration) * 1000).toFixed(0);
    r.layoutMs = +((after.LayoutDuration - before.LayoutDuration) * 1000).toFixed(0);
    r.heapMB = +(after.JSHeapUsedSize / 1e6).toFixed(1);
    results.push(r);
    await page.close();
  }
}
await browser.close(); server.close();
console.log(JSON.stringify(results));
