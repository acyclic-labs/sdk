import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { join, resolve, sep } from "node:path";
import { spawn } from "node:child_process";
import { tmpdir } from "node:os";

const [packageRoot, fixtureEndpoint] = process.argv.slice(2);
assert.ok(packageRoot && fixtureEndpoint, "package root and fixture endpoint are required");
const compiled = resolve(packageRoot, "qualification.js");
assert.ok(existsSync(compiled), `Elm output is missing: ${compiled}`);

const html = `<!doctype html><html><body><div id="app"></div><script src="/qualification.js"></script><script>
const app = Elm.QualificationMain.init({ node: document.getElementById("app"), flags: {
  basePath: ${JSON.stringify(fixtureEndpoint)}, token: "qualification"
} });
</script></body></html>`;
const server = createServer((request, response) => {
  if (request.url === "/qualification.js") {
    response.writeHead(200, { "content-type": "text/javascript" });
    response.end(readFileSync(compiled));
    return;
  }
  response.writeHead(200, { "content-type": "text/html" });
  response.end(html);
});
await new Promise(resolveListen => server.listen(0, "127.0.0.1", resolveListen));

const profile = mkdtempSync(join(tmpdir(), "acyclic-elm-browser-"));
const chromePath = [
  process.env.CHROME,
  "/usr/bin/google-chrome",
  "/usr/bin/chromium",
  "/usr/bin/chromium-browser",
  "C:/Program Files/Google/Chrome/Application/chrome.exe",
].find(path => path && existsSync(path));
assert.ok(chromePath, "set CHROME to a Chrome/Chromium executable");
const chrome = spawn(chromePath, [
  "--headless=new", "--disable-gpu", "--disable-web-security", "--remote-debugging-port=0",
  `--user-data-dir=${profile}`, "--no-first-run", "--no-default-browser-check", "about:blank",
], { stdio: "ignore" });
const until = async (description, probe, duration = 120000) => {
  const end = Date.now() + duration;
  while (Date.now() < end) {
    const value = await probe();
    if (value !== undefined) return value;
    await new Promise(resolveDelay => setTimeout(resolveDelay, 100));
  }
  throw new Error(`timed out waiting for ${description}`);
};
let socket;
try {
  const endpoint = await until("Chrome DevTools", () => {
    const file = join(profile, "DevToolsActivePort");
    if (!existsSync(file)) return;
    const [port, path] = readFileSync(file, "utf8").split("\n");
    return path ? `ws://127.0.0.1:${port}${path.trim()}` : undefined;
  });
  socket = new WebSocket(endpoint);
  await new Promise((resolveOpen, reject) => {
    socket.addEventListener("open", resolveOpen, { once: true });
    socket.addEventListener("error", reject, { once: true });
  });
  let sequence = 0;
  const pending = new Map();
  socket.addEventListener("message", event => {
    const message = JSON.parse(event.data);
    if (message.id === undefined) return;
    const item = pending.get(message.id);
    pending.delete(message.id);
    if (message.error) item.reject(new Error(message.error.message));
    else item.resolve(message.result);
  });
  const send = (method, params = {}, sessionId) => new Promise((resolveSend, reject) => {
    const id = ++sequence;
    pending.set(id, { resolve: resolveSend, reject });
    socket.send(JSON.stringify({ id, method, params, sessionId }));
  });
  const { targetId } = await send("Target.createTarget", { url: "about:blank" });
  const { sessionId } = await send("Target.attachToTarget", { targetId, flatten: true });
  await send("Runtime.enable", {}, sessionId);
  await send("Page.navigate", { url: `http://127.0.0.1:${server.address().port}/` }, sessionId);
  const result = await until("Elm generated client fixture roundtrip", async () => {
    const value = await send("Runtime.evaluate", {
      expression: "({status: document.querySelector('#elm-qualification')?.dataset.result, detail: document.querySelector('#elm-qualification')?.textContent})",
      returnByValue: true,
    }, sessionId);
    return value.result.value?.status ? value.result.value : undefined;
  });
  assert.equal(result.status, "passed", result.detail);
  console.log(`Elm browser transport: ${result.detail}`);
  await send("Browser.close");
} finally {
  socket?.close();
  if (chrome.exitCode === null) chrome.kill();
  server.closeAllConnections();
  await new Promise(resolveClose => server.close(resolveClose));
  assert.ok(resolve(profile).startsWith(`${resolve(tmpdir())}${sep}`));
  rmSync(profile, { recursive: true, force: true, maxRetries: 50, retryDelay: 100 });
}
