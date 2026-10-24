#!/usr/bin/env node
// Cancel a held GET body through the shipping TUI, then explicitly resume it.
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { isAbsolute, join, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { Processes } from "./graphcoder-e2e/process.mjs";
import { pty } from "./graphcoder-e2e/pty.mjs";

const [binary] = process.argv.slice(2);
assert.ok(binary && isAbsolute(binary), "absolute shipping GraphCoder required");
const begin = performance.now(), processes = new Processes();
const root = mkdtempSync(join(tmpdir(), "graphcoder-fetch-cancel-"));
const workspace = join(root, "workspace"), settings = join(root, "settings"), catalog = join(root, "catalog.json");
for (const path of [workspace, ...["AppData/Local", "AppData/Roaming", ".codex"].map(p => join(settings, p))]) mkdirSync(path, { recursive: true });
const requests = [], captures = [], listeners = new Set();
let rejectFailure, closedSource, source, generations = 0, fetches = 0, sourceRetired = false;
const failed = new Promise((_, reject) => { rejectFailure = reject; }); failed.catch(() => {});
const retired = new Promise(resolve => { closedSource = resolve; });
const sse = value => `data: ${JSON.stringify(value)}\n\n`;
const record = event => { requests.push(event); for (const listener of listeners) listener(event); };
const server = createServer(async (request, response) => {
  try {
    if (request.url === "/source") {
      assert.equal(request.method, "GET"); assert.equal(++fetches, 1, "fetch must not be redispatched on resume");
      response.on("close", () => { sourceRetired = true; closedSource(); });
      response.writeHead(200, { "content-type": "text/plain" }); response.flushHeaders();
      record({ path: "source", step: 1 }); return;
    }
    assert.equal(request.method, "POST");
    let bytes = ""; for await (const chunk of request) bytes += chunk;
    JSON.parse(bytes);
    if (request.url === "/count") { response.setHeader("content-type", "application/json"); response.end('{"input_tokens":64}'); return; }
    assert.equal(request.url, "/responses"); assert.equal(++generations, 1, "cancelled task must not redispatch a model");
    response.setHeader("content-type", "text/event-stream");
    response.write(sse({ type: "response.output_item.done", output_index: 0, item: { type: "function_call", call_id: "held-source", name: "url_fetch", arguments: JSON.stringify({ url: source }) } }));
    response.end(sse({ type: "response.completed", response: { status: "completed", usage: { input_tokens: 64, output_tokens: 8 } } }));
  } catch (error) { rejectFailure(error); response.writeHead(500); response.end("fixture validation failed"); }
});
await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
const base = `http://127.0.0.1:${server.address().port}`; source = `${base}/source`;
writeFileSync(catalog, JSON.stringify({ e2e: { adapter: "responses", authentication: "optional", base_url: base,
  models: { tiny: { capacity: { context_tokens: 32768, output_tokens: 1024 }, features: { tools: true, images: false } } }, max_event_bytes: 65536,
  token_counter: { path: "count", adapter: "responses", response_field: "input_tokens", framing_tokens: 0 } } }));
const env = { ...process.env, HOME: settings, USERPROFILE: settings, CODEX_HOME: join(settings, ".codex"), LOCALAPPDATA: join(settings, "AppData/Local"), APPDATA: join(settings, "AppData/Roaming"),
  XDG_CONFIG_HOME: settings, XDG_DATA_HOME: settings, XDG_CACHE_HOME: settings, XDG_STATE_HOME: settings };
for (const key of Object.keys(env)) if (/API_KEY$|AUTH_TOKEN$|^GRAPHCODER_MODEL$/.test(key)) delete env[key];
const fixture = { workspace, settings, catalog, env, requests,
  args: ["--workspace", workspace, "--settings-root", settings, "--catalog", catalog, "--model", "e2e/tiny"],
  onRequest(listener) { listeners.add(listener); return () => listeners.delete(listener); } };
const prompt = "cancel this held source fetch";
const inputs = [
  { actions: [
    { when: { contains: ["GraphCoder"], cursor: true }, prompt },
    { after_request: { path: "source", step: 1 }, when: { contains: ["GraphCoder"] }, send: "\u001b" },
    { when: { contains: ["interaction cancelled", "STOPPED", "○"] }, send: "\u0004" },
  ], exit: 2, settled_contains: [prompt, "cancelled", "○"] },
  { resume: true, actions: [{ when: { contains: ["GraphCoder", prompt, "interaction cancelled", "STOPPED", "○"], cursor: true }, send: "\u0004" }], exit: 2, settled_contains: [prompt, "interaction cancelled", "STOPPED", "○"] },
];
const timer = setTimeout(() => rejectFailure(new Error("held fetch cancellation exceeded the standard nine-second work budget")), 9000);
let passed = false, activePty;
try {
  for (const input of inputs) { activePty = pty(processes, binary, fixture, input, capture => captures.push(capture)); await Promise.race([failed, activePty]); }
  await Promise.race([failed, retired]);
  assert.equal(generations, 1); assert.equal(fetches, 1); assert.equal(sourceRetired, true);
  passed = true; console.log("PASS held fetch Escape, owned HTTP retirement and explicit resume without redispatch");
} finally {
  clearTimeout(timer); await processes.close(); await activePty?.catch(() => {}); server.closeAllConnections(); await new Promise(resolve => server.close(resolve));
  const proof = { passed, seconds: (performance.now() - begin) / 1000, generations, fetches, sourceRetired, requests, captures, fixture: passed ? null : root };
  if (process.env.E2E_OUTPUT) { const output = resolve(process.env.E2E_OUTPUT); mkdirSync(output, { recursive: true }); writeFileSync(join(output, "fetch-cancellation.json"), JSON.stringify(proof)); }
  if (passed) rmSync(root, { recursive: true, force: true }); else console.error(`FETCH_CANCELLATION_FAILURE_FIXTURE ${root}`);
}
