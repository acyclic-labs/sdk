#!/usr/bin/env node
// Actual search -> fetch -> citation, followed by durable replay without redispatch.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { isAbsolute, join, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { Processes } from "./graphcoder-e2e/process.mjs";

const [binary] = process.argv.slice(2);
assert.ok(binary && isAbsolute(binary), "absolute shipping GraphCoder required");
const begin = performance.now(), processes = new Processes();
const root = mkdtempSync(join(tmpdir(), "graphcoder-web-"));
const workspace = join(root, "workspace"), settings = join(root, "settings");
for (const path of [workspace, ...["AppData/Local", "AppData/Roaming", ".codex"].map(p => join(settings, p))]) mkdirSync(path, { recursive: true });
const content = "FETCH_E2E_SOURCE λ\nexact source bytes\n", title = "CITED_E2E_SOURCE";
let generations = 0, searches = 0, fetches = 0, offline = false, source, answer, rejectFailure;
const failed = new Promise((_, reject) => { rejectFailure = reject; });
failed.catch(() => {});
const captures = [], http = [];
const sse = value => `data: ${JSON.stringify(value)}\n\n`;
const server = createServer(async (request, response) => {
  try {
    // A token-count probe is not a provider attempt or a source fetch.
    if (request.url !== "/count") assert.equal(offline, false, "durable replay must not repeat provider/search/fetch dispatch");
    http.push({ method: request.method, path: request.url });
    if (request.url === "/source") {
      assert.equal(request.method, "GET"); fetches++;
      response.writeHead(200, { "content-type": "text/plain; charset=utf-8" }); response.end(content); return;
    }
    let bytes = ""; for await (const chunk of request) bytes += chunk;
    const body = JSON.parse(bytes);
    if (request.url === "/count") { response.setHeader("content-type", "application/json"); response.end(JSON.stringify({ input_tokens: 64 })); return; }
    if (request.url === "/search") {
      assert.equal(request.method, "POST"); assert.equal(body.query, "bounded source query"); assert.equal(body.numResults, 5); searches++;
      response.setHeader("content-type", "application/json");
      response.end(JSON.stringify({ results: [{ url: source, title, highlights: ["EXACT_E2E_EXCERPT"] }] })); return;
    }
    assert.equal(request.url, "/responses"); assert.equal(request.method, "POST");
    const outputs = body.input.filter(item => item.type === "function_call_output").map(item => JSON.parse(item.output));
    const step = generations++;
    response.setHeader("content-type", "text/event-stream");
    let call;
    if (step === 0) call = { call_id: "search-source", name: "web_search", arguments: JSON.stringify({ query: "bounded source query" }) };
    else if (step === 1) {
      assert.deepEqual(outputs.at(-1)?.results, [{ snippet: "EXACT_E2E_EXCERPT", title, url: source }], "actual configured search result");
      call = { call_id: "fetch-source", name: "url_fetch", arguments: JSON.stringify({ url: outputs.at(-1).results[0].url }) };
    } else {
      assert.equal(step, 2, "one search and one fetch before completion");
      const fetched = outputs.at(-1);
      assert.equal(fetched.content, content, "exact fetched UTF-8 body");
      assert.equal(fetched.source_url, source, "original source authority"); assert.equal(fetched.final_url, source, "final source authority"); assert.equal(fetched.status, 200);
      response.write(sse({ type: "response.output_item.done", output_index: 0, item: { type: "message", role: "assistant", content: [{ type: "output_text", text: answer }] } }));
    }
    if (call) response.write(sse({ type: "response.output_item.done", output_index: 0, item: { type: "function_call", ...call } }));
    response.end(sse({ type: "response.completed", response: { status: "completed", usage: { input_tokens: 64, output_tokens: 8 } } }));
  } catch (error) { rejectFailure(error); response.writeHead(500); response.end("fixture validation failed"); }
});
await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
const base = `http://127.0.0.1:${server.address().port}`;
source = `${base}/source`; answer = `FETCH_E2E_DONE [${title}](${source})`;
writeFileSync(join(settings, ".codex/search.json"), JSON.stringify({ provider: "exa", api_keys: { exa: "fixture-unused" }, endpoint: `${base}/search` }));
const catalog = join(root, "catalog.json"), operation = randomUUID();
writeFileSync(catalog, JSON.stringify({ fixture: { adapter: "responses", authentication: "optional", base_url: base,
  models: { tiny: { capacity: { context_tokens: 32768, output_tokens: 1024 }, features: { tools: true, images: false } } }, max_event_bytes: 65536,
  token_counter: { path: "count", adapter: "responses", response_field: "input_tokens", framing_tokens: 0 } } }));
const env = { ...process.env, HOME: settings, USERPROFILE: settings, CODEX_HOME: join(settings, ".codex"),
  LOCALAPPDATA: join(settings, "AppData/Local"), APPDATA: join(settings, "AppData/Roaming"),
  XDG_CONFIG_HOME: settings, XDG_DATA_HOME: settings, XDG_CACHE_HOME: settings, XDG_STATE_HOME: settings };
for (const key of Object.keys(env)) if (/API_KEY$|AUTH_TOKEN$|^GRAPHCODER_MODEL$/.test(key)) delete env[key];
const timer = setTimeout(() => rejectFailure(new Error("web lifecycle exceeded the standard nine-second work budget")), 9000);
let passed = false;
try {
  for (const replay of [false, true]) {
    const result = await Promise.race([failed, processes.run(binary, ["--workspace", workspace, "--settings-root", settings, "--catalog", catalog,
      "--model", "fixture/tiny", "--operation", operation, "-p", "search then fetch the cited source"], { env, cwd: workspace })]);
    captures.push(result); assert.equal(result.code, 0, result.stderr + `\nstdout tail: ${result.stdout.slice(-2500)}`);
    const frames = result.stdout.trim().split("\n").map(JSON.parse);
    const files = [...frames[0][1].files, ...frames.slice(1).flatMap(change => [change.file, ...(change.files ?? [])].filter(Boolean))];
    const outcomes = files.filter(file => file.key.Session?.file.ExecutionResult === operation).map(file => JSON.parse(file.bytes.text));
    assert.equal(outcomes.length, 1); assert.equal(outcomes[0].completion, "completed"); assert.equal(outcomes[0].ok.text, answer);
    assert.equal(generations, 3); assert.equal(searches, 1); assert.equal(fetches, 1);
    if (!replay) offline = true;
  }
  passed = true; console.log("PASS web search-fetch-citation and replay without redispatch");
} finally {
  clearTimeout(timer); await processes.close(); server.closeAllConnections(); await new Promise(resolve => server.close(resolve));
  const proof = { passed, seconds: (performance.now() - begin) / 1000, generations, searches, fetches, http, captures, fixture: passed ? null : root };
  if (process.env.E2E_OUTPUT) { const output = resolve(process.env.E2E_OUTPUT); mkdirSync(output, { recursive: true }); writeFileSync(join(output, "web-lifecycle.json"), JSON.stringify(proof)); }
  if (passed) rmSync(root, { recursive: true, force: true }); else console.error(`WEB_FAILURE_FIXTURE ${root}`);
}
