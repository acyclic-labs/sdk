#!/usr/bin/env node
// Qualify the installed GraphCoder, including durable replay, without credentials.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";

const [binary, scenario = "unicode", mode = "prompt"] = process.argv.slice(2);
assert.ok(binary && isAbsolute(binary), "absolute shipping binary required");
assert.ok(["unicode", "empty", "completed-only", "terminal-output", "pre-dispatch"].includes(scenario));
assert.ok(["prompt", "jsonl"].includes(mode));
const root = mkdtempSync(join(tmpdir(), "final-answer-"));
const workspace = join(root, "workspace"), settings = join(root, "settings");
mkdirSync(workspace);
for (const path of ["AppData/Local", "AppData/Roaming", "codex-home"]) {
  mkdirSync(join(settings, path), { recursive: true });
}
const failed = scenario === "pre-dispatch";
const answer = scenario === "empty" ? "" : "  final λ\nanswer 猫\n";
const sse = value => `data: ${JSON.stringify(value)}\n\n`;
let requests = 0;
const server = createServer(async (request, response) => {
  let bytes = "";
  for await (const chunk of request) bytes += chunk;
  const body = JSON.parse(bytes);
  response.setHeader("content-type", request.url === "/count" ? "application/json" : "text/event-stream");
  if (request.url === "/count") { response.end(JSON.stringify({ input_tokens: failed && JSON.stringify(body).includes("qualify final answer") ? 70000 : 64 })); return; }
  assert.equal(request.url, "/responses");
  requests++;
  const step = body.input.filter(item => item.type === "function_call_output").length;
  const message = text => ({ type: "message", role: "assistant", content: [{ type: "output_text", text }] });
  if (step < 2) {
    response.write(sse({ type: "response.output_text.delta", delta: `discarded step ${step}\n` }));
    response.write(sse({ type: "response.output_item.done", output_index: 1, item: {
      type: "function_call", call_id: `status-${step}`, name: "fs", arguments: '{"command":"status"}',
    } }));
  } else {
    if (scenario !== "completed-only") {
      response.write(sse({ type: "response.output_text.delta", delta: "discarded draft\n" }));
      response.write(sse({ type: "response.output_text.delta", delta: answer }));
    }
    // Arrival order must not override the provider's output positions.
    for (const [output_index, text] of [[1, answer], [0, "discarded older message"]]) {
      response.write(sse({ type: "response.output_item.done", output_index, item: message(text) }));
    }
  }
  const terminal = { status: "completed", usage: { input_tokens: 64, output_tokens: 8 } };
  if (step === 2 && scenario === "terminal-output") terminal.output = [message("discarded terminal draft"), message(answer)];
  response.end(sse({ type: "response.completed", response: terminal }));
});
await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
const catalog = join(root, "catalog.json");
writeFileSync(catalog, JSON.stringify({ fixture: {
  adapter: "responses", authentication: "optional", base_url: `http://127.0.0.1:${server.address().port}`,
  models: { tiny: { capacity: { context_tokens: failed ? 65536 : 8192, output_tokens: failed ? 4096 : 1024 }, features: { tools: true, images: false } } },
  max_event_bytes: 8192,
  token_counter: { path: "count", adapter: "responses", response_field: "input_tokens", framing_tokens: 0 },
} }));
const operation = randomUUID();
const args = ["--workspace", workspace, "--settings-root", settings, "--catalog", catalog,
  "--model", "fixture/tiny", "--operation", operation];
async function run(replay) {
  const env = { ...process.env, HOME: settings, USERPROFILE: settings, CODEX_HOME: join(settings, "codex-home"),
    LOCALAPPDATA: join(settings, "AppData/Local"), APPDATA: join(settings, "AppData/Roaming"),
    XDG_DATA_HOME: settings, OPENAI_API_KEY: "", GRAPHCODER_MODEL: "" };
  const command = mode === "prompt" ? [...args, "-p", "qualify final answer"] : [...args, "--input-format", "jsonl"];
  const child = spawn(binary, command, { env, cwd: workspace, windowsHide: true,
    stdio: ["pipe", "pipe", "pipe"], signal: AbortSignal.timeout(8000), killSignal: "SIGKILL" });
  let stdout = "", stderr = "";
  child.stdout.on("data", bytes => { stdout += bytes; });
  child.stderr.on("data", bytes => { stderr += bytes; });
  const request = { key: { Session: { session: operation, file: { Request: operation } } }, channel: null,
    bytes: { text: JSON.stringify({ kind: "prompt", text: "qualify final answer", source: null, mentions: [] }) } };
  child.stdin.end(mode === "jsonl" && !replay ? `${JSON.stringify(request)}\n` : "");
  const code = await new Promise((resolve, reject) => { child.on("error", reject); child.on("close", resolve); });
  assert.equal(code, failed ? 1 : 0, stderr);
  assert.ok(stdout.trim(), `missing event frames: ${stderr}`);
  const frames = stdout.trim().split("\n").map(line => JSON.parse(line));
  const files = [...frames[0][1].files, ...frames.slice(1).flatMap(change => [change.file, ...change.files].filter(Boolean))];
  const results = files.filter(file => file.key.Session?.file.ExecutionResult === operation)
    .map(file => JSON.parse(file.bytes.text));
  assert.equal(results.length, 1, "one authoritative result");
  assert.equal(results[0].completion, failed ? "failed" : "completed");
  if (failed) {
    assert.match(JSON.stringify(results[0]), /no older source fits the compaction policy/);
    const task = frames[0][0].TaskSession.task;
    assert.equal(typeof task, "string", "headless query identifies its admitted task");
    const statuses = files.filter(file => file.key.Task?.task === task && file.key.Task?.file === "Status")
      .map(file => JSON.parse(file.bytes.text));
    assert.equal(statuses.at(-1)?.state, "failed", "pre-dispatch failure has a durable terminal task status");
  } else {
    assert.equal(results[0].ok.steps, 3);
    assert.equal(results[0].ok.text, answer);
  }
}
try {
  await run(false);
  assert.equal(requests, failed ? 0 : 3);
  await run(true);
  assert.equal(requests, failed ? 0 : 3, "replay must not dispatch again");
  console.log(`PASS final-answer ${scenario} ${mode}`);
} finally {
  server.closeAllConnections();
  await new Promise(resolve => server.close(resolve));
  rmSync(root, { recursive: true, force: true });
}
