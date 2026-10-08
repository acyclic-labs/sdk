// Compare the public Rust consumer with generated WASM, then serve the same
// vectors to an actual browser. This exercises data contracts, never OS dispatch.
import assert from "node:assert/strict";
import { readFileSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { dirname, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { fileURLToPath, pathToFileURL } from "node:url";

const arguments_ = process.argv.slice(2);
const nodeOnly = arguments_.at(-1) === "--node-only";
if (nodeOnly) arguments_.pop();
assert.equal(arguments_.length, 3, "provide generated JS, WASM and Rust consumer paths");
const [bridgePath, binaryPath, consumerPath] = arguments_.map(path => resolve(path));
const digest = path => createHash("sha256").update(readFileSync(path)).digest("hex");
const identity = {
  run: randomUUID(), bridgeSha256: digest(bridgePath), wasmSha256: digest(binaryPath),
  consumerSha256: digest(consumerPath), fixtureSha256: digest(fileURLToPath(import.meta.url)),
};
const provider = { namespace: "native-contract", family: "filesystem", version: "1" };
const agent = "03030303-0303-0303-0303-030303030303";
const task = "01010101-0101-0101-0101-010101010101";
const command = "02020202-0202-0202-0202-020202020202";
const generation = { kind: "generation", provider, key: Array(32).fill(8), version: "pinned" };
const mapping = (id, writable) => ({
  volume: { provider, id, class: "agent_private", owner: { kind: "agent", id: agent } },
  generation, path: `/view/${id}`, writable, authority_revision: Array(32).fill(7),
});
const base = {
  executable: "/bin/bash", argv: ["--noprofile", "--norc", "-c", "printf '%s' '$1'", "literal", "argument space"],
  cwd: "/view", environment: { TOKEN: "selected", OTHER: "value" },
  timeout_ms: 10_000, control_timeout_ms: 250, cancellation_poll_ms: 10,
  maximum_output_bytes: 8192, maximum_result_bytes: 65_536,
  view: { kind: "materialized_directories", options: {
    root: "/view", maximum_volumes: 2, work_per_volume: { authorityRecordsRead: "EXACT_U64" },
  }, volumes: [mapping("source", false), mapping("destination", true)] },
};
const json = (request, allowance = "9007199254740993") =>
  JSON.stringify(request).replace('"EXACT_U64"', allowance);
const vectors = [];
const add = (label, mutate = () => {}, options = {}) => {
  const request = structuredClone(base);
  mutate(request);
  const requestJson = json(request, options.allowance);
  const selectedTask = options.task ?? task;
  const selectedCommand = options.command ?? command;
  const input = `{"task":${JSON.stringify(selectedTask)},"command":${JSON.stringify(selectedCommand)},"request":${requestJson}}`;
  const native = spawnSync(consumerPath, [], { input, encoding: "utf8", timeout: 10_000, windowsHide: true });
  assert.ifError(native.error);
  if (options.invalid) assert.notEqual(native.status, 0, label);
  else assert.equal(native.status, 0, `${label}: ${native.stderr}`);
  const expected = options.invalid ? null : JSON.parse(native.stdout);
  if (expected) assert.equal(expected.length, 32);
  vectors.push({ label, task: selectedTask, command: selectedCommand, requestJson, expected });
};
add("exact u64 beyond JS safe integer");
add("different exact u64", () => {}, { allowance: "9007199254740992" });
add("ordered argv", r => r.argv.reverse());
add("executable", r => r.executable = "/other/bash");
add("cwd", r => r.cwd = "/view/destination");
add("selected environment", r => r.environment.TOKEN = "changed");
add("capture allowance", r => r.timeout_ms++);
add("control allowance", r => r.control_timeout_ms++);
add("poll allowance", r => r.cancellation_poll_ms++);
add("output allowance", r => r.maximum_output_bytes++);
add("result allowance", r => r.maximum_result_bytes++);
add("namespace root", r => r.view.options.root = "/other-view");
add("volume allowance", r => r.view.options.maximum_volumes++);
add("volume ordering", r => r.view.volumes.reverse());
add("generation", r => r.view.volumes[0].generation.key[0]++);
add("authority revision", r => r.view.volumes[0].authority_revision[0]++);
add("write mode", r => r.view.volumes[0].writable = true);
add("task identity", () => {}, { task: agent });
add("command identity", () => {}, { command: agent });
for (const vector of vectors.slice(1)) assert.notDeepEqual(vector.expected, vectors[0].expected, vector.label);
add("environment key order", r => r.environment = { OTHER: "value", TOKEN: "selected" });
assert.deepEqual(vectors.at(-1).expected, vectors[0].expected);
add("unknown request field", r => r.unapproved = true, { invalid: true });
add("u64 overflow", () => {}, { allowance: "18446744073709551616", invalid: true });
add("invalid task", () => {}, { task: "not-a-task", invalid: true });

const bridge = await import(pathToFileURL(bridgePath));
await bridge.default({ module_or_path: readFileSync(binaryPath) });
for (const vector of vectors) {
  if (vector.expected === null) assert.throws(() => bridge.nativeProcessApprovalDigest(vector.task, vector.command, vector.requestJson));
  else assert.deepEqual([...bridge.nativeProcessApprovalDigest(vector.task, vector.command, vector.requestJson)], vector.expected, vector.label);
}
console.log(JSON.stringify({ scope: "public native consumer/generated WASM data agreement", passed: vectors.length, identity }));
if (nodeOnly) process.exit(0);
const data = JSON.stringify(vectors).replaceAll("<", "\\u003c");
const html = `<!doctype html><meta charset="utf-8"><title>Native approval contract</title>
<h1>Native approval contract</h1><p>Pure data contract; no process dispatch.</p><pre id="result">Running</pre>
<script type="module">
const vectors = ${data};
const identity = ${JSON.stringify(identity)};
try {
  const bridge = await import('/bridge.js');
  await bridge.default({module_or_path: await fetch('/bridge.wasm').then(r => r.arrayBuffer())});
  for (const v of vectors) {
    let actual, threw = false;
    try { actual = [...bridge.nativeProcessApprovalDigest(v.task, v.command, v.requestJson)]; } catch { threw = true; }
    if (v.expected === null ? !threw : threw || JSON.stringify(actual) !== JSON.stringify(v.expected)) throw new Error(v.label);
  }
  const result = {status:'passed', checks:vectors.length, identity, userAgent:navigator.userAgent};
  document.querySelector('#result').textContent = JSON.stringify(result, null, 2);
  await fetch('/result', {method:'POST', body:JSON.stringify(result)});
} catch (error) {
  const result = {status:'failed', error:String(error), identity, userAgent:navigator.userAgent};
  document.querySelector('#result').textContent = 'FAILED: ' + error;
  await fetch('/result', {method:'POST', body:JSON.stringify(result)});
}
</script>`;
const server = createServer(async (request, response) => {
  if (request.url === "/result" && request.method === "POST") {
    let body = "";
    for await (const chunk of request) { body += chunk; assert(body.length <= 16_384); }
    const result = JSON.parse(body);
    assert.deepEqual(result.identity, identity, "browser receipt belongs to another artifact/run");
    writeFileSync(resolve(dirname(bridgePath), "browser-result.json"), JSON.stringify(result, null, 2));
    console.log(JSON.stringify({ browser: result }));
    if (result.status !== "passed" || result.checks !== vectors.length) process.exitCode = 1;
    response.end("retained");
    clearTimeout(deadline);
    server.close();
    return;
  }
  const selected = request.url === "/bridge.js" ? ["text/javascript", readFileSync(bridgePath)]
    : request.url === "/bridge.wasm" ? ["application/wasm", readFileSync(binaryPath)]
    : request.url === "/" ? ["text/html", html] : null;
  if (!selected) { response.writeHead(404); response.end(); return; }
  response.writeHead(200, { "Content-Type": selected[0], "Cache-Control": "no-store" });
  response.end(selected[1]);
});
server.listen(0, "127.0.0.1", () => console.log(`BROWSER_URL=http://127.0.0.1:${server.address().port}/`));
const deadline = setTimeout(() => {
  console.error("Actual browser receipt was not received");
  process.exitCode = 1;
  server.close();
}, 300_000);
