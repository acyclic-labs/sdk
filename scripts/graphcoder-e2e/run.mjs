#!/usr/bin/env node
// Data cases drive the shipping executable through argv, HTTP and real PTYs.
// Build graphcoder and harness-mock-model ahead of time, then run:
//   node scripts/graphcoder-e2e/run.mjs GRAPHCODER MOCK_MODEL [case-id...]
// Install scripts/graphcoder-pty's pinned @xterm/headless before timing.
// On Windows, cross-build the ConPTY transport and set E2E_CONPTY_HOST
// to its EXE. Use serial.mjs for the full matrix. E2E_SCRATCH_ROOT selects
// all temporary directories and is mandatory on Windows.
// E2E_OUTPUT selects the matrix/capture directory; roots are retained on failure.
// Unix requires python3 and a controlling PTY; no alternate GraphCoder runtime.
import assert from "node:assert/strict";
import { existsSync, mkdirSync, readFileSync } from "node:fs";
import { mkdir, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { fileURLToPath } from "node:url";
import { Processes, Spool, schedule } from "./process.mjs";
import { requests, subset, recordedRequests } from "./assertions.mjs";
import { seed, expand as expandFixture } from "./fixtures.mjs";
import { inventory, readCoverage } from "./coverage.mjs";
import { fenced } from "./orphan-check.mjs";
import { qualification } from "./qualification.mjs";
import { publishReport } from "./report.mjs";
import { environment, scratchDirectory } from "./scratch.mjs";
import { withOwnedCase } from "./suite.mjs";
import { conversation } from "./conversation.mjs";

const [binaryArg, mockArg, ...selected] = process.argv.slice(2);
assert.ok(binaryArg && mockArg, "usage: node run.mjs GRAPHCODER MOCK_MODEL [case-id...]");
const binary = resolve(binaryArg), mock = resolve(mockArg);
const directory = dirname(fileURLToPath(import.meta.url));
const scratch = scratchDirectory("graphcoder-e2e-");
const output = resolve(process.env.E2E_OUTPUT ?? join(scratch, "results"));
mkdirSync(output, { recursive: true });
const roots = [join(scratch, "primary")];
if (process.platform === "win32") {
  roots.push(join(scratch, "ntfs"));
}
for (const root of roots) mkdirSync(root, { recursive: true });
const scripts = inventory();
const coverage = readCoverage(scripts);
const identities = new Set(scripts.map(test => test.id));
for (const id of selected) assert.ok(identities.has(id), `unknown case ${id}`);
assert.ok(process.platform !== "win32" || selected.length === 1,
  "Windows requires one case suite at a time; use serial.mjs for the complete matrix");
const cases = scripts
  .filter(test => !selected.length || selected.includes(test.id))
  .flatMap(test => roots.filter((_, index) => index === 0 || test.ntfs).map((root, index) => ({ ...test, id: test.id + (index ? "-ntfs" : ""), root })));
assert.ok(cases.length, "no matching cases");
assert.equal(new Set(cases.map(test => test.id)).size, cases.length, "case identities must be unique");
const processes = new Processes({ transcripts: join(output, "transcripts") });
mkdirSync(join(output, "transcripts"), { recursive: true });
const contexts = new Map();
const evidence = new Map();
let server, events, port;
const started = performance.now();
const json = (path, value) => writeFile(path, JSON.stringify(value, null, 2) + "\n");
function expand(value, fixture) {
  return expandFixture(value, { support: directory.replaceAll("\\", "/"),
    repository: resolve(directory, "../..").replaceAll("\\", "/"),
    workspace: fixture.workspace.replaceAll("\\", "/"),
    ...(port ? { url: `http://127.0.0.1:${port}/${fixture.id}/v1` } : {}) });
}
// GraphCoder's headless outcome: `-p` prints the answer text and exits 0;
// JSONL prints one `{"id", "inbox", "text"}` (or `"error"`) line per prompt.
function outcome(input, result) {
  if (input.mode !== "jsonl") {
    return result.code === 0 ? { completion: "completed", ok: { text: result.stdout.replace(/\r?\n$/, "") } }
      : { completion: "failed", error: result.stderr.trim() };
  }
  const results = result.stdout.trim().split("\n").filter(Boolean).map(JSON.parse).filter(line => line.id !== undefined);
  assert.equal(results.length, 1, "one authoritative result");
  const [line] = results;
  return line.error === undefined ? { completion: "completed", ok: { text: line.text } } : { completion: "failed", error: line.error };
}
// A mock request is indexed by its case and step, and by its case, path and step.
const requestKey = (id, { path, step }) => JSON.stringify(path === undefined ? [id, step] : [id, path, step]);
async function checkTree(test) {
  if (test.assert.dead_tree) {
    const records = recordedRequests(readFileSync(join(output, "requests.jsonl"), "utf8"), await events.history());
    fenced(records.filter(record => record.case === test.id), test.assert.dead_tree);
  }
}
async function execute(test, gitOrigin) {
  const begin = performance.now();
  const fixture = { id: test.id, workspace: join(test.root, test.id, "workspace"), settings: join(test.root, test.id, "settings"), catalog: join(test.root, test.id, "catalog.json") };
  const artifact = join(output, test.id); await mkdir(artifact, { recursive: true });
  fixture.binary = binary;
  fixture.output = artifact;
  const captures = [], interrupted = [];
  try {
    fixture.env = environment(fixture.settings);
    await mkdir(dirname(fixture.workspace), { recursive: true });
    if (test.workspace?.kind === "git") {
      const cloned = await processes.run("git", ["clone", "--quiet", "--shared", gitOrigin, fixture.workspace], { env: fixture.env });
      assert.equal(cloned.code, 0, cloned.stderr);
    } else await mkdir(fixture.workspace, { recursive: true });
    await mkdir(fixture.settings, { recursive: true });
    await Promise.all([seed(fixture.workspace, expand(test.workspace?.files, fixture)), seed(fixture.settings, expand(test.settings, fixture))]);
    const provider = { adapter: test.adapter ?? "responses", authentication: "optional", base_url: `http://127.0.0.1:${port}/${test.id}/v1`,
      models: { tiny: { capacity: test.capacity ?? { context_tokens: 32768, output_tokens: 1024 }, features: { tools: true, images: true } } }, max_event_bytes: 65536,
      token_counter: { path: "count", adapter: test.adapter ?? "responses", response_field: "input_tokens", framing_tokens: 0 } };
    await json(fixture.catalog, { e2e: provider });
    fixture.env.E2E_MOCK_URL = `http://127.0.0.1:${port}/${test.id}/v1`;
    fixture.env.ACYCLIC_TEST_SOURCE_ROOT = resolve(directory, "../..");
    fixture.requested = request => events.has(requestKey(test.id, request));
    fixture.onRequest = listener => events.subscribe(event => {
      if (event.case === test.id) listener(event);
    });
    fixture.waitRequest = request => events.keyed(requestKey(test.id, request));
    fixture.args = ["--workspace", fixture.workspace, "--settings-root", fixture.settings, "--catalog", fixture.catalog, "--model", "e2e/tiny"];
    const operation = "00000000-0000-0000-0000-000000000001";
    for (const raw of test.inputs) {
      const input = expand(raw, fixture);
      if (input.mode === "conversation") {
        await conversation(processes, binary, fixture, { ...input, operation: input.operation ?? operation },
          capture => captures.push(capture));
        continue;
      }
      if (input.mode === "qualification") {
        await qualification(processes, input, fixture, capture => captures.push(capture));
        continue;
      }
      if (input.mode === "pty") {
        const { pty } = await import("./pty.mjs");
        await pty(processes, binary, fixture, input, capture => captures.push(capture));
        continue;
      }
      const op = input.operation ?? operation;
      const args = input.args ?? (input.mode === "jsonl" ? ["--operation", op, "--input-format", "jsonl"] : ["--operation", op, "-p", input.text ?? "hello"]);
      const running = processes.start(binary, [...fixture.args, ...args, ...(input.extra_args ?? [])], { cwd: input.cwd ?? fixture.workspace, env: fixture.env, scopeReportSignal: "SIGTERM" });
      if (input.kill_on) {
        await Promise.race([fixture.waitRequest(input.kill_on), running.closed.then(result => { throw new Error(`exited before kill point: ${JSON.stringify(result)}`); })]);
        running.child.kill("SIGKILL");
        const result = await running.exited;
        assert.ok(result.signal || result.code !== 0);
        interrupted.push(running.closed.then(capture => captures.push(capture)));
        continue;
      } else if (input.mode === "jsonl") {
        // Message `<operation>:e2e`: rerunning the operation replays its answer.
        running.child.stdin.end(input.stdin ?? JSON.stringify({ prompt: input.text ?? "hello", id: "e2e" }) + "\n");
      } else running.child.stdin.end();
      const result = await running.closed; captures.push(result);
      assert.equal(result.code, input.exit ?? 0, result.stderr + `\nstdout tail: ${result.stdout.slice(-1500)}`);
      if (input.stdout_contains) for (const text of input.stdout_contains) assert.ok(result.stdout.includes(text), `missing stdout ${text}`);
      if (input.stderr_contains) for (const text of input.stderr_contains) assert.ok(result.stderr.includes(text), `missing stderr ${text}`);
      if (((input.exit ?? 0) === 0 && !input.args) || input.outcome !== undefined) {
        const settled = outcome(input, result);
        if (input.outcome !== undefined) {
          assert.equal(settled.completion, input.outcome.completion, `terminal outcome mismatch: ${JSON.stringify(settled)}`);
          if (input.outcome.error !== undefined) assert.ok(settled.error.includes(input.outcome.error), `terminal error mismatch: ${JSON.stringify(settled)}`);
        } else assert.equal(settled.completion, "completed", JSON.stringify(settled));
        if (input.answer !== undefined) assert.equal(settled.ok.text, input.answer);
      }
    }
    // Check while the mock's held response is still open. Killing the mock
    // first would let an unfenced descendant exit and produce a false pass.
    await checkTree(test);
    contexts.set(test.id, { test, fixture, captures, artifact });
    return { case: test.id, platform: process.platform, status: "pass", seconds: (performance.now() - begin) / 1000 };
  } catch (error) {
    try { await checkTree(test); } catch (treeError) { error = new Error(`${error.stack}\n${treeError.stack}`); }
    return { case: test.id, platform: process.platform, status: "fail", seconds: (performance.now() - begin) / 1000, error: error.stack, workspace: fixture.workspace };
  } finally {
    await Promise.all(interrupted);
    evidence.set(test.id, { captures });
  }
}
async function verify(result, allRecords) {
  const records = allRecords.filter(record => record.case === result.case);
  if (result.status === "fail") return result;
  const { test, fixture, captures } = contexts.get(result.case);
  try {
    requests(test, records);
    const client = JSON.stringify(captures);
    for (const text of test.assert.wire_contains ?? []) assert.ok(client.includes(text), `client wire missing ${text}`);
    for (const [path, expected] of Object.entries(test.assert.files ?? {})) assert.equal(readFileSync(join(fixture.workspace, path), "utf8"), expected, path);
    for (const path of test.assert.absent_files ?? []) assert.ok(!existsSync(join(fixture.workspace, path)), `unexpected file ${path}`);
    for (const [path, expected] of Object.entries(test.assert.jsonl ?? {})) {
      const rows = readFileSync(join(fixture.workspace, path), "utf8").trim().split("\n").map(JSON.parse);
      for (const expectedRow of expected) assert.ok(rows.some(row => subset(row, expectedRow)), `missing JSONL ${JSON.stringify(expectedRow)} in ${path}`);
    }
    return result;
  } catch (error) {
    return { ...result, status: "fail", error: error.stack, workspace: fixture.workspace };
  }
}
let results;
const execution = await withOwnedCase({ processes, run: async () => {
  const scripts = Object.fromEntries(cases.map(test => [test.id, expand(test.model,
    { id: test.id, workspace: join(test.root, test.id, "workspace") })]));
  await json(join(output, "script.json"), { cases: scripts });
  const env = environment(join(scratch, "support-profile"));
  server = processes.start(mock, ["--script", join(output, "script.json"), "--record", join(output, "requests.jsonl")], { env });
  events = new Spool(server.child.stdout, server.closed, { transcript: join(output, "events.jsonl"),
    key: event => event.event === "ready" ? ["ready"]
      : event.event === "request" ? [requestKey(event.case, { step: event.step }), requestKey(event.case, event)] : [] });
  port = (await events.keyed("ready")).port;
  const origin = join(roots[0], "origin"); await mkdir(origin);
  await seed(origin, { "seed.txt": "base\n" });
  for (const args of [["init", "--quiet", "--initial-branch=main", origin], ["-C", origin, "add", "."],
    ["-C", origin, "-c", "user.name=e2e", "-c", "user.email=e2e@example.invalid", "-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "seed"]]) {
    const git = await processes.run("git", args, { env }); assert.equal(git.code, 0, git.stderr);
  }
  // Unix cases share one mock. Windows runs every case and filesystem variant
  // serially; serial.mjs gives each case its own unchanged ten-second budget.
  results = await schedule(cases, process.platform, test => execute(test, origin));
} });
// Every executable has exited and the server has been reaped. No concurrent
// append can expose an incomplete JSONL line to the assertion phase.
const allRecords = recordedRequests(readFileSync(join(output, "requests.jsonl"), "utf8"), await events.history());
results = await Promise.all(results.map(result => verify(result, allRecords)));
results = results.map(result => ({ ...result, ...(execution.expired ? { timeout: true } : {}), evidence: { ...evidence.get(result.case),
  requests: allRecords.filter(record => record.case === result.case) } }));
const seconds = (performance.now() - started) / 1000;
const report = { platform: process.platform, seconds, limit_seconds: 10, expired: execution.expired, results, output, roots,
  selected, qualification: !selected.length, coverage };
await publishReport(join(output, "matrix.json"), report);
process.exitCode = !execution.expired && results?.every(result => result.status === "pass") && seconds < 10
  && (selected.length > 0 || coverage.pass) ? 0 : 1;
