#!/usr/bin/env node
// Data cases drive the shipping executable through argv, HTTP and real PTYs.
// Build graphcoder and harness-mock-model ahead of time, then run:
//   node scripts/graphcoder-e2e/run.mjs GRAPHCODER MOCK_MODEL [case-id...]
// Install scripts/graphcoder-pty's pinned @xterm/headless before timing.
// On Windows, compile prepare-conpty.ps1 with Windows PowerShell and set
// E2E_CONPTY_ASSEMBLY to its DLL. E2E_WORKSPACE_ROOT=Q:/wt selects ReFS;
// designated cases also run under E2E_NTFS_ROOT (default: local C: temp).
// E2E_OUTPUT selects the matrix/capture directory; roots are retained on failure.
// Unix requires python3 and a controlling PTY; no alternate GraphCoder runtime.
import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { fileURLToPath } from "node:url";
import { Processes, Events } from "./process.mjs";
import { pty } from "./pty.mjs";
import { requests, subset } from "./assertions.mjs";

const [binaryArg, mockArg, ...selected] = process.argv.slice(2);
assert.ok(binaryArg && mockArg, "usage: node run.mjs GRAPHCODER MOCK_MODEL [case-id...]");
const binary = resolve(binaryArg), mock = resolve(mockArg);
const directory = dirname(fileURLToPath(import.meta.url));
const output = resolve(process.env.E2E_OUTPUT ?? mkdtempSync(join(tmpdir(), "graphcoder-e2e-results-")));
mkdirSync(output, { recursive: true });
const roots = [mkdtempSync(join(process.env.E2E_WORKSPACE_ROOT ?? tmpdir(), "graphcoder-e2e-"))];
if (process.platform === "win32") {
  const ntfs = process.env.E2E_NTFS_ROOT ?? join(process.env.LOCALAPPDATA, "Temp");
  mkdirSync(ntfs, { recursive: true });
  roots.push(mkdtempSync(join(ntfs, "graphcoder-e2e-ntfs-")));
}
const cases = readdirSync(join(directory, "cases")).filter(name => name.endsWith(".json"))
  .map(name => JSON.parse(readFileSync(join(directory, "cases", name), "utf8")))
  .filter(test => !selected.length || selected.includes(test.id))
  .flatMap(test => roots.filter((_, index) => index === 0 || test.ntfs).map((root, index) => ({ ...test, id: test.id + (index ? "-ntfs" : ""), root })));
assert.ok(cases.length, "no matching cases");
assert.equal(new Set(cases.map(test => test.id)).size, cases.length, "case identities must be unique");
const processes = new Processes();
const contexts = new Map();
let server, events, port;
const started = performance.now();
const timeout = setTimeout(() => { processes.close(); }, 10000);
const json = (path, value) => writeFileSync(path, JSON.stringify(value, null, 2) + "\n");
function seed(root, files = {}) {
  for (const [path, value] of Object.entries(files)) {
    assert.ok(!isAbsolute(path) && !path.split(/[\\/]/).includes(".."), `unsafe seed path ${path}`);
    const target = join(root, path); mkdirSync(dirname(target), { recursive: true });
    writeFileSync(target, typeof value === "string" ? value : Buffer.from(value.base64, "base64"));
  }
}
function expand(value, fixture) {
  if (typeof value === "string") return value.replaceAll("{{support}}", directory.replaceAll("\\", "/")).replaceAll("{{workspace}}", fixture.workspace.replaceAll("\\", "/")).replaceAll("{{url}}", `http://127.0.0.1:${port}/${fixture.id}/v1`);
  if (Array.isArray(value)) return value.map(item => expand(item, fixture));
  if (value && typeof value === "object") return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, expand(item, fixture)]));
  return value;
}
function environment(settings) {
  const env = { ...process.env, TERM: "xterm-256color", HOME: settings, USERPROFILE: settings,
    LOCALAPPDATA: join(settings, "AppData/Local"), APPDATA: join(settings, "AppData/Roaming"), CODEX_HOME: join(settings, ".codex"),
    XDG_CONFIG_HOME: settings, XDG_DATA_HOME: settings, XDG_CACHE_HOME: settings, XDG_STATE_HOME: settings };
  for (const key of Object.keys(env)) if (/API_KEY$|^GRAPHCODER_MODEL$|^ANTHROPIC_AUTH_TOKEN$/.test(key)) delete env[key];
  return env;
}
function files(frames) {
  return [...(frames[0]?.[1]?.files ?? []), ...frames.slice(1).flatMap(change => [change.file, ...(change.files ?? [])].filter(Boolean))];
}
function outcomes(frames, operation) {
  return files(frames).filter(file => file.key.Session?.file.ExecutionResult === operation).map(file => JSON.parse(file.bytes.text));
}
async function execute(test, gitOrigin) {
  const begin = performance.now();
  const fixture = { id: test.id, workspace: join(test.root, test.id, "workspace"), settings: join(test.root, test.id, "settings"), catalog: join(test.root, test.id, "catalog.json") };
  const artifact = join(output, test.id); mkdirSync(artifact, { recursive: true });
  const captures = [];
  try {
    mkdirSync(dirname(fixture.workspace), { recursive: true });
    if (test.workspace?.kind === "git") {
      const cloned = await processes.run("git", ["clone", "--quiet", "--shared", gitOrigin, fixture.workspace]);
      assert.equal(cloned.code, 0, cloned.stderr);
    } else mkdirSync(fixture.workspace, { recursive: true });
    mkdirSync(fixture.settings, { recursive: true });
    // Windows known-folder discovery verifies the expanded profile directories.
    for (const path of ["AppData/Local", "AppData/Roaming"]) mkdirSync(join(fixture.settings, path), { recursive: true });
    seed(fixture.workspace, expand(test.workspace?.files, fixture));
    seed(fixture.settings, expand(test.settings, fixture));
    const provider = { adapter: test.adapter ?? "responses", authentication: "optional", base_url: `http://127.0.0.1:${port}/${test.id}/v1`,
      models: { tiny: { capacity: test.capacity ?? { context_tokens: 32768, output_tokens: 1024 }, features: { tools: true, images: true } } }, max_event_bytes: 65536,
      token_counter: { path: "count", adapter: test.adapter ?? "responses", response_field: "input_tokens", framing_tokens: 0 } };
    json(fixture.catalog, { e2e: provider });
    fixture.env = environment(fixture.settings);
    fixture.requests = events.seen.filter(event => event.case === test.id);
    fixture.onRequest = listener => events.subscribe(event => {
      if (event.case === test.id) { fixture.requests.push(event); listener(event); }
    });
    fixture.args = ["--workspace", fixture.workspace, "--settings-root", fixture.settings, "--catalog", fixture.catalog, "--model", "e2e/tiny"];
    const operation = "00000000-0000-0000-0000-000000000001";
    for (const raw of test.inputs) {
      const input = expand(raw, fixture);
      if (input.mode === "pty") {
        await pty(processes, binary, fixture, input, capture => captures.push(capture));
        continue;
      }
      const op = input.operation ?? operation;
      const args = input.args ?? (input.mode === "jsonl" ? ["--operation", op, "--input-format", "jsonl"] : ["--operation", op, "-p", input.text ?? "hello"]);
      const running = processes.start(binary, [...fixture.args, ...args], { cwd: fixture.workspace, env: fixture.env });
      if (input.kill_on) {
        await Promise.race([events.wait(event => event.case === test.id && event.path === input.kill_on.path && event.step === input.kill_on.step), running.closed.then(result => { throw new Error(`exited before kill point: ${JSON.stringify(result)}`); })]);
        running.child.kill("SIGKILL");
      } else if (input.mode === "jsonl") {
        const request = { key: { Session: { session: op, file: { Request: op } } }, channel: null,
          bytes: { text: JSON.stringify(input.resolution ?? { kind: "prompt", text: input.text ?? "hello", source: null, mentions: [] }) } };
        running.child.stdin.end(input.stdin ?? (input.reopen ? "" : JSON.stringify(request) + "\n"));
      } else running.child.stdin.end();
      const result = await running.closed; captures.push(result);
      if (input.kill_on) { assert.ok(result.signal || result.code !== 0); continue; }
      assert.equal(result.code, input.exit ?? 0, result.stderr + `\nstdout tail: ${result.stdout.slice(-1500)}`);
      if (input.stdout_contains) for (const text of input.stdout_contains) assert.ok(result.stdout.includes(text), `missing stdout ${text}`);
      if (input.stderr_contains) for (const text of input.stderr_contains) assert.ok(result.stderr.includes(text), `missing stderr ${text}`);
      if ((input.exit ?? 0) === 0 && !input.args) {
        const frames = result.stdout.trim().split("\n").map(JSON.parse);
        assert.equal(frames[0][0].TaskSession.session, op);
        const outcome = outcomes(frames, op).at(-1);
        assert.equal(outcome?.completion, "completed", JSON.stringify(outcome));
        if (input.answer !== undefined) assert.equal(outcome.ok.text, input.answer);
      }
    }
    contexts.set(test.id, { test, fixture, captures, artifact });
    return { case: test.id, platform: process.platform, status: "pass", seconds: (performance.now() - begin) / 1000 };
  } catch (error) {
    return { case: test.id, platform: process.platform, status: "fail", seconds: (performance.now() - begin) / 1000, error: error.stack, workspace: fixture.workspace };
  } finally { json(join(artifact, "captures.json"), captures); }
}
function verify(result, allRecords) {
  const records = allRecords.filter(record => record.case === result.case);
  json(join(output, result.case, "requests.json"), records);
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
try {
  const scripts = Object.fromEntries(cases.map(test => [test.id, test.model]));
  json(join(output, "script.json"), { cases: scripts });
  server = processes.start(mock, ["--script", join(output, "script.json"), "--record", join(output, "requests.jsonl")]);
  events = new Events(server.child.stdout, server.closed);
  port = (await events.wait(event => event.event === "ready")).port;
  const origin = join(roots[0], "origin"); mkdirSync(origin);
  seed(origin, { "seed.txt": "base\n" });
  for (const args of [["init", "--quiet", "--initial-branch=main", origin], ["-C", origin, "add", "."],
    ["-C", origin, "-c", "user.name=e2e", "-c", "user.email=e2e@example.invalid", "-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "seed"]]) {
    const git = await processes.run("git", args); assert.equal(git.code, 0, git.stderr);
  }
  // All cases start together. Only the dependent steps inside one conversation
  // are sequential. There is one mock server for the entire platform run.
  results = await Promise.all(cases.map(test => execute(test, origin)));
} finally {
  clearTimeout(timeout);
  await processes.close();
}
// Every executable has exited and the server has been reaped. No concurrent
// append can expose an incomplete JSONL line to the assertion phase.
const allRecords = readFileSync(join(output, "requests.jsonl"), "utf8").trim().split("\n").filter(Boolean).map(JSON.parse);
results = results.map(result => verify(result, allRecords));
const seconds = (performance.now() - started) / 1000;
const report = { platform: process.platform, seconds, limit_seconds: 10, results, output, roots };
json(join(output, "matrix.json"), report);
console.log(JSON.stringify(report, null, 2));
process.exitCode = results?.every(result => result.status === "pass") && seconds < 10 ? 0 : 1;
