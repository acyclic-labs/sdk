#!/usr/bin/env node
// Data cases drive the shipping executable through argv, HTTP and real PTYs.
// Build graphcoder and harness-mock-model ahead of time, then run:
//   node scripts/graphcoder-e2e/run.mjs GRAPHCODER MOCK_MODEL [case-id...]
// Install scripts/graphcoder-pty's pinned @xterm/headless before timing.
// On Windows, cross-build the ConPTY transport and set E2E_CONPTY_HOST
// to its EXE. Use serial.mjs for the full matrix. E2E_SCRATCH_ROOT selects
// all temporary directories and is mandatory on Windows.
// E2E_OUTPUT selects the matrix/capture directory; roots are retained on failure.
// E2E_CASES_ROOT selects an isolated cases/ and coverage.json input tree.
// E2E_PRIMARY_MOUNTING=auto exercises default capability selection; primary
// filesystem variants otherwise select native explicitly.
// E2E_PTY_FRAMES=1 logs observed frames, consumed actions and model requests.
// Unix requires python3 and a controlling PTY; no alternate GraphCoder runtime.
import assert from "node:assert/strict";
import { randomBytes, randomUUID } from "node:crypto";
import { existsSync, mkdirSync, readFileSync } from "node:fs";
import { chmod, mkdir, utimes, writeFile } from "node:fs/promises";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { fileURLToPath } from "node:url";
import { Processes, Spool, schedule, withCancellation } from "./process.mjs";
import { executionSucceeded, requests, subset, recordedRequests, jsonlReply, CommittedRequests, isGeneration, toolResults, toolOutcome } from "./assertions.mjs";
import { seed, expand as expandFixture } from "./fixtures.mjs";
import { inventory, readCoverage } from "./coverage.mjs";
import { fenced } from "./orphan-check.mjs";
import { qualification } from "./qualification.mjs";
import { failedCase, failureReport, finishCase, publishReport, retainCapture, verifyReport } from "./report.mjs";
import { environment, scratchDirectory } from "./scratch.mjs";
import { commandCancellation, withOwnedCase } from "./suite.mjs";
import { mountingArguments, variants } from "./serial.mjs";
import { verifyNativeTrace } from "./native-trace.mjs";
import { conversation } from "./conversation.mjs";
import { nativeHostEnvironment } from "../../plugin/tests/hosts/native-host.mjs";
import { withFileSource } from "./file-source.mjs";

const [binaryArg, mockArg, ...selected] = process.argv.slice(2);
const recovery = [process.execPath, ...process.argv.slice(1)];
assert.ok(binaryArg && mockArg, "usage: node run.mjs GRAPHCODER MOCK_MODEL [case-id...]");
const binary = resolve(binaryArg), mock = resolve(mockArg);
const directory = dirname(fileURLToPath(import.meta.url));
const scratch = scratchDirectory("graphcoder-e2e-");
const output = resolve(process.env.E2E_OUTPUT ?? join(scratch, "results"));
mkdirSync(output, { recursive: true });
const caseRoot = resolve(process.env.E2E_CASES_ROOT ?? directory);
const scripts = inventory(caseRoot);
const coverage = readCoverage(scripts, caseRoot);
const suites = variants(scripts, process.platform);
const identities = new Set(suites.map(test => test.id));
for (const id of selected) assert.ok(identities.has(id), `unknown case ${id}`);
assert.ok(process.platform !== "win32" || selected.length === 1,
  "Windows requires one case variant at a time; use serial.mjs for the complete matrix");
const cases = suites.filter(test => !selected.length || selected.includes(test.id))
  .map(test => ({ ...test, root: join(scratch, test.variant) }));
assert.ok(cases.length, "no matching cases");
const hosts = new Set(cases.flatMap(test => test.inputs.flatMap(input =>
  input.format === "installed-host" ? [input.host] : input.format === "scripted-host" ? ["codex"] : [])));
const { env: hostEnvironment } = await nativeHostEnvironment(hosts, process.env);
const roots = [...new Set(cases.map(test => test.root))];
for (const root of roots) mkdirSync(root, { recursive: true });
assert.equal(new Set(cases.map(test => test.id)).size, cases.length, "case identities must be unique");
const processes = new Processes({ transcripts: join(output, "transcripts"), onReport: event => {
  process.stderr.write(`case-lifecycle: ${JSON.stringify({ phase: "report", ...event })}\n`);
} });
const cancellation = commandCancellation();
mkdirSync(join(output, "transcripts"), { recursive: true });
const contexts = new Map();
const evidence = new Map();
let server, events, port, committed;
const caseStates = new Map(cases.map(test => [test.id, { test, failure: new AbortController() }]));
// Private to the original runner/server/native host chain. It is never part
// of a case file, report, request body or retained provider configuration.
const nativeCases = new Set(cases.filter(test => test.inputs.some(input => input.format === "installed-host" && input.host === "pi"))
  .map(test => test.id));
const nativeCapability = nativeCases.size ? randomBytes(32).toString("hex") : undefined;
const started = 0; // Process start: suite time includes module load, as the envelope does.
const json = (path, value) => writeFile(path, JSON.stringify(value, null, 2) + "\n");
function expand(value, fixture) {
  return expandFixture(value, { support: directory.replaceAll("\\", "/"),
    provider: fixture.provider,
    sessions: fixture.sessions,
    repository: resolve(directory, "../..").replaceAll("\\", "/"),
    workspace: fixture.workspace.replaceAll("\\", "/"),
    ...(port ? { url: `http://127.0.0.1:${port}/${nativeCases.has(fixture.id) ? 'native-' : ''}${fixture.id}/v1` } : {}) });
}
// GraphCoder's headless outcome: `-p` prints the answer text and exits 0;
// JSONL prints one `{"id", "inbox", "text"}` (or `"error"`) line per prompt.
function outcome(input, result, id) {
  const failure = result.termination?.startup_rejection ?? result.termination?.execution_failure;
  if (failure) return { completion: "failed", error: failure.cause };
  if (input.mode !== "jsonl") {
    return result.code === 0 ? { completion: "completed", ok: { text: result.stdout.replace(/\r?\n$/, "") } }
      : { completion: "failed", error: result.stderr.trim() };
  }
  const line = jsonlReply(result.stdout, id);
  return line.error === undefined ? { completion: "completed", ok: { text: line.text } } : { completion: "failed", error: line.error };
}
// A mock request is indexed by its case and step, and by its case, path and step.
const requestKey = (id, { path, step }) => JSON.stringify(path === undefined ? [id, step] : [id, path, step]);
async function checkTree(test) {
  if (test.assert.dead_tree) {
    const records = await recordedRequests(events, join(output, "requests.jsonl"));
    fenced(records.filter(record => record.case === test.id), test.assert.dead_tree);
  }
}
async function prepare(test, gitOrigin) {
  const begin = performance.now();
  const { failure } = caseStates.get(test.id);
  const fixture = { id: test.id, signal: AbortSignal.any([cancellation.signal, failure.signal]),
    fail: error => failure.abort(error), workspace: join(test.root, test.id, "workspace"), settings: join(test.root, test.id, "settings"), catalog: join(test.root, test.id, "catalog.json") };
  const artifact = join(output, test.id); await mkdir(artifact, { recursive: true });
  fixture.url = `http://127.0.0.1:${port}/${test.id}/v1`;
  fixture.binary = binary;
  fixture.output = artifact;
  fixture.env = environment(fixture.settings, hostEnvironment);
  fixture.provider = test.keychain ? `e2e-first-run-${randomUUID()}` : "e2e";
  if (nativeCases.has(test.id)) {
    fixture.url = `http://127.0.0.1:${port}/native-${test.id}/v1`;
    fixture.nativeCapability = nativeCapability;
  }
  await mkdir(dirname(fixture.workspace), { recursive: true });
  if (test.workspace?.kind === "git") {
    const cloned = await processes.run("git", ["clone", "--quiet", "--shared", gitOrigin, fixture.workspace], { env: fixture.env });
    assert.equal(cloned.code, 0, cloned.stderr);
  } else await mkdir(fixture.workspace, { recursive: true });
  await mkdir(fixture.settings, { recursive: true });
  await Promise.all([seed(fixture.workspace, expand(test.workspace?.files, fixture)), seed(fixture.settings, expand(test.settings, fixture))]);
  if (test.workspace?.metadata?.mode !== undefined) await chmod(fixture.workspace, test.workspace.metadata.mode);
  if (test.workspace?.metadata?.modified !== undefined) {
    await utimes(fixture.workspace, test.workspace.metadata.modified, test.workspace.metadata.modified);
  }
  const provider = { adapter: test.adapter ?? "responses", authentication: test.authentication ?? "optional", base_url: expand(test.base_url ?? fixture.url, fixture),
    models: Object.fromEntries((test.models ?? ["tiny"]).map(model => [model, {
      capacity: test.capacity ?? { context_tokens: 32768, output_tokens: 1024 }, features: { tools: true, images: true }
    }])),
    token_counter: { path: "count", adapter: test.adapter ?? "responses", response_field: "input_tokens", framing_tokens: 0 } };
  await json(fixture.catalog, { ...(test.first_run ? { default_model: `${fixture.provider}/tiny` } : {}), [fixture.provider]: provider, morph: { ...provider, models: { ...provider.models, "morph-kimik31": Object.values(provider.models)[0], "morph-dsv41flash": Object.values(provider.models)[0] } } });
  fixture.env.E2E_MOCK_URL = fixture.url;
  fixture.env.ACYCLIC_TEST_SOURCE_ROOT = resolve(directory, "../..");
  fixture.requested = request => events.has(requestKey(test.id, request));
  fixture.onRequest = listener => events.subscribe(event => {
    if (event.case === test.id) listener(event);
  });
  fixture.waitRequest = request => events.keyed(requestKey(test.id, request));
  const selection = ["--workspace", fixture.workspace, "--catalog", fixture.catalog,
    ...(test.first_run ? [] : ["--model", `${fixture.provider}/tiny`])];
  const extra = expand(test.extra_args ?? [], fixture);
  fixture.clientArgs = [...selection, ...extra];
  fixture.args = [...selection, "--settings-root", fixture.settings,
    ...mountingArguments(test), ...extra];
  // Tracing owns explicit text storage, including through the native PTY host.
  for (const option of ["diagnostic-records", "diagnostic-record-bytes"]) {
    const value = process.env[`E2E_${option.toUpperCase().replaceAll("-", "_")}`];
    if (value !== undefined) fixture.args.push(`--${option}`, value);
  }
  return { fixture, artifact, seconds: (performance.now() - begin) / 1000 };
}
async function execute(test, prepared) {
  const begin = performance.now();
  const { fixture, artifact, seconds: preparationSeconds } = prepared;
  const captures = [], interrupted = [];
  let result, service;
  const ownerArguments = [...fixture.args];
  try {
    const operation = "00000000-0000-0000-0000-000000000001";
    const messageId = "e2e";
    for (const raw of test.inputs) {
      const input = expand(raw, fixture);
      const inputFixture = { ...fixture, env: { ...fixture.env, ...input.environment } };
      if (input.mode === "drive") {
        result = await processes.runOwned(process.execPath,
          [fileURLToPath(new URL("../graphcoder-pty/drive-case.mjs", import.meta.url)), binary, JSON.stringify(fixture.args)],
          { cwd: fixture.workspace, env: inputFixture.env });
        executionSucceeded(result);
        captures.push(result);
        continue;
      }
      if (input.mode === "service") {
        if (input.kill) {
          assert.ok(service, "service kill requires its original owner");
          service.killExecutable();
          await service.closed;
          service = undefined;
          continue;
        }
        // The service belongs to the case, rather than to a terminal whose
        // original custody owner correctly retires all of its descendants.
        service = await processes.startOwned(binary,
          [...ownerArguments, ...(input.endpoint ? ["serve", "127.0.0.1:0"] : ["--local-service", "--signal-ready"])],
          { cwd: fixture.workspace, env: inputFixture.env, scopeCapture: result => {
            captures.push({ ...result, stdout: "" });
            process.stderr.write(`service-termination: ${JSON.stringify({ case: test.id, code: result.code, signal: result.signal, stderr: result.stderr })}\n`);
          } });
        if (input.endpoint) {
          const publication = await new Spool(service.child.stdout, service.closed,
            { transcript: join(artifact, "endpoint.jsonl") }).next(() => true);
          fixture.env.ACYCLIC_API_KEY = publication.credential;
          fixture.args = [...fixture.clientArgs, "--endpoint", publication.endpoint];
          continue;
        }
        await new Promise((resolve, reject) => {
          const ready = bytes => {
            service.child.stdout.off("data", ready);
            if (bytes[0] === 10) resolve();
            else reject(new Error("session service refused readiness"));
          };
          service.child.stdout.on("data", ready);
          service.closed.then(result => {
            service.child.stdout.off("data", ready);
            reject(new Error(`session service exited before readiness: ${result.stderr}`));
          }, reject);
        });
        fixture.args = fixture.clientArgs;
        continue;
      }
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
        await pty(processes, binary, inputFixture, input, capture => captures.push(capture));
        continue;
      }
      if (input.mode === "pty-group") {
        const { pty } = await import("./pty.mjs");
        const seen = new Set(), listeners = new Set();
        const signal = name => { seen.add(name); for (const listener of listeners) listener(); };
        const shared = { ...inputFixture, emitSignal: signal, signalled: name => seen.has(name),
          onSignal: listener => { listeners.add(listener); return () => listeners.delete(listener); } };
        const results = await Promise.allSettled(input.terminals.map(terminal =>
          pty(processes, binary, shared, terminal, capture => captures.push(capture))
            .catch(error => { fixture.fail(error); throw error; })
            .finally(() => signal(`${terminal.name}:closed`))));
        for (const result of results) if (result.status === "rejected") throw result.reason;
        continue;
      }
      const op = input.operation ?? operation;
      const args = input.args ?? (input.mode === "jsonl" ? ["--operation", op, "--input-format", "jsonl"] : ["--operation", op, "-p", input.text ?? "hello"]);
      await withFileSource(processes, fixture, input.file_source, async (path, closed) => {
        const arguments_ = [...fixture.args, ...args, ...(input.extra_args ?? [])]
          .map(argument => argument === "{{file_source}}" ? path : argument);
        const running = await processes.startReported(binary, arguments_, { cwd: input.cwd ?? fixture.workspace, env: inputFixture.env });
        if (input.kill_on) {
          const signal = AbortSignal.any([running.failure, fixture.signal]);
          await withCancellation(running, signal, Promise.race([fixture.waitRequest(input.kill_on), running.closed.then(result => { throw new Error(`exited before kill point: ${JSON.stringify(result)}`); })]));
          running.child.kill("SIGKILL");
          const result = await running.exited;
          assert.ok(result.signal || result.code !== 0);
          interrupted.push(retainCapture(running.closed, captures));
          return;
        } else if (input.mode === "jsonl") {
          // Message `<operation>:e2e`: rerunning the operation replays its answer.
          running.child.stdin.end(input.stdin ?? JSON.stringify({ prompt: input.text ?? "hello", id: messageId }) + "\n");
        } else running.child.stdin.end();
        let result;
        try { result = await withCancellation(running, fixture.signal, closed(running)); }
        catch (error) { captures.push(await running.closed); throw error; }
        captures.push(result);
        if (input.capture_root) {
          executionSucceeded(result);
          const root = JSON.parse(result.stdout);
          assert.equal(root.kind, "root");
          assert.equal(typeof root.agent, "string");
          fixture.sessions ??= {};
          assert.ok(!Object.hasOwn(fixture.sessions, input.capture_root));
          fixture.sessions[input.capture_root] = root.agent;
        }
        if ((input.exit ?? 0) === 0) executionSucceeded(result);
        else assert.equal(result.code, input.exit, `exit ${result.code} (signal ${result.signal}), expected ${input.exit}; stderr: ${result.stderr.trim() || "(empty)"}\nstdout tail: ${result.stdout.slice(-1500)}`);
        if (input.stdout_contains) for (const text of input.stdout_contains) assert.ok(result.stdout.includes(text), `missing stdout ${text}; stdout tail: ${result.stdout.slice(-1500) || "(empty)"}`);
        if (input.stderr_contains) for (const text of input.stderr_contains) assert.ok(result.stderr.includes(text), `missing stderr ${text}; stderr: ${result.stderr.trim().slice(-1500) || "(empty)"}`);
        for (const text of input.stdout_absent ?? []) assert.ok(!result.stdout.includes(text), `unexpected stdout ${text}`);
        if (((input.exit ?? 0) === 0 && !input.args) || input.outcome !== undefined) {
          const settled = outcome(input, result, messageId);
          if (input.outcome !== undefined) {
            assert.equal(settled.completion, input.outcome.completion, `terminal outcome mismatch: ${JSON.stringify(settled)}`);
            if (input.outcome.error !== undefined) assert.ok(settled.error.includes(input.outcome.error), `terminal error mismatch: ${JSON.stringify(settled)}`);
          } else assert.equal(settled.completion, "completed", JSON.stringify(settled));
          if (input.answer !== undefined) assert.equal(settled.ok.text, input.answer);
        }
      });
    }
    for (const { path, ignored } of test.assert.git_ignored ?? []) {
      const result = await processes.run("git", ["check-ignore", "--no-index", "--quiet", path],
        { cwd: fixture.workspace, env: fixture.env });
      assert.equal(result.code, ignored ? 0 : 1, `Git exclusion for ${path}: ${result.stderr}`);
    }
    // Check while the mock's held response is still open. Killing the mock
    // first would let an unfenced descendant exit and produce a false pass.
    await checkTree(test);
    contexts.set(test.id, { test, fixture, captures, artifact });
    const productSeconds = (performance.now() - begin) / 1000;
    result = { case: test.id, platform: process.platform, status: "pass", seconds: preparationSeconds + productSeconds,
      preparation_seconds: preparationSeconds, product_seconds: productSeconds };
  } catch (error) {
    let treeFailure;
    try { await checkTree(test); } catch (treeError) { treeFailure = failureReport(treeError); }
    const productSeconds = (performance.now() - begin) / 1000;
    result = { case: test.id, platform: process.platform, status: "fail", phase: "run", seconds: preparationSeconds + productSeconds,
      preparation_seconds: preparationSeconds, product_seconds: productSeconds, ...failureReport(error),
      tree_failure: treeFailure, workspace: fixture.workspace };
  } finally {
    if (test.keychain) {
      try {
        assert.ok(process.env.E2E_CREDENTIALS_FIXTURE && isAbsolute(process.env.E2E_CREDENTIALS_FIXTURE),
          "E2E_CREDENTIALS_FIXTURE must name the absolute native provider credential fixture");
        const removed = await processes.run(process.env.E2E_CREDENTIALS_FIXTURE, [fixture.provider], { env: fixture.env });
        assert.equal(removed.code, 0, removed.stderr);
      } catch (error) {
        result = { ...result, status: "fail", credential_cleanup: failureReport(error) };
      }
    }
    result = await finishCase(result, interrupted, recovery);
    evidence.set(test.id, { captures });
  }
  return result;
}
function verify(result, records) {
  if (result.status === "fail") return result;
  const { test, fixture, captures } = contexts.get(result.case);
  try {
    requests(test, records);
    if (test.assert.trace) verifyNativeTrace(fixture.workspace, test.assert.trace);
    const client = JSON.stringify(captures);
    for (const text of test.assert.wire_contains ?? []) assert.ok(client.includes(text), `client wire missing ${text}`);
    for (const text of test.assert.wire_absent ?? []) assert.ok(!client.includes(text), "client output exposed forbidden content");
    for (const [path, expected] of Object.entries(test.assert.files ?? {})) assert.equal(readFileSync(join(fixture.workspace, path), "utf8"), expected, path);
    for (const path of test.assert.absent_files ?? []) assert.ok(!existsSync(join(fixture.workspace, path)), `unexpected file ${path}`);
    for (const [path, expected] of Object.entries(test.assert.jsonl ?? {})) {
      const rows = readFileSync(join(fixture.workspace, path), "utf8").trim().split("\n").map(JSON.parse);
      for (const expectedRow of expected) assert.ok(rows.some(row => subset(row, expectedRow)), `missing JSONL ${JSON.stringify(expectedRow)} in ${path}`);
    }
    if (test.assert.usage_rollup) {
      const expected = test.assert.usage_rollup;
      const rows = readFileSync(join(fixture.workspace, expected.path), "utf8").trim().split("\n").map(JSON.parse);
      const agents = new Map();
      for (const row of rows) {
        assert.equal(row.version, 3);
        assert.equal(typeof row.usage.agent_path, "string");
        const calls = agents.get(row.usage.agent_path) ?? [];
        calls.push(row); agents.set(row.usage.agent_path, calls);
      }
      assert.equal(agents.size, expected.agents);
      const root = rows[0].usage.agent_path;
      for (const agent of agents.keys()) assert.ok(agent === root || agent.startsWith(`${root}/children/`));
      for (const [selection, calls] of [["root", agents.get(root)], ["subtree", rows]]) {
        const wanted = expected[selection];
        assert.equal(calls.length, wanted.calls, `${selection} calls`);
        assert.equal(calls.filter(row => row.usage.completed).length, wanted.completed, `${selection} completed`);
        const known = calls.filter(row => row.estimated_cost.token_nano_units !== null);
        const subtotal = known.reduce((sum, row) => sum + row.estimated_cost.token_nano_units, 0);
        assert.equal(known.length === calls.length ? subtotal : null, wanted.token_nano_units, `${selection} charge`);
        if (wanted.known_nano_units !== undefined) assert.equal(subtotal, wanted.known_nano_units, `${selection} partial facts`);
        for (const row of calls) {
          assert.equal(row.estimated_cost.currency, wanted.currency);
          assert.equal(row.estimated_cost.price_revision, wanted.price_revision);
        }
      }
    }
    return result;
  } catch (error) {
    return { ...result, status: "fail", ...failureReport(error), workspace: fixture.workspace };
  }
}
let results, currentPhase, failedPhase, execution;
const phases = {};
// Include process startup in the elapsed-time report.
try { execution = await withOwnedCase({ processes, signal: cancellation.signal, origin: 0, onPhase: event => {
  currentPhase = event.phase;
  if (event.outcome === "failed") failedPhase ??= event.phase;
  phases[event.phase] = event;
  process.stderr.write(`case-lifecycle: ${JSON.stringify(event)}\n`);
}, prepare: async () => {
  const scripts = Object.fromEntries(cases.map(test => [nativeCases.has(test.id) ? 'native-'+test.id : test.id, expand(test.model,
    { id: test.id, workspace: join(test.root, test.id, "workspace") })]));
  await json(join(output, "script.json"), { cases: scripts });
  const env = environment(join(scratch, "support-profile"));
  if (nativeCapability) env.ACYCLIC_E2E_MODEL_CAPABILITY = nativeCapability;
  server = processes.start(mock, ["--script", join(output, "script.json"), "--record", join(output, "requests.jsonl")], { env });
  events = new Spool(server.child.stdout, server.closed, { transcript: join(output, "events.jsonl"),
    key: event => event.event === "ready" ? ["ready"]
      : event.event === "request" ? [requestKey(event.case, { step: event.step }), requestKey(event.case, event)] : [] });
  port = (await events.keyed("ready")).port;
  committed = await CommittedRequests.open(join(output, "requests.jsonl"));
  events.subscribe(event => {
    if (event.event !== "request") return;
    const state = caseStates.get(event.case);
    committed.next(event).then(record => {
      if (!state || !isGeneration(record)) return;
      const { test } = state;
      for (const output of toolResults([record])) {
        let value;
        try { value = JSON.parse(output); } catch { value = output; }
        toolOutcome(value, test.assert.tool_errors, recovery.join(" "), test.id);
      }
    }).catch(error => state?.failure.abort(error));
  });
  let origin;
  if (cases.some(test => test.workspace?.kind === "git")) {
    origin = join(roots[0], "origin"); await mkdir(origin);
    await seed(origin, { "seed.txt": "base\n" });
    for (const args of [["init", "--quiet", "--initial-branch=main", origin], ["-C", origin, "add", "."],
      ["-C", origin, "-c", "user.name=e2e", "-c", "user.email=e2e@example.invalid", "-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "seed"]]) {
      const git = await processes.run("git", args, { env }); assert.equal(git.code, 0, git.stderr);
    }
  }
  return new Map(await schedule(cases, process.platform, async test => [test.id, await prepare(test, origin)]));
}, run: async prepared => {
  // Unix cases share one mock. Windows runs one case variant per process;
  // serial.mjs reports each variant's elapsed time separately.
  results = await schedule(cases, process.platform, test => execute(test, prepared.get(test.id)));
} }); }
catch (error) {
  const failure = failureReport(error);
  results = cases.map((test, index) => failedCase({ ...results?.[index], case: test.id, platform: process.platform },
    failure, failedPhase ?? currentPhase ?? "setup", recovery));
  execution = { preparation_seconds: phases.preparation?.seconds };
}
finally { cancellation.dispose(); await committed?.close(); }
// Every executable has exited and the server has been reaped. No concurrent
// append can expose an incomplete JSONL line to the assertion phase.
const verified = await verifyReport(results,
  () => port ? recordedRequests(events, join(output, "requests.jsonl")) : [],
  verify, recovery);
results = verified.results.map(result => ({ ...result, evidence: { ...evidence.get(result.case),
  ...(verified.requests ? { requests: verified.requests.get(result.case) ?? [] } : {}) } }));
const seconds = (performance.now() - started) / 1000;
const report = { platform: process.platform, seconds, product_seconds: execution.seconds, preparation_seconds: execution.preparation_seconds,
  phases, results, output, roots,
  selected, qualification: !selected.length, coverage };
await publishReport(join(output, "matrix.json"), report);
process.exitCode = results?.every(result => result.status === "pass")
  && (selected.length > 0 || coverage.pass) ? 0 : 1;
