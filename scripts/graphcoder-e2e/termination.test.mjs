import assert from "node:assert/strict";
import test from "node:test";
import { once } from "node:events";
import fs, { mkdtempSync, readFileSync } from "node:fs";
import { syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";
import { Processes, ProcessObservationFailed } from "./process.mjs";

test("original process phases retain exact input, output and exit without treating close as EOF", async () => {
  const operation = "00000000-0000-0000-0000-000000000001", events = [];
  const scope = new Processes({ onProgress: event => events.push(event) });
  try {
    assert.throws(() => scope.start(process.execPath, [], { scopeOperation: "path/or/prompt" }), /process-progress-operation/);
    assert.equal(scope.started, 0, "invalid correlation cannot launch a process");
    for (const count of [1, 128]) {
      const program = `const input=[];process.stdin.on('data',bytes=>input.push(bytes));process.stdin.on('end',()=>{process.stdout.write(Buffer.concat(input));for(let index=0;index<${count};index++)process.stdout.write(Buffer.alloc(8192,97));process.stderr.write('original stderr');process.exitCode=124;});`;
      const running = scope.start(process.execPath, ["-e", program], { scopeOperation: operation });
      running.input.write("original ");
      running.input.end("input");
      const result = await running.closed;
      assert.equal(result.code, 124, "the actual child exit is not a cancellation inference");
      assert.equal(result.signal, null);
      assert.equal(result.stdout, "original input" + "a".repeat(count * 8192));
      assert.equal(result.stderr, "original stderr");
      const rows = events.filter(event => event.ordinal === scope.started - 1);
      assert.ok(rows.length <= 19, "fixed first observations do not grow with output transfers");
      assert.equal(new Set(rows.map(event => event.phase)).size, rows.length);
      for (const phase of ["node-spawn", "stdin-write-admitted", "stdin-write-complete", "stdin-end-submitted",
        "stdin-finish", "stdin-close", "stdout-first-write", "stderr-first-write", "stdout-eof", "stderr-eof",
        "stdout-finish", "stderr-finish", "node-exit", "node-close", "reaped", "closed"]) {
        assert.ok(rows.some(event => event.phase === phase), phase);
      }
      for (const event of rows) {
        assert.equal(event.schema, "acyclic.process-progress.v1");
        assert.equal(event.operation, operation);
        assert.equal(event.report_generation, null);
        assert.equal(event.report_request_id, null);
        assert.ok(Number.isFinite(event.elapsed_seconds) && event.elapsed_seconds >= 0);
        assert.deepEqual(Object.keys(event).sort(), ["schema", "ordinal", "operation", "report_generation",
          "report_request_id", "phase", "elapsed_seconds", ...(event.phase === "node-exit" ? ["code", "signal"] : [])].sort());
      }
      const exit = rows.find(event => event.phase === "node-exit");
      assert.equal(exit.code, 124);
      assert.ok(exit.elapsed_seconds <= rows.find(event => event.phase === "reaped").elapsed_seconds);
      assert.ok(rows.find(event => event.phase === "reaped").elapsed_seconds <= rows.find(event => event.phase === "closed").elapsed_seconds);
    }
    assert.ok(events.some(event => event.ordinal === 0) && events.some(event => event.ordinal === 1),
      "the same operation's launches retain distinct original ordinals without PID matching");
    const destroyed = scope.start(process.execPath, ["-e", "process.stdin.resume()"], { scopeOperation: operation });
    destroyed.child.stdout.destroy();
    destroyed.input.end();
    assert.equal((await destroyed.closed).code, 0);
    const rows = events.filter(event => event.ordinal === 2);
    assert.ok(rows.some(event => event.phase === "stdout-close"));
    assert.ok(!rows.some(event => event.phase === "stdout-eof"), "destroying the original source is not EOF");
    assert.ok(!rows.some(event => event.phase.endsWith("first-write")), "an empty stream has no observed bytes");
    assert.equal(scope.children.size, 0);
  } finally { await scope.close(); }
});

test("original process phase observer loss is typed only after the same physical close", async () => {
  const operation = "00000000-0000-0000-0000-000000000002";
  const cause = new Error("original failed phase sink"), events = [];
  const scope = new Processes({ onProgress: event => {
    events.push(event);
    if (event.phase === "node-spawn" || event.phase === "closed") throw cause;
  } });
  try {
    const running = scope.start(process.execPath, ["-e", "process.stdin.resume()"], { scopeOperation: operation });
    let physicallyClosed = false;
    running.child.once("close", () => { physicallyClosed = true; });
    const spawned = once(running.child, "spawn");
    await spawned;
    assert.equal(physicallyClosed, false, "a failed observation cannot certify owner exit");
    const closing = scope.close().then(() => assert.fail("the scope must retain its observer failure"), error => error);
    const original = await running.closed.then(() => assert.fail("a lost observer cannot become a successful result"), error => error);
    assert.ok(original instanceof ProcessObservationFailed);
    assert.equal(original.code, "PROCESS_OBSERVATION_FAILED");
    assert.equal(original.cause, cause);
    assert.equal(original.operation, operation);
    assert.equal(original.ordinal, 0);
    assert.equal(original.phase, "node-spawn", "later observer loss cannot overwrite the first terminal");
    assert.equal(physicallyClosed, true);
    assert.equal(scope.children.size, 0);
    assert.equal(await closing, original);
    assert.ok(events.some(event => event.phase === "reaped"));
    assert.ok(events.some(event => event.phase === "closed"));
  } finally { await scope.close().catch(error => { assert.ok(error instanceof ProcessObservationFailed); }); }

  const late = new Processes({ onProgress: event => { if (event.phase === "closed") throw cause; } });
  try {
    const running = late.start(process.execPath, ["-e", "process.stdin.resume()"], { scopeOperation: operation });
    running.input.end();
    await assert.rejects(running.closed, error => error instanceof ProcessObservationFailed
      && error.phase === "closed" && error.cause === cause);
    assert.equal(late.children.size, 0, "a failure after transcript completion still follows original physical retirement");
  } finally { await late.close().catch(error => { assert.ok(error instanceof ProcessObservationFailed); }); }
});

test("original process phase cleanup joins a blocked transcript after physical reap", async t => {
  const transcripts = mkdtempSync(join(tmpdir(), "process-phase-pipe-"));
  const path = join(transcripts, "0.stdout");
  // Hold the acknowledgement of a REAL file write. The original callback
  // remains the sole producer; the child and transcript still use real IO.
  const originalWrite = fs.write;
  let release, held;
  const pendingWrite = new Promise(resolve => { held = resolve; });
  t.mock.method(fs, "write", (...args) => {
    const callback = args.pop();
    return originalWrite(...args, (...result) => {
      if (!release) {
        let completed = false;
        release = () => { if (!completed) { completed = true; callback(...result); } };
        held();
      }
      else callback(...result);
    });
  });
  syncBuiltinESMExports();
  const cause = new Error("original final observer failed");
  let reaped;
  const physical = new Promise(resolve => { reaped = resolve; });
  const scope = new Processes({ transcripts, onProgress: event => {
    if (event.phase === "reaped") reaped();
    if (event.phase === "closed") throw cause;
  } });
  try {
    const running = scope.start(process.execPath, ["-e", "process.stdout.write('phase-output')"]);
    running.input.end();
    await Promise.all([physical, pendingWrite]);
    assert.equal(scope.children.size, 0, "the original child is already physically retired");
    let completed = false;
    const closing = scope.close();
    const observed = closing.then(() => { completed = true; }, error => { completed = true; return error; });
    assert.equal(scope.close(), closing, "cleanup retains the original terminal join");
    // One scheduler turn lets all already-runnable close continuations finish.
    // The original write callback remains held until released below.
    await new Promise(setImmediate);
    const completedBeforeDrain = completed;
    release();
    const failure = await observed;
    assert.equal(completedBeforeDrain, false, "physical reap cannot certify the still-blocked transcript/final observation");
    assert.ok(failure instanceof ProcessObservationFailed);
    assert.equal(failure.phase, "closed");
    assert.equal(failure.cause, cause);
    await assert.rejects(running.closed, error => error === failure);
    await assert.rejects(scope.close(), error => error === failure);
    assert.equal(readFileSync(path, "utf8"), "phase-output");
  } finally {
    release?.();
    await scope.close().catch(error => { assert.ok(error instanceof ProcessObservationFailed); });
    t.mock.restoreAll(); syncBuiltinESMExports();
  }
});

// This is the actual native report producer from the compiled test target,
// rather than a hand-authored external protocol transcript.
const probe = process.env.E2E_REPORT_TEST_BINARY;
const selector = "obs::report_port::tests::inherited_report_port_joins_the_selected_real_process";

function launch(scope, owned, mode) {
  assert.ok(probe && isAbsolute(probe), "report-control step=admit failed; recovery command: cargo test --locked -p acyclic-native-runtime --lib --no-run; set E2E_REPORT_TEST_BINARY to its emitted executable");
  const start = owned ? scope.startOwned.bind(scope) : scope.start.bind(scope);
  return start(probe, ["--exact", selector, "--nocapture"], {
    scopeReport: true, env: { ...process.env, ACYCLIC_REPORT_PORT_CHILD: mode },
  });
}

async function ready(running) {
  await Promise.race([running.reportReady, running.closed.then(() => {
    throw new Error("report-control step=ready failed; recovery command: inspect the compiled native report producer's stderr and executable binding");
  })]);
}

test("selected report acknowledgement distinguishes live, empty, refusal and producer loss before physical drain", { skip: process.platform === "win32" }, async () => {
  for (const owned of [false, true]) for (const [mode, expected] of [
    ["live", "report"], ["1", "empty"], ["expired", "unsupported"],
    ["crash", "exited"], ["hold", "unacknowledged"],
  ]) {
    const scope = new Processes();
    const running = launch(scope, owned, mode);
    try {
      await ready(running);
      if (mode === "expired") await running.requestReport(0);
      // Closing stderr must not alter delivery on the dedicated control port.
      if (mode === "live") running.child.stderr.destroy();
      await scope.close();
      const result = await running.closed;
      assert.equal(result.termination.report_requested, true);
      if (mode === "crash") {
        assert.equal(result.code, 17, "the real selected producer exited without acknowledging");
        // EOF and process exit are independent native observations. Either
        // terminal is valid, but transport loss must retain its precise cause.
        assert.ok(result.termination.report_outcome === "exited"
          || (result.termination.report_outcome === "write-failed"
            && result.termination.report_cause === "control-eof-before-acknowledgement"));
      } else assert.equal(result.termination.report_outcome, expected);
      assert.equal(result.termination.report_acknowledged, ["report", "empty", "unsupported"].includes(expected));
      assert.equal(result.termination.missing_wait_report, !["report", "empty"].includes(expected));
      if (expected === "report") assert.equal(result.termination.stuck_on[0].primitive, "channel-close");
      else assert.deepEqual(result.termination.stuck_on, []);
      assert.equal(scope.children.size, 0, "acknowledgement never substitutes for physical retirement");
    } finally { await scope.close(); }
  }
});

test("rejected control delivery remains a sink failure and still retires the same owner", { skip: process.platform === "win32" }, async () => {
  for (const error of [undefined, new Error("deliberately withdrawn report sink")]) {
    const scope = new Processes();
    const running = launch(scope, false, "1");
    try {
      await ready(running);
      const channel = running.child.stdio[3];
      const closed = new Promise(resolve => channel.once("close", resolve));
      channel.destroy(error);
      await closed;
      await scope.close();
      const result = await running.closed;
      assert.equal(result.termination.report_outcome, "write-failed");
      assert.equal(result.termination.report_acknowledged, false);
      assert.equal(result.termination.missing_wait_report, true);
      assert.equal(scope.children.size, 0);
    } finally { await scope.close(); }
  }
});

test("selected owner exit cannot discard its already written acknowledgement", { skip: process.platform === "win32" }, async () => {
  const scope = new Processes();
  const running = launch(scope, false, "1");
  try {
    await ready(running);
    const channel = running.child.stdio[3];
    channel.pause();
    const acknowledged = running.requestReport(Date.now() + 500);
    await running.exited;
    // The real producer has exited after writing its reply. Delivering those
    // bytes later must preserve the snapshot rather than fabricate its loss.
    channel.resume();
    await acknowledged;
    const result = await running.closed;
    assert.equal(result.code, 0);
    assert.equal(result.termination.report_outcome, "empty");
    assert.equal(result.termination.report_acknowledged, true);
    assert.equal(result.termination.missing_wait_report, false);
    await scope.close();
    assert.equal(scope.children.size, 0);
  } finally { await scope.close(); }
});

test("a capability wait ends when its report producer closes while the process stays alive", { skip: process.platform === "win32" }, async () => {
  const scope = new Processes();
  const running = launch(scope, false, "no-ready");
  let physicallyClosed = false;
  running.child.once("close", () => { physicallyClosed = true; });
  try {
    await assert.rejects(running.reportReady, error => error.code === "REPORT_CONTROL_ENDED"
      && error.outcome === "write-failed");
    assert.equal(physicallyClosed, false, "port closure does not imply process exit");
    await scope.close();
    const result = await running.closed;
    assert.equal(result.termination.report_outcome, "write-failed");
    assert.equal(result.termination.report_acknowledged, false);
    assert.equal(physicallyClosed, true);
    assert.equal(scope.children.size, 0);
  } finally { await scope.close(); }
});

test("a requested report settles at producer EOF while the selected process remains alive", { skip: process.platform === "win32" }, async () => {
  const scope = new Processes();
  const running = launch(scope, false, "eof-after-request");
  let physicallyClosed = false;
  running.child.once("close", () => { physicallyClosed = true; });
  try {
    await ready(running);
    const eof = once(running.child.stdio[3], "end");
    let settled = false;
    const request = running.requestReport(Date.now() + 500).then(() => { settled = true; });
    await eof;
    assert.equal(settled, true, "producer EOF must settle the original request before physical exit");
    assert.equal(physicallyClosed, false);
    await request;
    await scope.close();
    const result = await running.closed;
    assert.equal(result.termination.report_outcome, "write-failed");
    assert.equal(result.termination.report_cause, "control-eof-before-acknowledgement");
    assert.equal(result.termination.report_acknowledged, false);
    assert.equal(physicallyClosed, true);
    assert.equal(scope.children.size, 0);
  } finally { await scope.close(); }
});

test("an observed owner exit settles a later report request without an acknowledgement", { skip: process.platform === "win32" }, async () => {
  const scope = new Processes();
  const running = launch(scope, false, "no-ready");
  try {
    await assert.rejects(running.reportReady, error => error.code === "REPORT_CONTROL_ENDED");
    const requested = new Promise(resolve => running.child.once("exit", () => {
      resolve(running.requestReport(Date.now() + 500));
    }));
    running.killExecutable();
    await requested;
    const result = await running.closed;
    assert.equal(result.termination.report_outcome, "exited");
    assert.equal(result.termination.report_acknowledged, false);
    await scope.close();
    assert.equal(scope.children.size, 0);
  } finally { await scope.close(); }
});

test("a dropped report request cannot acknowledge and concurrent close joins the same physical owner", { skip: process.platform === "win32" }, async () => {
  const transitions = [];
  const scope = new Processes({ onReport: transition => transitions.push(transition) });
  const running = launch(scope, false, "live");
  try {
    await ready(running);
    let requests = 0;
    // Drop the original outgoing request before the real producer receives it.
    running.child.stdio[3].write = (_chunk, done) => { requests++; done(); return true; };
    const closing = scope.close();
    assert.equal(scope.close(), closing, "all callers retain the original close join and deadline");
    await closing;
    const result = await running.closed;
    assert.equal(requests, 1, "only the admitted request can be sent");
    assert.equal(result.termination.report_outcome, "unacknowledged");
    assert.equal(result.termination.report_cause, "original-drain-deadline");
    assert.equal(result.termination.report_acknowledged, false);
    assert.deepEqual(transitions.map(transition => transition.outcome), ["requested", "unacknowledged"]);
    assert.ok(transitions.every(transition => transition.acknowledged === false));
    for (const transition of transitions) {
      assert.equal(transition.request_id, result.termination.request_id);
      assert.equal(transition.generation, result.termination.generation);
    }
    assert.equal(scope.children.size, 0);
  } finally { await scope.close(); }
});

test("a report transition observer failure is returned only after physical retirement", { skip: process.platform === "win32" }, async () => {
  const failure = new Error("deliberately failed report phase observer");
  const transitions = [];
  const scope = new Processes({ onReport: transition => { transitions.push(transition); throw failure; } });
  const running = launch(scope, false, "live");
  let physicallyClosed = false;
  running.child.once("close", () => { physicallyClosed = true; });
  try {
    await ready(running);
    await assert.rejects(scope.close(), error => error === failure);
    const result = await running.closed;
    assert.equal(physicallyClosed, true);
    assert.equal(scope.children.size, 0);
    assert.deepEqual(transitions.map(transition => transition.outcome), ["requested", "report"]);
    assert.deepEqual(transitions.map(transition => transition.acknowledged), [false, true]);
    assert.equal(result.termination.report_outcome, "report");
  } finally { await scope.close().catch(() => {}); }
});

test("a caller IO error cannot substitute for the selected process completion event", { skip: process.platform === "win32" }, async () => {
  const scope = new Processes();
  const running = launch(scope, false, "hold");
  let physicallyClosed = false;
  running.child.once("close", () => { physicallyClosed = true; });
  try {
    await ready(running);
    const failure = new Error("deliberately failed caller input");
    running.child.stdin.destroy(failure);
    await assert.rejects(running.closed, error => error === failure);
    assert.equal(physicallyClosed, false, "the original report producer is still alive");
    await scope.close();
    assert.equal(physicallyClosed, true, "cleanup joins the actual native completion");
    assert.equal(scope.children.size, 0);
  } finally { await scope.close(); }
});

test("closing the original scope joins its in-flight native pipe admission", { skip: process.platform !== "win32", timeout: 5000 }, async () => {
  const scope = new Processes();
  let settled = false;
  const admission = scope.startReported(process.execPath, ["--version"]);
  const rejected = assert.rejects(admission, /scope is closed/).then(() => { settled = true; });
  try {
    await scope.close();
    assert.equal(scope.started, 0, "closing cannot admit a later process");
    assert.equal(settled, true, "scope closure must join the original pending admission");
  } finally { await rejected; await scope.close(); }
});

test("the selected Windows native producer acknowledges through the original parser and physical owner", { skip: process.platform !== "win32" }, async () => {
  const selector = "obs::report_port::windows::tests::native_pipe_keeps_request_ack_and_producer_loss_on_the_selected_owner";
  assert.ok(probe && isAbsolute(probe), "report-control step=admit failed; recovery command: set E2E_REPORT_TEST_BINARY to the emitted Windows native-runtime test executable");
  for (const owned of [false, true]) for (const [mode, expired, expected] of [
    ["ack", false, "empty"], ["live", false, "report"], ["ack", true, "unsupported"],
  ]) {
    const scope = new Processes();
    const start = owned ? scope.startReportedOwned.bind(scope) : scope.startReported.bind(scope);
    let physicallyClosed = false;
    const running = await start(probe, ["--exact", selector, "--nocapture"], {
      env: { ...process.env, ACYCLIC_REPORT_PIPE_CHILD: mode },
    });
    running.child.once("close", () => { physicallyClosed = true; });
    try {
      await ready(running);
      await running.requestReport(expired ? 0 : Date.now() + 500);
      const result = await running.closed;
      await scope.close();
      assert.equal(result.code, 0, result.stderr);
      assert.equal(result.termination.report_outcome, expected);
      assert.equal(result.termination.report_acknowledged, true);
      assert.equal(result.termination.missing_wait_report, expired);
      assert.match(result.termination.generation, /^[0-9a-f]{32}$/);
      assert.match(result.termination.request_id, /^[0-9a-f]{32}$/);
      assert.notEqual(result.termination.request_id, result.termination.generation);
      if (expected === "report") {
        assert.equal(result.termination.stuck_on.length, 1);
        assert.equal(result.termination.stuck_on[0].primitive, "channel-close");
        assert.equal(result.termination.stuck_on[0].key, "report-control");
      } else assert.deepEqual(result.termination.stuck_on, []);
      assert.equal(physicallyClosed, true, "acknowledgement cannot substitute for native close");
      assert.equal(scope.children.size, 0);
    } finally { await scope.close(); }
  }
});
