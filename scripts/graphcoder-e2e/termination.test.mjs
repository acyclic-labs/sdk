import assert from "node:assert/strict";
import test from "node:test";
import { isAbsolute } from "node:path";
import { Processes } from "./process.mjs";

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
      assert.equal(result.termination.report_outcome, expected);
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
