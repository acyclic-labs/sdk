import assert from "node:assert/strict";
import { connect } from "node:net";
import { isAbsolute } from "node:path";
import test from "node:test";
import { Processes, Spool } from "./process.mjs";

test("selected Windows native producer acknowledges through the original decoder and physical close", {
  skip: process.platform !== "win32",
}, async () => {
  const executable = process.env.E2E_REPORT_TEST_BINARY;
  assert.ok(executable && isAbsolute(executable),
    "step=admit-native-report-producer; recovery=cargo test --locked -p acyclic-native-runtime --lib --no-run; set E2E_REPORT_TEST_BINARY to its Windows executable");
  const scope = new Processes();
  try {
    const running = await scope.startReported(executable, ["--exact",
      "obs::report_port::windows::tests::native_pipe_keeps_request_ack_and_producer_loss_on_the_selected_owner", "--nocapture"], {
      env: { ...process.env, ACYCLIC_REPORT_PIPE_CHILD: "ack" },
    });
    await running.reportReady;
    await running.requestReport(Date.now() + 500);
    const result = await running.closed;
    assert.equal(result.code, 0, result.stderr);
    assert.equal(result.termination.report_outcome, "empty");
    assert.equal(result.termination.report_acknowledged, true);
    assert.equal(result.termination.missing_wait_report, false);
    assert.equal(scope.children.size, 0);
  } finally { await scope.close(); }
});

test("selected Windows launch withdraws an unconnected report listener at original process close", {
  skip: process.platform !== "win32",
}, async () => {
  const scope = new Processes();
  const inherited = { ...process.env, ACYCLIC_REPORT_FD: "99", ACYCLIC_REPORT_PIPE: "foreign",
    ACYCLIC_REPORT_OWNER_PID: "1", ACYCLIC_REPORT_GENERATION: "foreign", ACYCLIC_REPORT_REQUEST_ID: "foreign" };
  const inspect = `console.log(JSON.stringify(Object.fromEntries(Object.entries(process.env)
    .filter(([key]) => key.startsWith('ACYCLIC_REPORT_'))))); process.stdin.resume();`;
  try {
    const running = await scope.startReported(process.execPath, ["-e", inspect], { env: inherited });
    const environment = await new Spool(running.child.stdout, running.closed).next(() => true);
    assert.match(environment.ACYCLIC_REPORT_GENERATION, /^[0-9a-f]{32}$/);
    assert.match(environment.ACYCLIC_REPORT_REQUEST_ID, /^[0-9a-f]{32}$/);
    assert.equal(environment.ACYCLIC_REPORT_PIPE, `\\\\.\\pipe\\acyclic-report-${environment.ACYCLIC_REPORT_GENERATION}`);
    assert.equal(environment.ACYCLIC_REPORT_OWNER_PID, String(process.pid));
    assert.equal(environment.ACYCLIC_REPORT_FD, undefined);
    running.child.stdin.end();
    const result = await running.closed;
    assert.equal(result.code, 0, result.stderr);
    await assert.rejects(running.reportReady, error => error.code === "REPORT_CONTROL_ENDED");
    assert.equal(result.termination, undefined, "exit without a request is not an acknowledged empty report");
    await new Promise((resolve, reject) => {
      const socket = connect(environment.ACYCLIC_REPORT_PIPE);
      socket.once("error", error => error.code === "ENOENT" ? resolve() : reject(error));
      socket.once("connect", () => { socket.destroy(); reject(new Error("original close retained its report listener")); });
    });
    const ordinary = scope.start(process.execPath, ["-e", inspect], { env: inherited });
    ordinary.child.stdin.end();
    const unselected = await ordinary.closed;
    assert.equal(unselected.code, 0, unselected.stderr);
    assert.deepEqual(JSON.parse(unselected.stdout), {}, "another launch cannot inherit the selected endpoint");
  } finally { await scope.close(); }
  assert.equal(scope.children.size, 0);
});
