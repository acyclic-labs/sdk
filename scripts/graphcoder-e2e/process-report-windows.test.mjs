import assert from "node:assert/strict";
import { connect } from "node:net";
import { mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";
import test from "node:test";
import { Processes, Spool } from "./process.mjs";
import { requireReportCapability } from "./assertions.mjs";

test("selected Windows owner requires its original bound physical completion", {
  skip: process.platform !== "win32",
}, async t => {
  const host = process.env.E2E_PROCESS_HOST;
  assert.ok(host && isAbsolute(host),
    "step=admit-native-completion-producer; recovery=set E2E_PROCESS_HOST to the absolute installed acyclic-process-host executable");
  const root = mkdtempSync(join(realpathSync(tmpdir()), "process-completion-"));
  try {
    for (const mode of ["plain", "reported-input", "wrong-invocation", "missing", "incomplete", "refused"]) {
      let running, original, failure, captured;
      const exitCode = mode === "reported-input" ? 0xc0000005 : 17;
      const directory = join(root, mode);
      mkdirSync(directory);
      const scope = new Processes({ transcripts: directory, onProgress(event) {
        if (event.phase !== "node-close") return;
        const index = running.child.spawnargs.indexOf("--completion");
        if (index < 0) return;
        const receipt = running.child.spawnargs[index + 1];
        original = readFileSync(receipt);
        if (mode === "missing") rmSync(receipt);
        else if (mode === "wrong-invocation" || mode === "incomplete") {
          const mutated = JSON.parse(original);
          if (mode === "wrong-invocation") mutated.invocation += "-foreign";
          else mutated.process_tree_retired = false;
          writeFileSync(receipt, JSON.stringify(mutated));
        }
      } });
      const options = { env: { ...process.env, E2E_PROCESS_HOST: host }, scopeCapture: result => { captured = result; },
        scopeInheritInput: mode === "reported-input" };
      // No native report producer is substituted for the selected executable.
      try {
        running = mode === "reported-input"
          ? await scope.startReportedOwned(process.execPath, ["-e", `process.exit(${exitCode})`], options)
          : scope.startOwned(mode === "refused" ? join(directory, "missing.exe") : process.execPath,
            ["-e", `process.exit(${exitCode})`], options);
        if (["wrong-invocation", "missing", "incomplete"].includes(mode)) {
          await assert.rejects(running.closed, error => {
            failure = error;
            assert.equal(error.code, "PROCESS_COMPLETION_INVALID");
            return true;
          });
        } else if (mode === "refused") {
          await assert.rejects(running.closed, error => {
            failure = error;
            assert.equal(error.code, "PROCESS_HOST_FAILED");
            assert.equal(error.completion.root_status, null);
            assert.equal(error.completion.process_tree_retired, false);
            assert.equal(error.completion.failure.phase, "root-admission");
            return true;
          });
        } else {
          const result = await running.closed;
          assert.equal(captured, result, "the original observer receives the validated selected process result");
          assert.equal(typeof result.completion, "object", "root exit requires the original bound physical completion");
          assert.deepEqual(result.completion, JSON.parse(original));
          assert.equal(result.code, exitCode);
          assert.equal(result.signal, null);
          assert.equal(result.completion.platform, "windows");
          assert.equal(result.completion.process_tree_retired, true);
          assert.equal(result.completion.failure, null);
          assert.equal(result.hostStatus.code >>> 0, exitCode);
          assert.equal(result.hostStatus.signal, null);
          if (mode === "plain") t.diagnostic(JSON.stringify({completion_base64: original.toString("base64")}));
        }
        if (failure) assert.equal(captured, undefined, "invalid completion cannot reach the terminal observer");
        assert.equal(scope.children.size, 0);
      } finally {
        if (failure) await assert.rejects(scope.close(), error => error === failure);
        else await scope.close();
      }
    }
  } finally { rmSync(root, {recursive: true}); }
});

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
    const recovery = 'node --test --test-name-pattern="selected Windows native producer acknowledges" scripts/graphcoder-e2e/process-report-windows.test.mjs';
    requireReportCapability(result, recovery);
    assert.throws(() => requireReportCapability({ ...result, report_capability: undefined }, recovery),
      /step=admit selected native report capability/,
      "the actual successful exit and ACK cannot replace the selected ready fact");
    assert.equal(result.code, 0, result.stderr);
    assert.deepEqual(result.report_capability, { generation: result.termination.generation, ready: true });
    assert.equal(Object.isFrozen(result.report_capability), true);
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
