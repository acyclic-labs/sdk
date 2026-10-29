import assert from "node:assert/strict";
import test from "node:test";
import { createInterface } from "node:readline";
import { Processes } from "./process.mjs";

test("deadline termination retains primitive diagnostics then physically drains", { skip: process.platform === "win32" }, async () => {
  for (const reports of [true, false]) {
    const scope = new Processes();
    const script = `process.on('SIGTERM', () => { ${reports ? "process.stderr.write('waiting: provider-event opaque-1 since 1.125 (0.1s elapsed)\\n', () => process.exit(143));" : ""} }); process.stdout.write('ready\\n'); process.stdin.resume();`;
    const running = scope.start(process.execPath, ["-e", script], { scopeReportSignal: "SIGTERM" });
    const ready = createInterface({ input: running.child.stdout });
    await new Promise((resolve, reject) => { ready.once("line", resolve); running.closed.then(() => reject(new Error("producer exited before readiness"))); });
    ready.close();
    await scope.close();
    const result = await running.closed;
    assert.equal(result.termination.report_requested, true);
    assert.equal(result.termination.missing_wait_report, !reports);
    if (reports) {
      assert.deepEqual(result.termination.stuck_on, [{ primitive: "provider-event", key: "opaque-1", since: 1.125 }]);
      assert.equal(result.code, 143);
    } else {
      assert.equal(result.signal, "SIGKILL", "an unresponsive report producer must still be physically retired");
      assert.deepEqual(result.termination.stuck_on, []);
    }
    assert.equal(scope.children.size, 0);
  }
});
