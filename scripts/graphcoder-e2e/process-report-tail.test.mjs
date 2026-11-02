import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { Processes } from "./process.mjs";
import { executionSucceeded } from "./assertions.mjs";

const captured = JSON.parse(readFileSync(new URL("./fixtures/native-report-ready.json", import.meta.url)));
// Replay recorded native frames through the shipped process/report owner and
// actual child pipe. Only launch identities and explicit damage are changed.
const producer = `
  const { connect, Socket } = require('node:net');
  const { createInterface } = require('node:readline');
  const { ready, acknowledgement } = JSON.parse(process.argv[1]);
  const [damage, requested] = process.argv.slice(2);
  const channel = process.env.ACYCLIC_REPORT_PIPE
    ? connect(process.env.ACYCLIC_REPORT_PIPE) : new Socket({ fd: 3 });
  ready.generation = process.env.ACYCLIC_REPORT_GENERATION;
  const finish = prefix => {
    const tail = damage === 'json' ? JSON.stringify(ready).slice(0, -1) + '\\n'
      : damage === 'tail' ? JSON.stringify(ready) : '';
    channel.end(prefix + tail, () => channel.destroy());
  };
  if (requested === 'true') {
    const input = createInterface({ input: channel });
    input.once('line', line => {
      const request = JSON.parse(line);
      input.close();
      acknowledgement.generation = request.generation;
      acknowledgement.request_id = request.request_id;
      finish(JSON.stringify(acknowledgement) + '\\n');
    });
  }
  channel.write(JSON.stringify(ready) + '\\n');
  if (requested !== 'true') finish('');
`;

test("report framing failure survives a prior acknowledgement and successful physical exit", async () => {
  const scope = new Processes();
  try {
    for (const requested of [false, true]) for (const damage of ["", "json", "tail"]) {
      const running = await scope.startReported(process.execPath,
        ["-e", producer, JSON.stringify(captured), damage, String(requested)]);
      running.input.end();
      if (requested) {
        await running.reportReady;
        await running.requestReport(Number.MAX_SAFE_INTEGER);
      }
      const result = await running.closed;
      assert.equal(result.code, 0, result.stderr);
      assert.equal(result.signal, null);
      assert.equal(scope.children.size, 0, "the real original child must retire");
      assert.equal(result.report_capability.ready, true, "retain the actual ready fact");
      if (requested) {
        assert.equal(result.termination.report_acknowledged, true, "retain the original ACK");
        assert.equal(result.termination.report_outcome, "empty");
      }
      if (!damage) executionSucceeded(result);
      else {
        assert.equal(result.termination?.control_failure, damage === "json"
          ? "invalid-acknowledgement" : "unterminated-control-frame");
        assert.throws(() => executionSucceeded(result), error =>
          error.actual === result.termination.control_failure);
      }
    }
  } finally { await scope.close(); }
});
