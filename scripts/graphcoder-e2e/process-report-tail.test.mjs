import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { once } from "node:events";
import { createServer } from "node:net";
import test from "node:test";
import { Processes } from "./process.mjs";
import { ProcessReport } from "./process-report.mjs";
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
      : damage === 'tail' ? JSON.stringify(ready)
      : damage === 'foreign' ? JSON.stringify({ ...ready, generation: '0'.repeat(32) }) + '\\n'
      : damage === 'schema' ? JSON.stringify({ ...acknowledgement, outcome: 'invalid' }) + '\\n' : '';
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

for (const requested of [false, true]) test(`semantic report refusals survive physical exit (requested=${requested})`, async () => {
  const scope = new Processes();
  try {
    for (const damage of ["foreign", "schema"]) {
      const running = await scope.startReported(process.execPath,
        ["-e", producer, JSON.stringify(captured), damage, String(requested)]);
      running.input.end();
      await running.reportReady;
      if (requested) await running.requestReport(Number.MAX_SAFE_INTEGER);
      const result = await running.closed;
      assert.equal(result.code, 0, result.stderr);
      assert.equal(result.signal, null);
      assert.equal(scope.children.size, 0, "the original child must retire");
      assert.equal(result.report_capability.ready, true);
      if (requested) assert.equal(result.termination?.report_acknowledged, true);
      assert.equal(result.termination?.control_failure, "foreign-or-invalid-acknowledgement");
      assert.throws(() => executionSucceeded(result), error =>
        error.actual === result.termination.control_failure);
    }
  } finally { await scope.close(); }
});

for (const requested of [false, true]) test(`native report transport reset refuses success (requested=${requested})`, async () => {
  const scope = new Processes();
  try {
    const report = new ProcessReport();
    const listener = createServer();
    let channel;
    try {
      listener.listen(0, "127.0.0.1");
      await once(listener, "listening");
      const connected = once(listener, "connection");
      const running = scope.start(process.execPath, ["-e", `
        const { connect } = require('node:net');
        const { createInterface } = require('node:readline');
        const [port, generation, frames] = process.argv.slice(1);
        const { ready, acknowledgement } = JSON.parse(frames);
        const channel = connect(Number(port), '127.0.0.1');
        ready.generation = generation;
        createInterface({ input: channel }).once('line', line => {
          const request = JSON.parse(line);
          acknowledgement.generation = request.generation;
          acknowledgement.request_id = request.request_id;
          channel.write(JSON.stringify(acknowledgement) + String.fromCharCode(10));
        });
        channel.write(JSON.stringify(ready) + String.fromCharCode(10));
        channel.once('close', () => process.exit(0));
        process.stdin.once('data', () => channel.resetAndDestroy());
      `, String(listener.address().port), report.generation, JSON.stringify(captured)]);
      [channel] = await Promise.race([connected, running.closed.then(result => {
        executionSucceeded(result);
        throw new Error("step=connect native report control; recovery=inspect original child stdout/stderr");
      })]);
      report.attach(running.child, channel);
      await report.ready;
      if (requested) await report.request(Number.MAX_SAFE_INTEGER);
      // A genuine native reset after the original ACK/ready, with no sleep
      // or authored transport error. The same child still exits successfully.
      running.input.write("reset\n");
      await report.closeTransport(() => {});
      const result = await running.closed;
      assert.equal(result.code, 0, result.stderr);
      assert.equal(result.signal, null);
      assert.equal(scope.children.size, 0);
      assert.equal(report.termination?.control_failure, "ECONNRESET");
      if (requested) assert.equal(report.termination.report_acknowledged, true);
      assert.throws(() => executionSucceeded({ ...result, termination: report.termination }),
        error => error.actual === "ECONNRESET");
    } finally {
      channel?.destroy();
      await new Promise(resolve => listener.close(resolve));
    }
  } finally { await scope.close(); }
});
