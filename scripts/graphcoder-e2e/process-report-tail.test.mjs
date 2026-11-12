import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { once } from "node:events";
import { createServer } from "node:net";
import test from "node:test";
import { Processes, withCancellation } from "./process.mjs";
import { ProcessReport } from "./process-report.mjs";
import { executionSucceeded } from "./assertions.mjs";

const captured = JSON.parse(readFileSync(new URL("./fixtures/native-report-ready.json", import.meta.url)));

test("native terminal failures retire a live executable independently of diagnostic acknowledgement", async () => {
  for (const phase of ["startup", "execution", "execution-after-ack"]) {
    const scope = new Processes();
    try {
      const running = await scope.startReported(process.execPath, ["-e", `
        const { Socket } = require('node:net');
        const { connect } = require('node:net');
        const { createInterface } = require('node:readline');
        const channel = process.env.ACYCLIC_REPORT_PIPE
          ? connect(process.env.ACYCLIC_REPORT_PIPE) : new Socket({ fd: 3 });
        const generation = process.env.ACYCLIC_REPORT_GENERATION;
        const phase = process.argv[1];
        createInterface({ input: channel }).on('line', line => {
          const request = JSON.parse(line);
          channel.write(JSON.stringify({ generation, request_id: request.request_id, outcome: 'empty', waits: [] }) + '\\n');
        });
        if (phase !== 'startup') channel.write(JSON.stringify({ generation, ready: true }) + '\\n');
        process.stdin.once('data', () => {
          const cause = { phase: phase === 'startup' ? 'startup' : 'execution', cause: 'original native refusal', raw_os_error: 5 };
          channel.write(JSON.stringify({ generation, [phase === 'startup' ? 'rejected' : 'failed']: cause }) + '\\n');
        });
      `, phase]);
      const completed = withCancellation(running, running.failure);
      completed.catch(() => {});
      if (phase !== "startup") await running.reportReady;
      if (phase === "execution-after-ack") {
        await running.requestReport(Number.MAX_SAFE_INTEGER);
        assert.equal(running.failure.aborted, false, "a diagnostic ACK is not execution settlement");
      }
      running.input.write("fail\n");
      await assert.rejects(completed, error => {
        assert.equal(error.code, phase === "startup" ? "PROCESS_STARTUP_REJECTED" : "PROCESS_EXECUTION_FAILED");
        assert.equal(error.cause.cause, "original native refusal");
        return true;
      });
      const result = await running.closed;
      assert.ok(result.signal || result.code !== 0, "the still-live producer is stopped on its native failure event");
      assert.equal(scope.children.size, 0, "native settlement includes physical retirement");
      if (phase === "execution-after-ack") assert.equal(result.termination.report_acknowledged, true);
    } finally { await scope.close(); }
  }
});

test("ordinary commands retain expected native refusal status and output", async () => {
  const scope = new Processes();
  try {
    for (const phase of ["startup", "execution"]) {
      const running = await scope.startReported(process.execPath, ["-e", `
        const { Socket, connect } = require('node:net');
        const channel = process.env.ACYCLIC_REPORT_PIPE
          ? connect(process.env.ACYCLIC_REPORT_PIPE) : new Socket({ fd: 3 });
        const generation = process.env.ACYCLIC_REPORT_GENERATION;
        const phase = process.argv[1];
        const cause = { phase, cause: 'missing explicit credentials', raw_os_error: 2 };
        channel.end(JSON.stringify({ generation, [phase === 'startup' ? 'rejected' : 'failed']: cause }) + '\\n', () => {
          process.stdout.write(JSON.stringify({ kind: 'error', code: 'execution_failed', step: 'execute', message: '--credentials' }) + '\\n', () => process.exit(1));
        });
      `, phase]);
      const result = await withCancellation(running);
      assert.equal(result.code, 1, result.stderr);
      assert.equal(result.signal, null, "native refusal is not forced cancellation");
      assert.deepEqual(JSON.parse(result.stdout), {
        kind: "error", code: "execution_failed", step: "execute", message: "--credentials",
      });
      assert.equal((result.termination.startup_rejection ?? result.termination.execution_failure).cause,
        "missing explicit credentials");
      assert.throws(() => executionSucceeded(result), /selected native execution reported failure/);
      assert.equal(scope.children.size, 0);
    }
  } finally { await scope.close(); }
});
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
