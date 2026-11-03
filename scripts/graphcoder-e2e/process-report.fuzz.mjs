import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { readFileSync } from "node:fs";
import { Duplex } from "node:stream";
import { ProcessReport } from "./process-report.mjs";

const { ready, acknowledgement } = JSON.parse(readFileSync(new URL("./fixtures/native-report-ready.json", import.meta.url)));
const { execution_failure: failure } = JSON.parse(readFileSync(new URL("./fixtures/native-execution-failure.json", import.meta.url))).capture.termination;
const selected = { generation: acknowledgement.generation, requestId: acknowledgement.request_id };
const wire = value => Buffer.from(JSON.stringify(value) + "\n");
// Captured acknowledgement/cause, followed by a generated foreign-owner
// mutation. Arbitrary bytes cannot erase any of these prior observations.
const retained = Buffer.concat([wire(ready), wire(acknowledgement), wire({ generation: selected.generation, failed: failure }),
  wire({ ...ready, generation: "0".repeat(32) })]);
// Preserve a real OS error at the original channel/error interface. This is
// transport fault injection, not an authored external wire fixture.
let transportError;
try { readFileSync(new URL("./fixtures/absent-report-transport", import.meta.url)); }
catch (error) { transportError = error; }
assert.equal(transportError?.code, "ENOENT");

// Fuzz the actual stream/JSON/report decoder, including unterminated frames
// and source loss. This checks partition invariance, not native retirement.
export async function fuzz(data, selection = selected) {
  const bytes = Buffer.from(data);
  const result = await partitions(bytes, selection);
  for (const ownerExited of [false, true]) {
    const late = await partitions(bytes, selection, { late: true, ownerExited });
    if (bytes.length === 0) {
      assert.equal(late.termination.report_outcome, ownerExited ? "exited" : "write-failed",
        "a request after the original source ended must settle without another producer");
      assert.equal(late.termination.report_cause, ownerExited ? undefined : "control-eof-before-request");
    }
  }
  const protectedResult = await partitions(Buffer.concat([retained, bytes]), selected);
  assert.equal(protectedResult.capability.ready, true, "retain the original ready observation");
  assert.equal(protectedResult.termination.report_acknowledged, true, "later bytes cannot erase the captured ACK");
  assert.deepEqual(protectedResult.termination.execution_failure, failure, "later bytes cannot erase the original execution cause");
  assert.equal(protectedResult.termination.control_failure, "foreign-or-invalid-acknowledgement",
    "an ACK cannot authorize a later foreign frame or erase its first refusal");
  for (const error of [transportError, new Error(transportError.message, { cause: transportError })]) {
    for (const ownerExited of [false, true]) {
      for (const source of ["promise", "stream", "write"]) await sourceFailure(error, source, ownerExited);
      await sourceFailure(error, "stream", ownerExited, true);
    }
  }
  for (const ownerExited of [false, true]) await deadline(ownerExited);
  return result;
}

async function sourceFailure(error, source, ownerExited, acknowledged = false) {
  const report = new ProcessReport();
  Object.assign(report, selected);
  const child = new EventEmitter();
  const channel = new Duplex({ read() {}, write(_chunk, _encoding, done) {
    done(source === "write" ? error : undefined);
  } });
  const readiness = report.ready.then(() => "ready", failure => failure.code);
  try {
    report.attach(child, source === "promise" ? Promise.reject(error) : channel);
    if (ownerExited) child.emit("exit", 0, null);
    const completion = report.request(Number.MAX_SAFE_INTEGER);
    if (acknowledged) {
      channel.push(Buffer.concat([wire(ready), wire(acknowledgement)]));
      await completion;
      channel.destroy(error);
    } else if (source === "write") channel.push(wire(ready));
    else if (source === "stream") channel.destroy(error);
    await completion;
    await report.closeTransport(() => {});
    assert.equal(await readiness, acknowledged || source === "write" ? "ready" : "REPORT_CONTROL_ENDED");
    assert.equal(report.termination.report_outcome, acknowledged ? acknowledgement.outcome
      : source === "promise" && ownerExited ? "exited" : "write-failed");
    assert.equal(report.termination.report_cause, acknowledged ? undefined : error.code ?? "transport-error",
      "original source error must survive EOF and physical owner exit");
    assert.equal(report.termination.report_acknowledged, acknowledged);
    if (source !== "promise") assert.equal(report.termination.control_failure, error.code ?? "transport-error");
  } finally {
    channel.destroy();
  }
}

async function deadline(ownerExited) {
  const report = new ProcessReport();
  Object.assign(report, selected);
  const child = new EventEmitter();
  const channel = new Duplex({ read() {}, write(_chunk, _encoding, done) { done(); } });
  try {
    report.attach(child, channel);
    if (ownerExited) child.emit("exit", 0, null);
    // The original captured ACK is already too late for this admitted request.
    // A past deadline is deterministic; no wall-clock wait or clock patch.
    const completed = report.request(0);
    channel.push(Buffer.concat([wire(ready), wire(acknowledgement)]));
    await completed;
    assert.equal(report.termination.report_outcome, ownerExited ? "exited" : "unacknowledged");
    assert.equal(report.termination.report_cause, ownerExited ? undefined : "original-drain-deadline");
    assert.equal(report.termination.report_acknowledged, false);
    channel.push(null);
    await report.closeTransport(() => {});
  } finally {
    channel.destroy();
  }
}

async function partitions(bytes, selection, { late = false, ownerExited = false } = {}) {
  const results = [];
  for (const fragmented of [false, true]) {
    const report = new ProcessReport();
    const readiness = report.ready.then(() => "ready", error => {
      assert.ok(["PROCESS_STARTUP_REJECTED", "PROCESS_EXECUTION_FAILED", "REPORT_CONTROL_ENDED"].includes(error.code),
        "source loss must end readiness with its original typed failure");
      return error.code;
    });
    // Selection is trusted test setup, never derived from the untrusted frame.
    Object.assign(report, selection);
    const child = new EventEmitter();
    const channel = new Duplex({ read() {}, write(_chunk, _encoding, done) { done(); } });
    let delivery;
    try {
      report.attach(child, channel);
      if (ownerExited) child.emit("exit", 0, null);
      const completed = late ? undefined : report.request(Number.MAX_SAFE_INTEGER);
      delivery = (async () => {
        for (let offset = 0; offset < bytes.length;) {
          const end = fragmented ? Math.min(bytes.length, offset + 1 + bytes[offset]) : bytes.length;
          channel.push(bytes.subarray(offset, end));
          offset = end;
          await new Promise(setImmediate);
        }
        channel.push(null);
      })();
      // The actual report drain is the event. No clock, repeated readiness
      // query or parallel lifecycle model stands in for source completion.
      if (late) await report.closeTransport(() => {});
      await (completed ?? report.request(Number.MAX_SAFE_INTEGER));
      await report.closeTransport(() => {});
      const terminal = report.termination;
      report.expire();
      assert.deepEqual(report.termination, terminal, "a later deadline cannot replace original source settlement");
      results.push({ capability: report.capability, termination: report.termination, readiness: await readiness });
      assert.ok(report.termination.report_outcome, "source loss must settle the original request");
    } finally {
      await delivery;
      child.emit("exit", 0, null);
      channel.destroy();
    }
  }
  assert.deepEqual(results[1], results[0], "report admission must be independent of byte partitions");
  return results[0];
}
