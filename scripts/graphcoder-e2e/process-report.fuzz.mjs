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

// Fuzz the actual stream/JSON/report decoder, including unterminated frames
// and source loss. This checks partition invariance, not native retirement.
export async function fuzz(data, selection = selected) {
  const bytes = Buffer.from(data);
  const result = await partitions(bytes, selection);
  const protectedResult = await partitions(Buffer.concat([retained, bytes]), selected);
  assert.equal(protectedResult.capability.ready, true, "retain the original ready observation");
  assert.equal(protectedResult.termination.report_acknowledged, true, "later bytes cannot erase the captured ACK");
  assert.deepEqual(protectedResult.termination.execution_failure, failure, "later bytes cannot erase the original execution cause");
  assert.equal(protectedResult.termination.control_failure, "foreign-or-invalid-acknowledgement",
    "an ACK cannot authorize a later foreign frame or erase its first refusal");
  return result;
}

async function partitions(bytes, selection) {
  const results = [];
  for (const fragmented of [false, true]) {
    const report = new ProcessReport();
    // Selection is trusted test setup, never derived from the untrusted frame.
    Object.assign(report, selection);
    const child = new EventEmitter();
    const channel = new Duplex({ read() {}, write(_chunk, _encoding, done) { done(); } });
    let delivery;
    try {
      report.attach(child, channel);
      const completed = report.request(Number.MAX_SAFE_INTEGER);
      delivery = (async () => {
        for (let offset = 0; offset < bytes.length;) {
          const end = fragmented ? Math.min(bytes.length, offset + 1 + bytes[offset]) : bytes.length;
          channel.push(bytes.subarray(offset, end));
          offset = end;
          await new Promise(setImmediate);
        }
        channel.push(null);
      })();
      await completed;
      await report.closeTransport(() => {});
      results.push({ capability: report.capability, termination: report.termination });
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
