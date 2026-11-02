import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { Duplex } from "node:stream";
import { ProcessReport } from "./process-report.mjs";

// Fuzz the actual stream/JSON/report decoder, including unterminated frames
// and source loss. This checks partition invariance, not native retirement.
export async function fuzz(data) {
  const bytes = Buffer.from(data), results = [];
  let selected;
  // Bind a recorded first frame to its original identifiers, so native corpus
  // seeds also reach accepted readiness and failure branches. All subsequent
  // bytes still go through the production decoder unchanged.
  let first;
  try { first = JSON.parse(bytes.toString("utf8").split("\n", 1)[0]); }
  catch (error) { if (!(error instanceof SyntaxError)) throw error; }
  for (const fragmented of [false, true]) {
    const report = new ProcessReport();
    if (selected) Object.assign(report, selected);
    else {
      if (typeof first?.generation === "string" && /^[0-9a-f]{32}$/.test(first.generation)) report.generation = first.generation;
      if (typeof first?.request_id === "string" && /^[0-9a-f]{32}$/.test(first.request_id)) report.requestId = first.request_id;
      selected = { generation: report.generation, requestId: report.requestId };
    }
    const child = new EventEmitter();
    const channel = new Duplex({ read() {}, write(_chunk, _encoding, done) { done(); } });
    try {
      report.attach(child, channel);
      const completed = report.request(Infinity);
      for (let offset = 0; offset < bytes.length;) {
        const end = fragmented ? Math.min(bytes.length, offset + 1 + bytes[offset]) : bytes.length;
        channel.push(bytes.subarray(offset, end));
        offset = end;
      }
      channel.push(null);
      await completed;
      results.push({ capability: report.capability, termination: report.termination });
      assert.ok(report.termination.report_outcome, "source loss must settle the original request");
    } finally {
      child.emit("exit", 0, null);
      channel.destroy();
    }
  }
  assert.deepEqual(results[1], results[0], "report admission must be independent of byte partitions");
  return results[0];
}
