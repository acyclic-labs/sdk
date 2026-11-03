import { readFileSync } from "node:fs";
import assert from "node:assert/strict";
import test from "node:test";
import { fuzz as lines } from "./lines.fuzz.mjs";
import { fuzz as report } from "./process-report.fuzz.mjs";

test("decoder fuzz targets retain captured native and shipping seeds across byte mutations", async () => {
  const { ready, acknowledgement } = JSON.parse(readFileSync(new URL("./fixtures/native-report-ready.json", import.meta.url)));
  const { termination } = JSON.parse(readFileSync(new URL("./fixtures/native-execution-failure.json", import.meta.url))).capture;
  const wire = value => Buffer.from(JSON.stringify(value) + "\n");
  const failed = wire({ generation: termination.generation, failed: termination.execution_failure });
  const shipping = JSON.parse(readFileSync(new URL("./fixtures/shipping-output.json", import.meta.url))).captures;
  const acknowledged = wire(acknowledgement);
  const seeds = [Buffer.alloc(0), wire(ready), failed, acknowledged,
    ...Object.values(shipping).map(value => Buffer.from(value.stdout))];
  assert.equal((await report(wire(ready))).capability.ready, true);
  for (const field of ["generation", "request_id"]) {
    const foreign = { ...acknowledgement, [field]: "0".repeat(32) };
    const result = await report(wire(foreign));
    assert.equal(result.termination.report_acknowledged, false, "the first frame cannot choose its own owner");
    assert.equal(result.termination.control_failure, "foreign-or-invalid-acknowledgement");
  }
  assert.deepEqual((await report(failed, { generation: termination.generation, requestId: acknowledgement.request_id }))
    .termination.execution_failure, termination.execution_failure);
  for (const tail of [wire(ready).subarray(0, -2), Buffer.from("{\n")]) {
    const result = await report(Buffer.concat([acknowledged, tail]));
    assert.equal(result.termination.report_acknowledged, true);
    assert.equal(result.termination.control_failure, tail.at(-1) === 10
      ? "invalid-acknowledgement" : "unterminated-control-frame");
  }
  const afterAck = await report(Buffer.concat([acknowledged,
    wire({ generation: acknowledgement.generation, failed: termination.execution_failure }), Buffer.from("{\n")]));
  assert.deepEqual(afterAck.termination.execution_failure, termination.execution_failure);
  assert.equal(afterAck.termination.report_acknowledged, true);
  assert.equal(afterAck.termination.control_failure, "invalid-acknowledgement");
  for (const tail of [wire({ ...ready, generation: "0".repeat(32) }),
    wire({ ...acknowledgement, outcome: "invalid" })]) {
    const result = await report(Buffer.concat([acknowledged, tail]));
    assert.equal(result.termination.report_acknowledged, true);
    assert.equal(result.termination.control_failure, "foreign-or-invalid-acknowledgement");
  }
  // Keep captured wire bytes intact in the corpus; truncation, invalid UTF-8
  // and inserted frame boundaries are generated mutations, not new fixtures.
  for (const seed of seeds) {
    for (const bytes of [seed, seed.subarray(0, seed.length >>> 1), Buffer.concat([seed, Buffer.from([255, 10, 123])])]) {
      lines(bytes);
      await report(bytes);
    }
  }
});
