import { readFileSync } from "node:fs";
import assert from "node:assert/strict";
import test from "node:test";
import { fuzz as lines } from "./lines.fuzz.mjs";
import { fuzz as report } from "./process-report.fuzz.mjs";

test("decoder fuzz targets retain captured native and shipping seeds across byte mutations", async () => {
  const ready = JSON.parse(readFileSync(new URL("./fixtures/native-report-ready.json", import.meta.url))).ready;
  const { termination } = JSON.parse(readFileSync(new URL("./fixtures/native-execution-failure.json", import.meta.url))).capture;
  const wire = value => Buffer.from(JSON.stringify(value) + "\n");
  const failed = wire({ generation: termination.generation, failed: termination.execution_failure });
  const shipping = JSON.parse(readFileSync(new URL("./fixtures/shipping-output.json", import.meta.url))).captures;
  const seeds = [Buffer.alloc(0), wire(ready), failed, ...Object.values(shipping).map(value => Buffer.from(value.stdout))];
  assert.equal((await report(wire(ready))).capability.ready, true);
  assert.deepEqual((await report(failed)).termination.execution_failure, termination.execution_failure);
  // Keep captured wire bytes intact in the corpus; truncation, invalid UTF-8
  // and inserted frame boundaries are generated mutations, not new fixtures.
  for (const seed of seeds) {
    for (const bytes of [seed, seed.subarray(0, seed.length >>> 1), Buffer.concat([seed, Buffer.from([255, 10, 123])])]) {
      lines(bytes);
      await report(bytes);
    }
  }
});
