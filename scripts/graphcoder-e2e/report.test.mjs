import assert from "node:assert/strict";
import test from "node:test";
import { failureReport, publishReport } from "./report.mjs";

test("failure reports retain operands omitted by the original assertion stack", async () => {
  const actual = { bytes: "a".repeat(20_000), tail: "original actual tail", unknown: undefined,
    count: 2n, get untouched() { throw new Error("diagnostic invoked a getter"); } };
  actual.self = actual;
  let failure;
  try { assert.deepEqual(actual, { bytes: "b".repeat(20_000), tail: "original expected tail" }); }
  catch (error) { failure = error; }
  assert.ok(failure instanceof assert.AssertionError);
  assert.ok(!failure.stack.includes(actual.bytes), "original stack is the truncation negative control");
  const report = failureReport(failure);
  let published;
  await publishReport("matrix.json", { results: [report] }, {
    publish: async text => { published = JSON.parse(text); }, writeFile: async () => {},
  });
  const retained = published.results[0];
  assert.equal(retained.error, failure.stack);
  assert.ok(retained.assertion, "original assertion operands must survive publication");
  assert.equal(retained.assertion.operator, failure.operator);
  assert.ok(retained.assertion.actual.includes(actual.bytes));
  assert.ok(retained.assertion.actual.includes(actual.tail));
  assert.match(retained.assertion.actual, /unknown: undefined/);
  assert.match(retained.assertion.actual, /count: 2n/);
  assert.match(retained.assertion.actual, /\[Circular/);
  assert.ok(retained.assertion.expected.includes("b".repeat(20_000)));
  assert.ok(retained.assertion.expected.includes("original expected tail"));
});

test("complete evidence is published before failed export can release its snapshot", async () => {
  const report = { results: [{ case: "host", status: "fail",
    evidence: { captures: [{ stdout: "real host output", stderr: "native failure", code: 101 }],
      requests: [{ sequence: 1, path: "/actual/request" }] } }] };
  let published;
  const io = { publish: async text => { published = text; },
    writeFile: async () => { throw new Error("ENOTCONN: export snapshot unavailable"); } };
  await assert.rejects(publishReport("matrix.json", report, io), /ENOTCONN/);
  assert.deepEqual(JSON.parse(published), report);
  // Old write-first ordering loses every byte when the export rejects.
  published = undefined;
  await assert.rejects((async () => {
    await io.writeFile("matrix.json", JSON.stringify(report));
    await io.publish(JSON.stringify(report));
  })(), /ENOTCONN/);
  assert.equal(published, undefined);
  const order = [];
  await publishReport("matrix.json", report, {
    publish: async text => order.push(["published", text]),
    writeFile: async (path, text) => order.push([path, text]),
  });
  assert.deepEqual(order.map(([event]) => event), ["published", "matrix.json"]);
  assert.equal(order[0][1], order[1][1]);
});
