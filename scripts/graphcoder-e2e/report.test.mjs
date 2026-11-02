import assert from "node:assert/strict";
import test from "node:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { failureReport, publishReport, verifyReport } from "./report.mjs";

test("request evidence failure still publishes original case failures and cannot qualify a pass", async () => {
  const root = await mkdtemp(join(tmpdir(), "request-evidence-"));
  try {
    let original;
    try { assert.equal("original result", "original expectation", "original operation"); }
    catch (error) { original = failureReport(error); }
    const cases = [{ case: "failed", status: "fail", ...original }, { case: "unchecked", status: "pass" }];
    const recovery = [process.execPath, "scripts/graphcoder-e2e/run.mjs", "GRAPHCODER", "MOCK_MODEL"];
    let verified = 0;
    const verify = result => { verified++; return result; };
    const requests = join(root, "requests.jsonl"), destination = join(root, "matrix.json");
    const report = await verifyReport(cases, () => readFile(requests, "utf8"), verify, recovery);
    assert.equal(verified, 0, "missing evidence cannot enter case assertions");
    assert.equal(report.requests, undefined, "unavailable evidence is not an empty request history");
    let published;
    await publishReport(destination, report, { publish: async text => { published = text; }, writeFile });
    const retained = JSON.parse(await readFile(destination, "utf8"));
    assert.equal(published, await readFile(destination, "utf8"));
    assert.equal(retained.results[0].error, original.error);
    assert.deepEqual(retained.results[0].assertion, original.assertion);
    const failure = retained.results[0].evidence_failure;
    assert.match(failure.error, /ENOENT/);
    assert.equal(failure.step, "case-evidence");
    assert.deepEqual(failure.recovery, recovery);
    assert.equal(retained.results[1].status, "fail");
    assert.equal(retained.results[1].phase, "evidence");
    assert.equal(retained.results[1].error, failure.error);
    // A readable, empty history remains distinct from an unavailable history.
    await writeFile(requests, "[]");
    const complete = await verifyReport(cases,
      async () => JSON.parse(await readFile(requests, "utf8")), verify, recovery);
    assert.equal(verified, cases.length);
    assert.deepEqual(complete.results, cases);
    assert.equal(complete.requests.size, 0);
    // Generated routing keys count work without inventing provider payloads.
    let visits = 0;
    const rows = cases.flatMap(({ case: id }) => Array.from({ length: 3 }, () => ({
      get case() { visits++; return id; },
    })));
    const grouped = await verifyReport(cases, () => rows, (result, ownRows) => {
      assert.equal(ownRows.length, 3);
      return result;
    }, recovery);
    assert.equal(visits, rows.length, "each request routes once, independently of the number of cases");
    for (const result of grouped.results) assert.equal(grouped.requests.get(result.case).length, 3);
  } finally { await rm(root, { recursive: true, force: true }); }
});

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
