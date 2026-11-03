import assert from "node:assert/strict";
import test from "node:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { failureReport, finishCase, publishReport, retainCapture, summarizeCases, verifyReport } from "./report.mjs";

test("case summaries retain every attempted row with one identity visit regardless of population", () => {
  // Generated internal report rows exercise accounting, not measured intervals.
  const names = Array.from({ length: 100_000 }, (_, index) => String(index));
  let visits = 0;
  const results = names.map((name, index) => ({
    get case() {
      assert.ok(++visits <= names.length, "summary scanned original identities again");
      return name;
    },
    status: index % 2 ? "fail" : "pass", seconds: index + 1,
  }));
  const cells = summarizeCases([...names, "not-run"], results);
  assert.equal(visits, results.length);
  assert.equal(cells.length, names.length + 1);
  for (const [index, cell] of cells.entries()) {
    const attempted = index < names.length, passed = attempted && index % 2 === 0;
    assert.deepEqual(cell, { case: attempted ? names[index] : "not-run", attempts: Number(attempted),
      n: Number(passed), failures: Number(attempted && !passed),
      p50_seconds: passed ? index + 1 : null, p99_seconds: passed ? index + 1 : null });
  }
});

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

test("failure reports retain arbitrary failures and operands omitted by the original assertion stack", async () => {
  // Generated thrown values exercise the shared reporter, not provider wire.
  const unknown = Object.create(null);
  Object.defineProperty(unknown, "stack", { enumerable: true, get() { assert.fail("diagnostic invoked an arbitrary getter"); } });
  Object.defineProperty(unknown, Symbol.toPrimitive, { value() { assert.fail("diagnostic coerced an arbitrary failure"); } });
  const inputs = [[undefined, "undefined"], [null, "null"], [false, "false"], [0, "0"],
    ["", "''"], [Symbol("callback refusal"), "Symbol(callback refusal)"], [unknown, "stack: [Getter]"]];
  const captures = [], pending = [];
  for (const [failure, representation] of inputs) {
    const report = failureReport(failure);
    assert.equal(typeof report.error, "string");
    assert.ok(report.error.includes(representation), "the original thrown value must survive diagnostic formatting");
    pending.push(retainCapture(Promise.reject(failure), captures));
  }
  const result = await finishCase({ case: "original", status: "pass" }, pending, ["repeat original case"]);
  assert.equal(result.status, "fail", "capture rejection cannot become an accepted case");
  assert.deepEqual(result.capture_failures, inputs.map(([failure]) => failureReport(failure)));
  assert.deepEqual(captures, [], "refused captures cannot publish a completed result");

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
  const check = async publish => {
    let published;
    await assert.rejects(publish("matrix.json", report, {
      publish: async text => { published = text; },
      writeFile: async () => { throw new Error("ENOTCONN: export snapshot unavailable"); },
    }), /ENOTCONN/);
    assert.equal(typeof published, "string", "original evidence must precede the failed export");
    assert.deepEqual(JSON.parse(published), report);
    const order = [];
    await publish("matrix.json", report, {
      publish: async text => order.push(["published", text]),
      writeFile: async (path, text) => order.push([path, text]),
    });
    assert.deepEqual(order.map(([event]) => event), ["published", "matrix.json"]);
    assert.equal(order[0][1], order[1][1]);
  };
  await check(publishReport);
  // Qualify the same oracle against the actual production order, rather than
  // restating a deleted write-first implementation in a separate example.
  const source = await readFile(new URL("./report.mjs", import.meta.url), "utf8");
  const before = "    await io.publish(text);\n    await io.writeFile(path, text);";
  assert.equal(source.split(before).length, 2, "production mutation site must be unique");
  const mutated = source.replace(before, "    await io.writeFile(path, text);\n    await io.publish(text);");
  const { publishReport: reordered } = await import("data:text/javascript," + encodeURIComponent(mutated));
  await assert.rejects(check(reordered), error => error.code === "ERR_ASSERTION"
    && error.message.includes("original evidence must precede the failed export"));
});

test("cancellation during either original report write retains a failed terminal snapshot", async () => {
  const root = await mkdtemp(join(tmpdir(), "report-cancellation-"));
  try {
    for (const boundary of ["before", "publish", "file", "none"]) {
      const signal = new AbortController(), report = { pass: true, results: [] };
      const snapshots = [], path = join(root, boundary + ".json");
      const reason = boundary === "file" ? Object.create(null) : new Error("caller cancelled at " + boundary);
      if (boundary === "before") Object.defineProperty(reason, "cause", { get() { assert.fail("cancellation must not inspect an unrelated cause"); } });
      if (boundary === "before") signal.abort(reason);
      const io = {
        publish: async text => {
          await writeFile(join(root, boundary + ".stdout"), text, { flag: "a" });
          snapshots.push(JSON.parse(text));
          if (boundary === "publish") signal.abort(reason);
        },
        writeFile: async (path, text) => {
          await writeFile(path, text);
          if (boundary === "file") signal.abort(reason);
        },
      };
      await publishReport(path, report, io, signal.signal);
      const expected = boundary === "none";
      assert.equal(report.pass, expected);
      assert.deepEqual(JSON.parse(await readFile(path, "utf8")), report);
      assert.deepEqual(snapshots.at(-1), report);
      assert.equal(snapshots.length, ["publish", "file"].includes(boundary) ? 2 : 1);
      if (!expected) assert.match(report.cancellation, boundary === "file" ? /null prototype/ : /caller cancelled at/);
      if (snapshots.length === 2) assert.equal(snapshots[0].pass, true);
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
