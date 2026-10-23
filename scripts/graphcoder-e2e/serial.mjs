// A matrix is a collection of bounded case suites. Windows executes them serially.
import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { inventory, readCoverage } from "./coverage.mjs";
import { Processes } from "./process.mjs";
import { publishReport } from "./report.mjs";

export function collect(cases, receipts, platform, coverage) {
  assert.equal(receipts.length, cases.length, "every case needs a suite receipt");
  const results = [], suites = [];
  for (let index = 0; index < cases.length; index++) {
    const test = cases[index], { code, report, error, seconds, execution } = receipts[index];
    const expected = [test.id, ...(platform === "win32" && test.ntfs ? [`${test.id}-ntfs`] : [])];
    try {
      assert.ok(report, error ?? "missing suite receipt");
      assert.equal(report.platform, platform, "suite OS mismatch");
      assert.deepEqual(report.selected, [test.id], "suite must select exactly its case");
      assert.equal(report.qualification, false, "selected suite is diagnostic");
      assert.deepEqual(report.coverage, coverage, "suite coverage changed during the matrix");
      assert.deepEqual(report.results.map(result => result.case), expected, "missing or duplicate filesystem variant");
      assert.ok(report.results.every(result => result.platform === platform), "case OS mismatch");
      assert.ok(Number.isFinite(report.seconds) && report.seconds > 0, "missing suite timing");
      assert.equal(report.limit_seconds, 10, "suite budget changed");
      suites.push({ case: test.id, seconds: report.seconds, code, execution });
      results.push(...report.results);
    } catch (failure) {
      suites.push({ case: test.id, seconds: seconds ?? null, code: code === 0 ? 1 : code,
        error: failure.stack, execution });
      results.push(...expected.map(id => ({ case: id, platform, status: "fail", error: failure.stack })));
    }
  }
  return { platform, qualification: true, coverage, suites, results,
    pass: coverage.pass && suites.every(suite => suite.code === 0 && suite.seconds < 10)
      && results.every(result => result.status === "pass") };
}

export async function runSuites(cases, run, read) {
  const receipts = [];
  for (const test of cases) {
    const start = performance.now();
    let result;
    try {
      result = await run(test);
      const report = await read(test);
      receipts.push({ code: result.code, report, execution: result,
        seconds: (performance.now() - start) / 1000 });
    } catch (error) {
      receipts.push({ code: result?.code ?? 1, report: null, error: error.stack,
        execution: result, seconds: (performance.now() - start) / 1000 });
    }
  }
  return receipts;
}

async function main() {
  const [binary, mock, ...extra] = process.argv.slice(2);
  assert.ok(binary && mock && !extra.length, "usage: node serial.mjs GRAPHCODER MOCK_MODEL");
  const directory = dirname(fileURLToPath(import.meta.url));
  const output = resolve(process.env.E2E_OUTPUT ?? await mkdtemp(join(tmpdir(), "graphcoder-e2e-matrix-")));
  await mkdir(output, { recursive: true });
  const cases = inventory(), coverage = readCoverage(cases), processes = new Processes();
  let receipts;
  const begin = performance.now();
  try {
    receipts = await runSuites(cases, async test => {
      const artifact = join(output, test.id);
      const result = await processes.run(process.execPath, [join(directory, "run.mjs"), resolve(binary), resolve(mock), test.id],
        { env: { ...process.env, E2E_OUTPUT: artifact } });
      console.log(`${test.id}: exit ${result.code}`);
      return result;
    }, async test => JSON.parse(await readFile(join(output, test.id, "matrix.json"), "utf8")));
  } finally { await processes.close(); }
  const report = { ...collect(cases, receipts, process.platform, coverage), seconds: (performance.now() - begin) / 1000 };
  await publishReport(join(output, "matrix.json"), report);
  process.exitCode = report.pass ? 0 : 1;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await main();
