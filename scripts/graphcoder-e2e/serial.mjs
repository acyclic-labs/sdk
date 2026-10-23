// A matrix is a collection of bounded case suites. Windows executes them serially.
import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { inventory, readCoverage } from "./coverage.mjs";
import { Processes } from "./process.mjs";

export function collect(cases, receipts, platform, coverage) {
  assert.equal(receipts.length, cases.length, "every case needs a suite receipt");
  const results = [], suites = [];
  for (let index = 0; index < cases.length; index++) {
    const test = cases[index], { code, report } = receipts[index];
    assert.equal(report.platform, platform, "suite OS mismatch");
    assert.deepEqual(report.selected, [test.id], "suite must select exactly its case");
    assert.equal(report.qualification, false, "selected suite is diagnostic");
    assert.deepEqual(report.coverage, coverage, "suite coverage changed during the matrix");
    const expected = [test.id, ...(platform === "win32" && test.ntfs ? [`${test.id}-ntfs`] : [])];
    assert.deepEqual(report.results.map(result => result.case), expected, "missing or duplicate filesystem variant");
    assert.ok(report.results.every(result => result.platform === platform), "case OS mismatch");
    assert.ok(Number.isFinite(report.seconds) && report.seconds > 0, "missing suite timing");
    assert.equal(report.limit_seconds, 10, "suite budget changed");
    suites.push({ case: test.id, seconds: report.seconds, code });
    results.push(...report.results);
  }
  return { platform, qualification: true, coverage, suites, results,
    pass: coverage.pass && suites.every(suite => suite.code === 0 && suite.seconds < 10)
      && results.every(result => result.status === "pass") };
}

async function main() {
  const [binary, mock, ...extra] = process.argv.slice(2);
  assert.ok(binary && mock && !extra.length, "usage: node serial.mjs GRAPHCODER MOCK_MODEL");
  const directory = dirname(fileURLToPath(import.meta.url));
  const output = resolve(process.env.E2E_OUTPUT ?? await mkdtemp(join(tmpdir(), "graphcoder-e2e-matrix-")));
  await mkdir(output, { recursive: true });
  const cases = inventory(), coverage = readCoverage(cases), receipts = [], processes = new Processes();
  const begin = performance.now();
  try {
    for (const test of cases) {
      const artifact = join(output, test.id);
      const result = await processes.run(process.execPath, [join(directory, "run.mjs"), resolve(binary), resolve(mock), test.id],
        { env: { ...process.env, E2E_OUTPUT: artifact } });
      await writeFile(join(output, `${test.id}.log`), result.stdout + "\n" + result.stderr);
      const report = JSON.parse(await readFile(join(artifact, "matrix.json"), "utf8"));
      receipts.push({ code: result.code, report });
      console.log(`${test.id}: ${report.results.map(result => result.status).join(",")} (${report.seconds.toFixed(3)}s)`);
    }
  } finally { await processes.close(); }
  const report = { ...collect(cases, receipts, process.platform, coverage), seconds: (performance.now() - begin) / 1000 };
  await writeFile(join(output, "matrix.json"), JSON.stringify(report, null, 2) + "\n");
  console.log(JSON.stringify(report, null, 2));
  process.exitCode = report.pass ? 0 : 1;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await main();
