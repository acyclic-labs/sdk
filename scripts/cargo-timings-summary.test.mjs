import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";
import { readCargoTimings, summarizeCargoTimings } from "./cargo-timings-summary.mjs";

mkdirSync(".tools", { recursive: true });
const html = units => `<script>const UNIT_DATA = ${JSON.stringify(units)};
const CONCURRENCY_DATA = [{"t":10,"active":1,"waiting":0,"inactive":0}];
const CPU_USAGE = [[1,50]];</script>`;

test("Cargo timing summary retains separate builds, top units and unavailable binary phase boundaries", () => {
  const root = mkdtempSync(".tools/cargo-timings-");
  try {
    const units = Array.from({ length: 7 }, (_, i) => ({ name: "dep" + i, target: "", start: i, duration: i + 1, sections: null }));
    units.push({ name: "acyclic-plugin", target: ' bin "acyclic"', start: 9, duration: 800, sections: null });
    for (const build of ["product", "qualifier"]) {
      mkdirSync(join(root, build, "cargo-timings"), { recursive: true });
      writeFileSync(join(root, build, "cargo-timings", "cargo-timing.html"), html(units));
    }
    const output = join(root, "out");
    const reports = summarizeCargoTimings(output, [["product", join(root, "product")], ["qualifier", join(root, "qualifier")], ["install", join(root, "missing")]]);
    assert.equal(reports.length, 2);
    assert.deepEqual(reports[0].slowest.map(unit => unit.duration), [800, 7, 6, 5, 4]);
    assert.equal(reports[0].finalBinaryUnits[0].duration, 800);
    assert.equal(reports[0].finalBinaryUnits[0].sections, null);
    assert.deepEqual(reports[0].cpuUsage, [[1, 50]]);
    assert.equal(readFileSync(join(output, "product-cargo-timing.html"), "utf8"), html(units));
    assert.equal(JSON.parse(readFileSync(join(output, "qualifier-cargo-timings.json"), "utf8")).units.length, 8);
    assert.throws(() => summarizeCargoTimings(output, [["product", join(root, "missing")]]), /no Cargo timing reports/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("Cargo timings parse JSON without executing HTML and reject incomplete or invalid evidence", () => {
  assert.deepEqual(readCargoTimings(html([]) + '<script>throw new Error("must not execute");</script>').units, []);
  assert.throws(() => readCargoTimings("<script>const UNIT_DATA = [malicious()];</script>"));
  assert.throws(() => readCargoTimings(html([{ name: "bad", target: "", start: 0, duration: -1 }])), /invalid Cargo/);
  assert.throws(() => readCargoTimings(html([]).replace("CONCURRENCY_DATA", "MISSING")), /missing CONCURRENCY_DATA/);
});

test("Cargo timing CLI logs only observed phases and writes the uploaded JSON summary", () => {
  const root = mkdtempSync(".tools/cargo-timing-cli-");
  try {
    mkdirSync(join(root, "target", "cargo-timings"), { recursive: true });
    writeFileSync(join(root, "target", "cargo-timings", "cargo-timing.html"), html([
      { name: "acyclic-plugin", target: ' bin "acyclic"', start: 1, duration: 900,
        sections: [["codegen", { start: 20, end: 700 }], ["link", { start: 700, end: 890 }]] },
      { name: "dep", target: "", start: 0, duration: 5 },
    ]));
    const output = join(root, "out");
    const summary = join(root, "summary.md");
    const result = spawnSync(process.execPath, ["scripts/cargo-timings-summary.mjs", output, `product=${join(root, "target")}`],
      { encoding: "utf8", timeout: 60000, env: { ...process.env, GITHUB_STEP_SUMMARY: summary, SDK_SOURCE_SHA: "fixture-source", GITHUB_SHA: "workflow-source" } });
    assert.ifError(result.error);
    assert.equal(result.status, 0, result.stderr);
    assert.match(result.stdout, /reported codegen 680\.00s; reported link 190\.00s/);
    assert.match(result.stdout, /dep \| 5\.00s \| not reported \| not reported/);
    assert.equal(readFileSync(summary, "utf8"), result.stdout);
    assert.equal(JSON.parse(readFileSync(join(output, "product-cargo-timings.json"), "utf8")).finalBinaryUnits[0].duration, 900);
    assert.equal(JSON.parse(readFileSync(join(output, "product-cargo-timings.json"), "utf8")).source, "fixture-source");
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("a malformed report retains its HTML while later builds still produce JSON and logged summaries", () => {
  const root = mkdtempSync(".tools/cargo-timing-partial-");
  try {
    for (const build of ["product", "qualifier", "install"]) {
      mkdirSync(join(root, build, "cargo-timings"), { recursive: true });
      writeFileSync(join(root, build, "cargo-timings", "cargo-timing.html"), build === "qualifier"
        ? "<html>incomplete Cargo report</html>"
        : html([{ name: build, target: "", start: 0, duration: 25 }]));
    }
    const output = join(root, "out");
    const summary = join(root, "summary.md");
    const result = spawnSync(process.execPath, ["scripts/cargo-timings-summary.mjs", output,
      ...["product", "qualifier", "install"].map(build => `${build}=${join(root, build)}`)],
      { encoding: "utf8", timeout: 60000, env: { ...process.env, GITHUB_STEP_SUMMARY: summary } });
    assert.ifError(result.error);
    assert.equal(result.status, 1);
    assert.match(result.stderr, /qualifier: Cargo timings missing UNIT_DATA/);
    assert.equal(readFileSync(join(output, "qualifier-cargo-timing.html"), "utf8"), "<html>incomplete Cargo report</html>");
    for (const build of ["product", "install"]) {
      assert.equal(JSON.parse(readFileSync(join(output, `${build}-cargo-timings.json`), "utf8")).build, build);
      assert.ok(result.stdout.includes(`### ${build}`));
    }
    assert.equal(readFileSync(summary, "utf8"), result.stdout);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("pinned Cargo's real Windows ARM report selects the binary and preserves reported phase evidence", () => {
  const fixture = readFileSync("scripts/fixtures/cargo-timings/windows-arm64-1.98.1.html", "utf8");
  const pinned = readFileSync("rust-toolchain.toml", "utf8").match(/^channel = "([^"]+)"/m)?.[1]
    ?? assert.fail("missing pinned Cargo toolchain");
  assert.ok(fixture.includes(`rustc ${pinned} `), "refresh the real fixture when the pinned toolchain changes");
  const parsed = readCargoTimings(fixture);
  assert.equal(parsed.units.length, 5);
  assert.deepEqual(parsed.units.map(unit => [unit.name, unit.duration]), [
    ["acyclic-plugin", 283.35], ["acyclic-harness", 146.95], ["acyclic-fs", 86.28],
    ["acyclic-objects", 19.82], ["acyclic-stream", 13.62],
  ]);
  const root = mkdtempSync(".tools/real-cargo-report-");
  try {
    mkdirSync(join(root, "target", "cargo-timings"), { recursive: true });
    writeFileSync(join(root, "target", "cargo-timings", "cargo-timing.html"), fixture);
    const output = join(root, "out");
    const result = spawnSync(process.execPath, ["scripts/cargo-timings-summary.mjs", output, `product=${join(root, "target")}`],
      { encoding: "utf8", timeout: 60000, env: { ...process.env, GITHUB_STEP_SUMMARY: join(root, "summary.md") } });
    assert.ifError(result.error);
    assert.equal(result.status, 0, result.stderr);
    const report = JSON.parse(readFileSync(join(output, "product-cargo-timings.json"), "utf8"));
    assert.equal(report.finalBinaryUnits.length, 1);
    assert.equal(report.finalBinaryUnits[0].target, ' acyclic "bin"');
    assert.equal(report.finalBinaryUnits[0].duration, 283.35);
    assert.equal(report.finalBinaryUnits[0].sections, null);
    assert.match(result.stdout, /combined compilation\/codegen\/link 283\.35s; reported codegen not reported; reported link not reported/);
    assert.match(result.stdout, /acyclic-fs \| 86\.28s \| 0\.00s \| not reported/);
    assert.deepEqual(report.cpuUsage, [
      [6.0088253, 27.272727272727266], [6.1500796, 97.22222222222223],
      [563.0982845, 38.28125], [563.5992267, 32.8125],
    ]);
    assert.deepEqual(report.concurrency, [
      { t: 6.01, active: 4, waiting: 0, inactive: 407 },
      { t: 6.08, active: 5, waiting: 3, inactive: 403 },
      { t: 280.4, active: 1, waiting: 0, inactive: 0 },
      { t: 563.75, active: 0, waiting: 0, inactive: 0 },
    ]);
    assert.equal(readFileSync(join(output, "product-cargo-timing.html"), "utf8"), fixture);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
