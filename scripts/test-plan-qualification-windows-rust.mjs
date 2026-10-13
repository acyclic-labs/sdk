import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { chooseLanes } from "./plan-qualification.mjs";

const lanes = JSON.parse(readFileSync(".github/qualification-lanes.json", "utf8"));

test("Windows Rust tests run independently without changing workspace or ignored-test selection", () => {
  const matrix = chooseLanes(lanes, {
    mainPush: false, force: false, fullQualification: true,
    marker: () => ({ run_id: 1, run_attempt: 1, source_commit: "a".repeat(40) }),
    retained: () => "retained",
  }).matrix;
  const rust = matrix.find(lane => lane.lane === "windows-rust");
  assert.equal(rust.runner, "blacksmith-16vcpu-windows-2025");
  assert.equal(rust.workers, 16);
  assert.equal(rust.source_bound, true);
  assert.equal(rust.scope, "full");
  assert.equal(rust.inputs, "rust");
  assert.equal(rust.artifact, "");
  assert.equal(matrix.find(lane => lane.lane === "windows").artifact, "stream-native-windows");
  const script = readFileSync("scripts/qualify-ci.ps1", "utf8");
  const body = script.split("    'windows-rust' {")[1]?.split("    'windows-napi' {")[0] ?? assert.fail("missing Rust dispatch");
  assert.ok(body.includes("cargo test --workspace --all-features --locked"));
  assert.ok(body.includes("cargo test -p acyclic-plugin --all-features --locked --test fork_join --"));
  assert.ok(body.includes("--ignored --test-threads=1 --skip support::"));
  assert.ok(body.includes("Remove-Item Env:RUSTC_WRAPPER"));
  assert.ok(body.includes("finally"));
  const packages = script.slice(script.indexOf("& .\\scripts\\test-qualify-ci-windows.ps1"));
  assert.doesNotMatch(packages, /cargo test/);
  assert.ok(packages.includes("bun test --parallel=4"));
  const workflow = readFileSync(".github/workflows/qualification.yml", "utf8");
  for (const marker of ["Find windows-rust marker", "Record windows-rust"])
    assert.ok(workflow.includes(marker));
  assert.ok(workflow.includes("max-parallel: 17"));
  const full = chooseLanes(lanes, { mainPush: false, force: true, marker: () => null, retained: () => "" }).matrix;
  assert.ok(full.length <= 17);
  const action = readFileSync(".github/actions/qualification-lane/action.yml", "utf8");
  assert.ok(action.includes("inputs.lane == 'windows-rust'"));
  const projfs = action.slice(action.indexOf("    - name: Enable Windows ProjFS"), action.indexOf("    - name: Run qualification lane"));
  assert.ok(projfs.includes("if: runner.os == 'Windows'"));
});
