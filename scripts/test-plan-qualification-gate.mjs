import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const lanes = JSON.parse(readFileSync(".github/qualification-lanes.json", "utf8"));

test("the workspace gate uses sixteen CPUs while short independent workspaces use two", () => {
  const matrix = lanes;
  const gate = matrix.find(lane => lane.lane === "gate");
  assert.equal(gate.runner, "blacksmith-16vcpu-ubuntu-2404");
  assert.equal(gate.workers, 16);
  assert.equal(gate.artifact, "");
  for (const name of ["docs", "generation"]) {
    const lane = matrix.find(lane => lane.lane === name);
    assert.equal(lane.runner, "blacksmith-2vcpu-ubuntu-2404");
    assert.equal(lane.workers, 2);
    assert.equal(lane.cache_size, "64M");
    assert.equal(lane.artifact, "");
    assert.ok(readFileSync(".github/actionlint.yaml", "utf8").includes(lane.runner));
  }
  const workflow = readFileSync(".github/workflows/qualification.yml", "utf8");
  assert.ok(workflow.includes("runs-on: ${{ matrix.runner }}"));
  for (const variable of ["CARGO_BUILD_JOBS", "CMAKE_BUILD_PARALLEL_LEVEL", "RAYON_NUM_THREADS", "RUST_TEST_THREADS"])
    assert.ok(workflow.includes(variable + ": ${{ matrix.workers }}"), variable);
  assert.ok(workflow.includes("MAKEFLAGS: -j${{ matrix.workers }}"), "MAKEFLAGS");
  assert.ok(readFileSync(".github/actionlint.yaml", "utf8").includes(gate.runner));
  const script = readFileSync("scripts/qualify-ci.sh", "utf8").replaceAll("\r\n", "\n");
  const body = script.match(/^  gate\)\n([\s\S]*?)(?=^  \S[^\n]*\)\n)/m)?.[1] ?? assert.fail("missing gate");
  assert.match(body, /^    sdk_workspace nextest --all-features --locked$/m);
});
