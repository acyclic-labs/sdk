import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { chooseLanes, ignored, laneKeys } from "./plan-qualification.mjs";

const lanes = JSON.parse(readFileSync(".github/qualification-lanes.json", "utf8"));
const blob = (path, object = "a".repeat(40)) => `100644 blob ${object}\t${path}`;
const tree = [
  blob("Cargo.lock"),
  blob("rust/crates/stream/src/lib.rs"),
  blob("rust/crates/stream/README.md"),
  blob("typescript/packages/stream/src/index.ts"),
  blob("typescript/packages/filesystem/package.json"),
  blob("README.md"),
  blob(".github/workflows/publish-npm.yml"),
  blob(".github/workflows/qualification.yml"),
];
const changed = (path, object = "b".repeat(40)) =>
  tree.map(entry => (entry.endsWith(`\t${path}`) ? blob(path, object) : entry));
const differing = (before, after) =>
  Object.keys(before).filter(lane => before[lane] !== after[lane]).sort();

test("every lane names a known input set and a Blacksmith runner", () => {
  for (const lane of lanes) {
    assert.ok(ignored[lane.inputs], lane.lane);
    assert.match(lane.runner, /^blacksmith-/);
  }
});

test("the early-start windows job runs on the windows lane's runner", () => {
  const workflow = readFileSync(".github/workflows/qualification.yml", "utf8");
  const job = workflow.slice(workflow.indexOf("\n  windows:\n"));
  const runsOn = job.match(/\n {4}runs-on: (\S+)\n/)?.[1];
  const early = lanes.filter(lane => lane.early_start);
  assert.deepEqual(early.map(lane => lane.lane), ["windows"]);
  assert.equal(runsOn, early[0].runner);
  assert.match(job, new RegExp(`\\n {6}CARGO_BUILD_JOBS: ${early[0].workers}\\n`));
});

test("root documentation changes reuse every lane", () => {
  assert.deepEqual(differing(laneKeys(lanes, tree), laneKeys(lanes, changed("README.md"))), []);
});

test("crate documentation reaches rustdoc and every lane", () => {
  const before = laneKeys(lanes, tree);
  const after = laneKeys(lanes, changed("rust/crates/stream/README.md"));
  assert.deepEqual(differing(before, after), lanes.map(lane => lane.lane).sort());
});

test("TypeScript sources execute only TypeScript-observing lanes", () => {
  const before = laneKeys(lanes, tree);
  const after = laneKeys(lanes, changed("typescript/packages/stream/src/index.ts"));
  assert.deepEqual(differing(before, after), ["linux", "policy", "windows"]);
});

test("the filesystem package manifest reaches native binding lanes", () => {
  const before = laneKeys(lanes, tree);
  const after = laneKeys(lanes, changed("typescript/packages/filesystem/package.json"));
  assert.deepEqual(differing(before, after), lanes.map(lane => lane.lane).sort());
});

test("unrelated workflows reach only policy and repository lanes", () => {
  const before = laneKeys(lanes, tree);
  const after = laneKeys(lanes, changed(".github/workflows/publish-npm.yml"));
  assert.deepEqual(differing(before, after), ["linux", "policy"]);
});

test("the qualification workflow reaches every lane", () => {
  const before = laneKeys(lanes, tree);
  const after = laneKeys(lanes, changed(".github/workflows/qualification.yml"));
  assert.deepEqual(differing(before, after), lanes.map(lane => lane.lane).sort());
});

const source = { run_id: 7, run_attempt: 2 };
const everywhere = () => source;
const retainedAll = (runId, prefix) => `${prefix}-${runId}-2`;

test("recorded lanes are reused with their retained artifact", () => {
  const { matrix, reused } = chooseLanes(lanes, {
    force: false, mainPush: false, trusted: null, marker: everywhere, retained: retainedAll,
  });
  assert.deepEqual(matrix, []);
  assert.equal(reused.linux.artifact, "packages-linux-7-2");
  assert.equal(reused.web.artifact, "");
});

test("a lane whose artifact expired executes again", () => {
  const { matrix } = chooseLanes(lanes, {
    force: false, mainPush: false, trusted: null, marker: everywhere,
    retained: (runId, prefix) => (prefix === "coverage" ? "" : retainedAll(runId, prefix)),
  });
  assert.deepEqual(matrix.map(lane => lane.lane), ["gate"]);
});

test("main pushes rebuild source-bound artifacts even from a trusted pull request", () => {
  const { matrix, reused } = chooseLanes(lanes, {
    force: false, mainPush: true, trusted: source, marker: () => null, retained: retainedAll,
  });
  assert.deepEqual(matrix.map(lane => lane.lane), lanes.filter(lane => lane.source_bound).map(lane => lane.lane));
  assert.ok(!("linux" in reused));
  assert.deepEqual(reused.windows, { run_id: 7, run_attempt: 2, artifact: "" });
});

test("forced runs execute every lane", () => {
  const { matrix, reused } = chooseLanes(lanes, {
    force: true, mainPush: false, trusted: source, marker: everywhere, retained: retainedAll,
  });
  assert.equal(matrix.length, lanes.length);
  assert.deepEqual(reused, {});
});
