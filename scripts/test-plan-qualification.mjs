import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { chooseLanes, ignored, laneKeys, qualificationSchema } from "./plan-qualification.mjs";

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

test("the Windows helper waits for the planner and cannot provision on an ordinary PR", () => {
  const workflow = readFileSync(".github/workflows/qualification.yml", "utf8").replaceAll("\r\n", "\n");
  const job = workflow.slice(workflow.indexOf("\n  windows:\n"));
  const runsOn = job.match(/\n {4}runs-on: (\S+)\n/)?.[1];
  const windows = lanes.find(lane => lane.lane === "windows");
  assert.equal(runsOn, windows.runner);
  assert.match(job, /\n {4}needs: plan\n/);
  assert.match(job, /if: github\.event_name == 'pull_request' && needs\.plan\.outputs\.windows == 'true'/);
  assert.match(job, new RegExp(`\\n {6}CARGO_BUILD_JOBS: ${windows.workers}\\n`));
});

test("release events force the full downstream qualification path", () => {
  const workflow = readFileSync(".github/workflows/qualification.yml", "utf8").replaceAll("\r\n", "\n");
  assert.match(workflow, /\n  release:\n    types: \[published\]/);
  assert.match(workflow, /github\.event_name == 'release'/);
});

test("the PR lane split invalidates pre-gating qualification markers", () => {
  assert.equal(qualificationSchema, "sdk-qualification-v2");
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

test("pull requests qualify only the core gate and policy lanes", () => {
  const { matrix, reused } = chooseLanes(lanes, {
    force: false, mainPush: false, pullRequest: true, trusted: null,
    marker: () => null, retained: retainedAll,
  });
  assert.deepEqual(matrix.map(lane => lane.lane), ["gate", "policy"]);
  assert.deepEqual(reused, {});
});

test("a pull request never reuses a downstream lane as full qualification", () => {
  const { matrix, reused } = chooseLanes(lanes, {
    force: false, mainPush: false, pullRequest: true, trusted: source,
    marker: everywhere, retained: retainedAll,
  });
  assert.deepEqual(matrix, []);
  assert.deepEqual(Object.keys(reused).sort(), ["gate", "policy"]);
});

test("a lane whose artifact expired executes again", () => {
  const { matrix } = chooseLanes(lanes, {
    force: false, mainPush: false, trusted: null, marker: everywhere,
    retained: (runId, prefix) => (prefix === "coverage" ? "" : retainedAll(runId, prefix)),
  });
  assert.deepEqual(matrix.map(lane => lane.lane), ["gate"]);
});

test("main pushes rebuild source-bound artifacts and do not trust PRs for downstream lanes", () => {
  const { matrix, reused } = chooseLanes(lanes, {
    force: false, mainPush: true, trusted: source, marker: () => null, retained: retainedAll,
  });
  assert.deepEqual(
    matrix.map(lane => lane.lane),
    lanes.filter(lane => lane.source_bound || !["gate", "policy"].includes(lane.lane)).map(lane => lane.lane),
  );
  assert.deepEqual(Object.keys(reused).sort(), ["gate", "policy"]);
});

test("forced runs execute every lane", () => {
  const { matrix, reused } = chooseLanes(lanes, {
    force: true, mainPush: false, trusted: source, marker: everywhere, retained: retainedAll,
  });
  assert.equal(matrix.length, lanes.length);
  assert.deepEqual(reused, {});
});

// Routine pushes and non-forced dispatches must never schedule downstream
// packaging merely because their caches are empty.
test("routine core-only runs never schedule downstream checks without cache markers", () => {
  for (const mainPush of [false, true]) {
    const { matrix, reused } = chooseLanes(lanes, {
      force: false, mainPush, coreOnly: true, trusted: null,
      marker: () => null, retained: () => "",
    });
    assert.deepEqual(matrix.map(lane => lane.lane).sort(), ["gate", "policy"]);
    assert.deepEqual(reused, {});
  }
});

test("full hosted qualification requires release or explicit force", () => {
  const workflow = readFileSync(".github/workflows/qualification.yml", "utf8").replaceAll("\r\n", "\n");
  assert.doesNotMatch(workflow, /^  schedule:/m);
  assert.match(workflow, /force:[\s\S]*?default: false/);
  const downstream = workflow.slice(workflow.indexOf("\n  rust_source:"));
  assert.doesNotMatch(downstream, /if: github.event_name == 'release' \|\| github.event_name == 'workflow_dispatch'/);
  assert.match(downstream, /inputs\.force/);
});

test("reusable package qualification does not duplicate central release runs", () => {
  const names = ["additional-language-qualification.yml", "python-go-release-qualification.yml", "http-target-release-qualification.yml", "dotnet-native-rid-manual.yml", "embedded-abi-release.yml"];
  for (const name of names) {
    const workflow = readFileSync(`.github/workflows/${name}`, "utf8").replaceAll("\r\n", "\n");
    assert.doesNotMatch(workflow, /^  (release|push):/m, name);
    assert.match(workflow, /^  workflow_call:/m, name);
    assert.match(workflow, /^  workflow_dispatch:/m, name);
  }
});


test("native embedded consumers share one aggregate package build", () => {
  const workflow = readFileSync(".github/workflows/qualification.yml", "utf8").replaceAll("\r\n", "\n");
  assert.doesNotMatch(workflow, /^  (rust_embedded|dotnet_embedded):/m);
  assert.equal((workflow.match(/uses: \.\/\.github\/workflows\/embedded-native-packaging\.yml/g) ?? []).length, 1);
  const aggregate = readFileSync(".github/workflows/embedded-native-packaging.yml", "utf8");
  assert.match(aggregate, /qualify-embedded-abi-installed/);
  assert.match(aggregate, /abi-installed-consumer/);
});

test("routine checks avoid full coverage and workspace qualification", () => {
  const script = readFileSync("scripts/qualify-ci.sh", "utf8").replaceAll("\r\n", "\n");
  assert.match(script, /gate\)\n    if \[\[ "\$\{FORCE:-false\}" != true/);
  assert.match(script, /cargo test -p acyclic-sdk-contract-wire --locked --lib --bins/);
  assert.match(script, /coverage_instrumented.*false/);
  assert.match(script, /cargo clippy -p acyclic-sdk-contract-wire --all-targets --locked/);
  assert.match(script, /cargo llvm-cov --workspace --all-features/);
});
