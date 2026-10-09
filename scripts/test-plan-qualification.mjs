import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  chooseLanes,
  classifyQualificationEvent,
  ignored,
  laneKeys,
  qualificationEventKinds,
  requiresFullQualification,
} from "./plan-qualification.mjs";

const lanes = JSON.parse(readFileSync(".github/qualification-lanes.json", "utf8"));
const blob = (path, object = "a".repeat(40)) => `100644 blob ${object}\t${path}`;
const tree = [
  blob("Cargo.lock"),
  blob("rust/crates/stream/src/lib.rs"),
  blob("rust/crates/stream/README.md"),
  blob("typescript/packages/stream/src/index.ts"),
  blob("typescript/packages/filesystem/package.json"),
  blob("rust/crates/harness-codex/src/lib.rs"),
  blob("rust/crates/harness/tests/effects.rs"),
  blob("rust/crates/sdk-docs/src/lib.rs"),
  blob("rust/crates/sdk-generation/src/main.rs"),
  blob("rust/crates/sdk-generation/scripts/qualify-typescript-snippets.mjs"),
  blob("README.md"),
  blob(".github/workflows/publish-npm.yml"),
  blob(".github/workflows/qualification.yml"),
  blob("tools/sdk-generator/backends/go/generate.go"),
  blob("tools/sdk-generator/backends/java/generate.mjs"),
  blob("tools/sdk-generator/backends/future/generate.mjs"),
];
const changed = (path, object = "b".repeat(40)) =>
  tree.map(entry => (entry.endsWith(`\t${path}`) ? blob(path, object) : entry));
const differing = (before, after) =>
  Object.keys(before).filter(lane => before[lane] !== after[lane]).sort();
const named = lanes => lanes.map(lane => lane.lane).sort();
const fullLanes = lanes.filter(lane => lane.scope !== "core");

test("every lane names a known input set and a Blacksmith runner", () => {
  for (const lane of lanes) {
    assert.ok(ignored[lane.inputs], lane.lane);
    assert.match(lane.runner, /^blacksmith-/);
  }
});

test("root documentation changes reuse every lane", () => {
  assert.deepEqual(differing(laneKeys(lanes, tree), laneKeys(lanes, changed("README.md"))), []);
});

test("full qualification has a separate cache identity from the core gate", () => {
  const core = laneKeys(lanes, tree, "core");
  const full = laneKeys(lanes, tree, "full");
  assert.deepEqual(differing(core, full), lanes.map(lane => lane.lane).sort());
});

test("crate documentation reaches rustdoc and every lane", () => {
  const before = laneKeys(lanes, tree);
  const after = laneKeys(lanes, changed("rust/crates/stream/README.md"));
  assert.deepEqual(differing(before, after), lanes.map(lane => lane.lane).sort());
});

test("TypeScript sources execute only TypeScript-observing lanes", () => {
  const before = laneKeys(lanes, tree);
  const after = laneKeys(lanes, changed("typescript/packages/stream/src/index.ts"));
  assert.deepEqual(differing(before, after), ["linux", "macos", "policy", "typescript", "windows"]);
});

test("Rust that no package compiles skips the TypeScript lane", () => {
  const before = laneKeys(lanes, tree);
  for (const path of [
    "rust/crates/harness-codex/src/lib.rs",
    "rust/crates/harness/tests/effects.rs",
    "rust/crates/sdk-docs/src/lib.rs",
    "rust/crates/sdk-generation/src/main.rs",
    "rust/crates/sdk-generation/scripts/qualify-typescript-snippets.mjs",
  ]) {
    const after = differing(before, laneKeys(lanes, changed(path)));
    assert.ok(after.includes("gate") && !after.includes("typescript"), path);
  }
  const wasm = differing(before, laneKeys(lanes, changed("rust/crates/stream/src/lib.rs")));
  assert.ok(wasm.includes("typescript"));
});

test("the filesystem package manifest reaches native binding lanes", () => {
  const before = laneKeys(lanes, tree);
  const after = laneKeys(lanes, changed("typescript/packages/filesystem/package.json"));
  assert.deepEqual(differing(before, after), lanes.map(lane => lane.lane).sort());
});

test("unrelated workflows reach only policy and repository lanes", () => {
  const before = laneKeys(lanes, tree);
  const after = laneKeys(lanes, changed(".github/workflows/publish-npm.yml"));
  assert.deepEqual(differing(before, after), ["linux", "macos", "policy"]);
});

test("isolated language generator changes reuse Rust and TypeScript builds", () => {
  const before = laneKeys(lanes, tree);
  for (const path of ["tools/sdk-generator/backends/go/generate.go", "tools/sdk-generator/backends/java/generate.mjs"]) {
    const after = laneKeys(lanes, changed(path));
    assert.deepEqual(differing(before, after), ["linux", "macos", "policy"]);
  }
});

test("unregistered generators retain full qualification inputs", () => {
  const before = laneKeys(lanes, tree);
  const after = laneKeys(lanes, changed("tools/sdk-generator/backends/future/generate.mjs"));
  assert.deepEqual(differing(before, after), lanes.map(lane => lane.lane).sort());
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
    force: false, mainPush: false, marker: everywhere, retained: retainedAll,
  });
  assert.deepEqual(matrix, []);
  assert.equal(reused.linux.artifact, "packages-linux-7-2");
  assert.equal(reused.web.artifact, "");
});

test("full qualification rebuilds source-bound packages after a README-only change", () => {
  const before = laneKeys(lanes, tree, "full");
  const after = laneKeys(lanes, changed("README.md"), "full");
  assert.deepEqual(before, after);

  const { matrix, reused } = chooseLanes(lanes, {
    force: false,
    mainPush: false,
    fullQualification: true,
   
    marker: everywhere,
    retained: retainedAll,
  });
  assert.deepEqual(matrix.map(lane => lane.lane), ["linux", "windows", "macos"]);
  assert.equal(reused.linux, undefined);
  assert.equal(reused.gate.run_id, source.run_id);
});

test("a lane whose artifact expired executes again", () => {
  const { matrix } = chooseLanes(lanes, {
    force: false, mainPush: false, marker: everywhere,
    retained: (runId, prefix) => (prefix === "coverage" ? "" : retainedAll(runId, prefix)),
  });
  assert.deepEqual(matrix.map(lane => lane.lane), ["gate"]);
});

test("ordinary main pushes keep downstream qualification off the routine path", () => {
  const { matrix, reused } = chooseLanes(lanes, {
    force: false, mainPush: true, marker: () => null, retained: retainedAll,
    coreOnly: true,
  });
  assert.deepEqual(named(matrix), ["gate", "policy", "typescript"]);
  assert.deepEqual(reused, {});
});

test("forced runs execute every full lane", () => {
  const { matrix, reused } = chooseLanes(lanes, {
    force: true, mainPush: false, marker: everywhere, retained: retainedAll,
  });
  assert.deepEqual(named(matrix), named(fullLanes));
  assert.deepEqual(reused, {});
});

test("forced reusable release calls keep the full lane scope", () => {
  const event = classifyQualificationEvent({
    eventName: "push",
    ref: "refs/tags/acyclic-v1.2.3",
    force: true,
  });
  assert.equal(event, qualificationEventKinds.forcedDispatch);
  assert.equal(requiresFullQualification(event), true);

  const { matrix, reused } = chooseLanes(lanes, {
    force: true,
    mainPush: false,
    coreOnly: true,
   
    marker: everywhere,
    retained: retainedAll,
  });
  assert.deepEqual(named(matrix), named(fullLanes));
  assert.deepEqual(reused, {});
});

test("routine pull requests qualify only the core lanes", () => {
  const { matrix } = chooseLanes(lanes, {
    force: false,
    mainPush: false,
    pullRequest: true,
    coreOnly: true,
   
    marker: () => null,
    retained: () => "",
  });
  assert.deepEqual(named(matrix), ["gate", "policy", "typescript"]);
});

test("release, manual, and scheduled events are full qualification events", () => {
  /** @type {Array<[{ eventName: string, force?: boolean }, string]>} */
  const cases = [
    [{ eventName: "release" }, qualificationEventKinds.release],
    [{ eventName: "workflow_dispatch", force: false }, qualificationEventKinds.manual],
    [{ eventName: "schedule" }, qualificationEventKinds.schedule],
  ];
  for (const [input, expected] of cases) {
    const event = classifyQualificationEvent(input);
    assert.equal(event, expected);
    assert.equal(requiresFullQualification(event), true);
  }
});

test("the workflow keeps full qualification off routine pull requests", () => {
  const workflow = readFileSync(".github/workflows/qualification.yml", "utf8").replaceAll("\r\n", "\n");
  assert.match(workflow, /^  release:/m);
  assert.doesNotMatch(workflow, /^  schedule:/m);
  assert.match(workflow, /default: false/);
  assert.match(workflow, /github\.event_name == 'release' && github\.event\.release\.tag_name/);
  for (const { lane } of lanes) {
    assert.match(workflow, new RegExp(`- name: Find ${lane} marker\\n`), lane);
    assert.match(workflow, new RegExp(`- name: Record ${lane}\\n`), lane);
  }
});

test("the core lanes cover the Rust workspace, docs crate, and TypeScript workspace", () => {
  const script = readFileSync("scripts/qualify-ci.sh", "utf8").replaceAll("\r\n", "\n");
  assert.match(script, /^ +nextest --workspace --all-features --locked$/m);
  assert.match(script, /cargo test --manifest-path rust\/crates\/sdk-docs\/Cargo\.toml --locked/);
  assert.match(script, /cargo test --manifest-path rust\/crates\/sdk-generation\/Cargo\.toml --locked/);
  assert.match(script, /node --test rust\/crates\/sdk-generation\/scripts\/test-qualify-typescript-snippets\.mjs/);
  const policy = script.slice(script.indexOf("\n  policy)"));
  assert.match(policy, /^ +cargo clippy --workspace --all-targets --all-features --locked -- -D warnings\n +node --test/m);
  const typescript = script.slice(script.indexOf("\n  typescript)"));
  assert.match(typescript, /bun run test\n(?: .*\n)+? +bun run check:generated\n/);
});
