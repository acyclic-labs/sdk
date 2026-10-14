import assert from "node:assert/strict";
import test from "node:test";
import { windowsRustTargets } from "./windows-rust-targets.mjs";

const metadata = {
  workspace_members: ["a", "b"],
  packages: [
    { id: "a", name: "a", targets: [
      { name: "a", kind: ["cdylib", "rlib"], test: true },
      { name: "cli", kind: ["bin"], test: true },
      { name: "consumer", kind: ["test"], test: true },
      { name: "compiled-example", kind: ["example"], test: false },
      { name: "build", kind: ["custom-build"], test: false },
    ] },
    { id: "b", name: "b", targets: [
      { name: "derive", kind: ["proc-macro"], test: true },
      { name: "tested-example", kind: ["example"], test: true },
    ] },
    { id: "registry", name: "dependency", targets: [{ name: "dep", kind: ["bench"], test: true }] },
  ],
};

test("Windows Rust target shards preserve workspace features and cover each test and example once", () => {
  const units = windowsRustTargets(metadata, "units");
  const scenarios = windowsRustTargets(metadata, "scenarios");
  assert.deepEqual(units.cargoArgs, ["--workspace", "--all-features", "--lib"]);
  assert.deepEqual(scenarios.cargoArgs, ["--workspace", "--all-features", "--bin", "cli", "--test", "consumer", "--example", "tested-example"]);
  assert.deepEqual(units.exampleBuildArgs, []);
  assert.deepEqual(scenarios.exampleBuildArgs, ["--workspace", "--all-features", "--examples"]);
  assert.deepEqual(units.targets, ["a/cdylib+rlib/a", "b/proc-macro/derive"]);
  assert.deepEqual(scenarios.targets, ["a/bin/cli", "a/example/compiled-example", "a/test/consumer", "b/example/tested-example"]);
  assert.equal(new Set([...units.targets, ...scenarios.targets]).size, 6);
  assert.deepEqual(windowsRustTargets({ ...metadata, packages: [...metadata.packages].reverse() }, "scenarios"), scenarios);
});

test("Windows Rust target shards reject unknown targets and ambiguous or empty inventories", () => {
  assert.throws(() => windowsRustTargets(metadata, "other"), /unknown/);
  assert.throws(() => windowsRustTargets({ ...metadata, workspace_members: ["a", "a"] }, "units"), /ambiguous/);
  assert.throws(() => windowsRustTargets({ ...metadata, packages: metadata.packages.slice(1) }, "units"), /ambiguous/);
  assert.throws(() => windowsRustTargets({ ...metadata, packages: [...metadata.packages, metadata.packages[0]] }, "units"), /ambiguous/);
  assert.throws(() => windowsRustTargets({ workspace_members: ["registry"], packages: metadata.packages }, "scenarios"), /unsupported/);
  assert.throws(() => windowsRustTargets({ workspace_members: [], packages: [] }, "units"), /empty/);
  const duplicate = structuredClone(metadata);
  duplicate.packages[0].targets.push(duplicate.packages[0].targets[0]);
  assert.throws(() => windowsRustTargets(duplicate, "units"), /duplicate/);
  const conflictingExample = structuredClone(metadata);
  conflictingExample.packages[0].targets.push({ name: "tested-example", kind: ["example"], test: false });
  assert.throws(() => windowsRustTargets(conflictingExample, "scenarios"), /share a name/);
  const conflictingTest = structuredClone(metadata);
  conflictingTest.packages[1].targets.push({ name: "consumer", kind: ["test"], test: false });
  assert.throws(() => windowsRustTargets(conflictingTest, "scenarios"), /share a name/);
  assert.throws(() => windowsRustTargets({ workspace_members: ["a"], packages: [{ id: "a", name: "a",
    targets: [{ name: "example", kind: ["example"], test: false }] }] }, "scenarios"), /empty scenario/);
});
