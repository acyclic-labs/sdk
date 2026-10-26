import assert from "node:assert/strict";
import test from "node:test";
import { packageClosure } from "./harness-package-closure.mjs";

const names = ["platform", "runtime", "filesystem", "harness"];
const packages = names.map((name, index) => ({
  name,
  version: "1.0.0",
  publish: null,
  dependencies: index === 0 ? [] : [{ name: names[index - 1], path: `/src/${names[index - 1]}`, kind: null }],
}));

test("all consumers use the transitive published dependency closure", () => {
  for (const selected of ["filesystem", "harness"]) {
    assert.deepEqual(packageClosure(packages, names, selected).map(item => item.name), names.slice(0, names.indexOf(selected) + 1));
  }
});

test("an omitted shared public dependency fails before packaging", () => {
  assert.throws(() => packageClosure(packages, names.slice(1), "filesystem"), /missing public package dependency: platform/);
});

test("private dev dependencies do not become registry artifacts", () => {
  const local = structuredClone(packages);
  local.at(-1).dependencies.push({ name: "fixtures", path: "/src/fixtures", kind: "dev" });
  local.push({ name: "fixtures", version: "0.0.0", publish: [], dependencies: [] });
  assert.deepEqual(packageClosure(local, names, "harness").map(item => item.name), names);
});

test("published dev dependencies remain available to package qualification", () => {
  const local = structuredClone(packages);
  local[2].dependencies.push({ name: "harness", path: "/src/harness", kind: "dev" });
  assert.deepEqual(packageClosure(local, names, "filesystem").map(item => item.name), names);
});
