import assert from "node:assert/strict";
import test from "node:test";
import { resolve } from "node:path";
import { devDependencyCycles } from "./check-dev-dependency-cycles.mjs";

const dep = (name, kind = null, extra = {}) => ({ name, kind, path: resolve("fixtures", name), ...extra });
const pkg = (name, dependencies = []) => ({ name, id: name, manifest_path: resolve("fixtures", name, "Cargo.toml"), dependencies });

test("rejects the harness/providers negative control, including renamed target dependencies", () => {
  const harness = pkg("harness", [dep("providers", "dev", { rename: "adapter", target: "cfg(unix)" })]);
  const providers = pkg("providers", [dep("harness")]);
  assert.deepEqual(devDependencyCycles([harness, providers]), ["harness -> providers -> harness"]);
  harness.dependencies = [];
  assert.deepEqual(devDependencyCycles([harness, providers]), []);
});

test("rejects direct self dependencies and transitive optional build paths", () => {
  const packages = [pkg("a", [dep("a", "dev"), dep("b", "dev")]), pkg("b", [dep("c", "build", { optional: true })]), pkg("c", [dep("a")])];
  assert.deepEqual(devDependencyCycles(packages), ["a -> a", "a -> b -> c -> a"]);
});

test("accepts acyclic test composition and does not traverse another package's dev dependencies", () => {
  const packages = [pkg("a", [dep("b", "dev")]), pkg("b", [dep("c")]), pkg("c", [dep("a", "dev")])];
  assert.deepEqual(devDependencyCycles(packages), []);
});
