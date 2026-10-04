import assert from "node:assert/strict";
import { test } from "node:test";
import {
  loadManifest,
  runtimeEnvironment,
  safeEnvironment,
  validateManifest,
} from "./graphcoder-platform-gates.mjs";

test("platform manifest rejects altered source identity and duplicate gate IDs", () => {
  const manifest = loadManifest();
  const wrongBase = structuredClone(manifest);
  wrongBase.base_commit = "0".repeat(40);
  assert.throws(() => validateManifest(wrongBase), /base commit is invalid/u);

  const duplicate = structuredClone(manifest);
  duplicate.gates.push(structuredClone(duplicate.gates[0]));
  assert.throws(() => validateManifest(duplicate), /duplicate gate id/u);
});

test("platform manifest rejects artifact paths that escape the worktree", () => {
  const manifest = loadManifest();
  const traversal = structuredClone(manifest);
  traversal.gates[0].produced_artifacts = ["../outside.exe"];
  assert.throws(() => validateManifest(traversal), /relative paths without parent traversal/u);
});

test("platform execution environments exclude credentials and ambient settings", () => {
  const environment = {
    PATH: "tool-path",
    CARGO_HOME: "cargo-home",
    RANDOM_SETTING: "discard",
    AWS_SECRET_ACCESS_KEY: "discard",
    GRAPHCODER_OPERATOR_TOKEN: "discard",
  };
  assert.deepEqual(safeEnvironment(environment), {
    PATH: "tool-path",
    CARGO_HOME: "cargo-home",
  });
  assert.deepEqual(runtimeEnvironment(environment), { PATH: "tool-path" });
});

test("runtime policy preserves case-insensitive Windows PATH without widening the allowlist", () => {
  const filtered = runtimeEnvironment({ Path: "case-preserved", HOME: "discard" });
  assert.equal(filtered.Path, "case-preserved");
  if (process.platform !== "win32") assert.equal(filtered.PATH, "case-preserved");
  assert.equal(filtered.HOME, undefined);
});
