import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import {
  loadManifest,
  runtimeEnvironment,
  safeEnvironment,
  readLaneReceipt,
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

test("qualification lanes require real installed drivers and keep mock fixtures out of native evidence", () => {
  const manifest = loadManifest();
  assert.deepEqual(manifest.qualification_lanes.map(lane => lane.id), [
    "installed-native-stage",
    "installed-headless",
    "installed-pty",
    "installed-transport-faults",
    "filesystem-plugin-ignored",
    "generated-contracts",
  ]);
  const native = manifest.qualification_lanes.find(lane => lane.id === "installed-native-stage");
  assert.equal(native.runtime_artifact_by_platform.windows, "target/debug/graphcoder-runtime.exe");
  assert.equal(native.allows_mock_fixture, false);
  const pty = manifest.qualification_lanes.find(lane => lane.id === "installed-pty");
  assert.equal(pty.requires_approval_cancellation_writeback, true);
  assert.equal(pty.platforms.length, 1);
  const filesystem = manifest.qualification_lanes.find(lane => lane.id === "filesystem-plugin-ignored");
  assert.equal(filesystem.requires_ignored_tests, true);
  assert.deepEqual(filesystem.skip_patterns, ["support::"]);

  const mock = structuredClone(manifest);
  mock.qualification_lanes[0].allows_mock_fixture = true;
  assert.throws(() => validateManifest(mock), /cannot allow a mock fixture/u);
  const missing = structuredClone(manifest);
  missing.qualification_lanes[0].driver = "scripts/missing-driver.mjs";
  assert.throws(() => validateManifest(missing), /does not exist/u);
});

test("installed lane receipts cannot claim completion from stale, failed, or empty evidence", () => {
  const manifest = loadManifest();
  const lane = manifest.qualification_lanes.find(item => item.id === "installed-native-stage");
  assert.equal(typeof lane.receipt_environment, "string");
  const source = { commit: "a".repeat(40), tree: "b".repeat(40) };
  assert.throws(() => validateManifest({ ...manifest, qualification_lanes: [{ ...lane, receipt_environment: "bad-name" }] }), /receipt environment is invalid/u);
  assert.equal(source.commit.length, 40);
});

test("lane receipt validation rejects missing, skipped, flaky, empty, and cross-source evidence", () => {
  const root = mkdtempSync(join(tmpdir(), "graphcoder-lane-receipt-"));
  const lane = loadManifest().qualification_lanes.find(item => item.id === "installed-native-stage");
  const source = { commit: "a".repeat(40), tree: "b".repeat(40) };
  assert.throws(() => readLaneReceipt(join(root, "missing.json"), lane, "native", source, "windows"), /receipt is missing/u);
  const descriptorPath = join(root, "descriptor.json");
  const baseDescriptor = {
    protocol: "acyclic.graphcoder.suite-descriptor.v1",
    source_commit: source.commit,
    source_tree: source.tree,
    command: { executable: "node", args: [lane.driver] },
  };
  writeFileSync(descriptorPath, `${JSON.stringify(baseDescriptor)}\n`);
  const makeReceipt = (status, artifacts = [{ source_commit: source.commit, source_tree: source.tree, fresh: true }]) => ({
    suite: { status, execution_kind: "native", platform: "windows", id: "native-stage" , descriptor_path: descriptorPath },
    artifacts,
  });
  for (const status of ["skipped", "flaky", "failed"]) {
    const path = join(root, `${status}.json`);
    writeFileSync(path, `${JSON.stringify(makeReceipt(status))}\n`);
    assert.throws(() => readLaneReceipt(path, lane, "native", source, "windows"), /receipt is not passed/u);
  }
  const emptyPath = join(root, "empty.json");
  writeFileSync(emptyPath, `${JSON.stringify(makeReceipt("passed", []))}\n`);
  assert.throws(() => readLaneReceipt(emptyPath, lane, "native", source, "windows"), /no artifact evidence/u);
  const stalePath = join(root, "stale.json");
  writeFileSync(stalePath, `${JSON.stringify(makeReceipt("passed", [{ source_commit: "c".repeat(40), source_tree: source.tree, fresh: true }]))}\n`);
  assert.throws(() => readLaneReceipt(stalePath, lane, "native", source, "windows"), /provenance is stale/u);
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
