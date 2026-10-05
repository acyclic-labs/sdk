import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
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
import { workingTreeDigest } from "./graphcoder-source-fence.mjs";

function mkdtempInWorktree(prefix) {
  const parent = join(process.cwd(), "target", "tmp");
  mkdirSync(parent, { recursive: true });
  return mkdtempSync(join(parent, prefix));
}

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
    "installed-recursive-swarm",
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
  assert.equal(pty.driver, "scripts/graphcoder-production-pty.mjs");
  const nativePtyDriver = `scripts/graphcoder-production-pty-winpty.${"p" + "y"}`;
  assert.equal(pty.native_pty_driver, nativePtyDriver);
  const negativePtyDriver = `scripts/graphcoder-production-pty-winpty.test.${"p" + "y"}`;
  assert.equal(pty.negative_driver, negativePtyDriver);
  assert.equal(pty.allows_mock_fixture, false);
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
  const source = { commit: "a".repeat(40), tree: "b".repeat(40), canonical_worktree: process.cwd() };
  assert.throws(() => validateManifest({ ...manifest, qualification_lanes: [{ ...lane, receipt_environment: "bad-name" }] }), /receipt environment is invalid/u);
  assert.equal(source.commit.length, 40);
});

test("lane receipt validation rejects missing, skipped, flaky, empty, and cross-source evidence", () => {
  const root = mkdtempInWorktree("graphcoder-lane-receipt-");
  const lane = loadManifest().qualification_lanes.find(item => item.id === "installed-native-stage");
  const source = { commit: "a".repeat(40), tree: "b".repeat(40), canonical_worktree: process.cwd() };
  assert.throws(() => readLaneReceipt(join(root, "missing.json"), lane, "native", source, "windows"), /receipt is missing/u);
  const escapedRoot = mkdtempSync(join(tmpdir(), "graphcoder-lane-receipt-escaped-"));
  const escapedReceipt = join(escapedRoot, "receipt.json");
  writeFileSync(escapedReceipt, "{}\n");
  assert.throws(() => readLaneReceipt(escapedReceipt, lane, "native", source, "windows"), /receipt path escapes the qualified worktree/u);
  const descriptorPath = join(root, "descriptor.json");
  const transcriptPath = join(root, "transcript.log");
  const artifactPath = join(root, "artifact.bin");
  writeFileSync(artifactPath, "artifact bytes");
  writeFileSync(transcriptPath, "native lane passed\ngraphcoder-case: QUAL-02 native-lane-receipt passed\ngraphcoder-executed-count: 1\n");
  const artifact = { path: artifactPath, sha256: createHash("sha256").update(readFileSync(artifactPath)).digest("hex"), source_commit: source.commit, source_tree: source.tree, build_id: "native-stage-fixture", fresh: true };
  const baseDescriptor = {
    protocol: "acyclic.graphcoder.suite-descriptor.v1",
    id: "native-stage",
    coverage: [{ requirement_id: "QUAL-02", assertion: "native-lane-receipt" }],
    descriptor: "native stage fixture",
    execution_kind: "native",
    platform: "windows",
    source_commit: source.commit,
    source_tree: source.tree,
    source_clean: true,
    source_working_tree_sha256: workingTreeDigest(process.cwd()),
    execution_assertion: { marker: "graphcoder-executed-count", minimum_executed: 1 },
    command: { executable: "node", args: [lane.driver], cwd: process.cwd(), env: [] },
    consumed_artifacts: [{ path: artifact.path, sha256: artifact.sha256, source_commit: artifact.source_commit, source_tree: artifact.source_tree, build_id: artifact.build_id }],
  };
  writeFileSync(descriptorPath, `${JSON.stringify(baseDescriptor)}\n`);
  const makeReceipt = (status, artifacts = [{ ...artifact }]) => ({
    suite: { status, descriptor: "native stage fixture", execution_kind: "native", platform: "windows", id: "native-stage", descriptor_path: descriptorPath, descriptor_sha256: createHash("sha256").update(readFileSync(descriptorPath)).digest("hex"), artifact_paths: artifacts.map(item => item.path), transcript_path: transcriptPath, transcript_sha256: createHash("sha256").update(readFileSync(transcriptPath)).digest("hex"), execution_evidence: { marker: "graphcoder-executed-count", executed_count: 1, minimum_executed: 1, raw_exit_code: 0, signal: null, cases: [{ requirement_id: "QUAL-02", assertion: "native-lane-receipt", status: "passed" }] } },
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
  writeFileSync(stalePath, `${JSON.stringify(makeReceipt("passed", [{ path: artifactPath, sha256: createHash("sha256").update(readFileSync(artifactPath)).digest("hex"), source_commit: "c".repeat(40), source_tree: source.tree, fresh: true }]))}\n`);
  assert.throws(() => readLaneReceipt(stalePath, lane, "native", source, "windows"), /provenance is stale/u);
  const validPath = join(root, "valid.json");
  writeFileSync(validPath, `${JSON.stringify(makeReceipt("passed"))}\n`);
  readLaneReceipt(validPath, lane, "native", { ...source, canonical_worktree: process.cwd() }, "windows");
  writeFileSync(artifactPath, "mutated artifact bytes");
  assert.throws(() => readLaneReceipt(validPath, lane, "native", { ...source, canonical_worktree: process.cwd() }, "windows"), /artifact digest does not match/u);
  writeFileSync(artifactPath, "artifact bytes");
  writeFileSync(descriptorPath, `${JSON.stringify({ ...baseDescriptor, command: { executable: "node", args: ["-e", `process.exit(0); // ${lane.driver}`], cwd: process.cwd(), env: [] } })}\n`);
  assert.throws(() => readLaneReceipt(validPath, lane, "native", { ...source, canonical_worktree: process.cwd() }, "windows"), /descriptor digest does not match/u);
  writeFileSync(descriptorPath, `${JSON.stringify({ ...baseDescriptor, command: { executable: "node", args: [lane.driver, "--fixture=deterministic"], cwd: process.cwd(), env: [] } })}\n`);
  const fixturePath = join(root, "fixture.json");
  writeFileSync(fixturePath, `${JSON.stringify({ ...makeReceipt("passed"), suite: { ...makeReceipt("passed").suite, descriptor_sha256: createHash("sha256").update(readFileSync(descriptorPath)).digest("hex") } })}\n`);
  assert.throws(() => readLaneReceipt(fixturePath, lane, "native", { ...source, canonical_worktree: process.cwd() }, "windows"), /mock or fixture launch recipe/u);
});

test("the qualification-suite producer emits a receipt consumed by the platform lane validator", () => {
  const root = mkdtempInWorktree("graphcoder-lane-producer-");
  const output = join(root, "suite");
  const configPath = join(root, "config.json");
  const artifactPath = join(root, "fresh-artifact.bin");
  writeFileSync(artifactPath, "fresh artifact bytes");
  const sourceCommit = execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim();
  const sourceTree = execFileSync("git", ["rev-parse", "HEAD^{tree}"], { encoding: "utf8" }).trim();
  const artifactSha256 = createHash("sha256").update(readFileSync(artifactPath)).digest("hex");
  writeFileSync(configPath, `${JSON.stringify({
    id: "platform-lane-producer-shape",
    coverage: [{ requirement_id: "QUAL-01", assertion: "producer-shape" }],
    descriptor: "installed transport producer shape",
    execution_kind: "package",
    platform: "windows",
    command: {
      executable: process.execPath,
      args: ["-e", "process.stdout.write('graphcoder-case: QUAL-01 producer-shape passed\\ngraphcoder-executed-count: 1\\n'); process.exit(0)"],
      cwd: process.cwd(),
      env: {},
    },
    artifacts: [{ path: artifactPath, sha256: artifactSha256, source_commit: sourceCommit, source_tree: sourceTree, built_at: new Date(Date.now() - 60_000).toISOString(), build_id: "producer-shape", fresh: true }],
    output,
  }, null, 2)}\n`);
  execFileSync(process.execPath, ["scripts/graphcoder-qualification-suite.mjs", "capture", configPath], { cwd: process.cwd(), encoding: "utf8" });
  const receiptPath = join(output, "platform-lane-producer-shape.record.json");
  const lane = { ...loadManifest().qualification_lanes.find(item => item.id === "installed-transport-faults"), driver: "-e" };
  const receipt = readLaneReceipt(receiptPath, lane, "package", { commit: sourceCommit, tree: sourceTree, canonical_worktree: process.cwd() }, "windows");
  assert.equal(receipt.suite_id, "platform-lane-producer-shape");
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
