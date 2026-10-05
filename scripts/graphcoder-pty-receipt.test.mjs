import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { loadManifest, readLaneReceipt } from "./graphcoder-platform-gates.mjs";
import { workingTreeDigest } from "./graphcoder-source-fence.mjs";

const digest = value => createHash("sha256").update(value).digest("hex");

function sourceIdentity(canonicalWorktree) {
  return {
    commit: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
    tree: execFileSync("git", ["rev-parse", "HEAD^{tree}"], { encoding: "utf8" }).trim(),
    canonical_worktree: canonicalWorktree,
  };
}

function writePtyReceipt(root, source, { fixture = false, fresh = true } = {}) {
  const lane = loadManifest().qualification_lanes.find(item => item.id === "installed-pty");
  const artifactPath = join(root, "graphcoder-package.tgz");
  const transcriptPath = join(root, "pty.transcript.log");
  const descriptorPath = join(root, "pty.descriptor.json");
  writeFileSync(artifactPath, "fresh installed package bytes\n");
  writeFileSync(transcriptPath, "pty passed\ngraphcoder-case: CLI-02 pty-receipt passed\ngraphcoder-executed-count: 1\n");
  const artifact = {
    path: artifactPath,
    sha256: digest(readFileSync(artifactPath)),
    source_commit: source.commit,
    source_tree: source.tree,
    build_id: "pty-package-build",
    fresh,
  };
  const descriptor = {
    protocol: "acyclic.graphcoder.suite-descriptor.v1",
    id: "pty-receipt-fixture",
    descriptor: "installed pty receipt fixture",
    source_commit: source.commit,
    source_tree: source.tree,
    source_clean: true,
    source_working_tree_sha256: workingTreeDigest(source.canonical_worktree),
    platform: "windows",
    execution_kind: "pty",
    coverage: [{ requirement_id: "CLI-02", assertion: "pty-receipt" }],
    command: {
      executable: process.execPath,
      args: [lane.driver, ...(fixture ? ["--fixture=deterministic"] : [])],
      cwd: source.canonical_worktree,
      env: [],
    },
    expected_exit_code: 0,
    execution_assertion: { marker: "graphcoder-executed-count", minimum_executed: 1 },
    consumed_artifacts: [{
      path: artifact.path,
      sha256: artifact.sha256,
      source_commit: artifact.source_commit,
      source_tree: artifact.source_tree,
      build_id: artifact.build_id,
    }],
  };
  const descriptorBytes = Buffer.from(`${JSON.stringify(descriptor)}\n`);
  writeFileSync(descriptorPath, descriptorBytes);
  return {
    lane,
    source,
    path: join(root, "pty.record.json"),
    record: {
      suite: {
        id: descriptor.id,
        descriptor: descriptor.descriptor,
        descriptor_path: descriptorPath,
        descriptor_sha256: digest(descriptorBytes),
        platform: descriptor.platform,
        execution_kind: descriptor.execution_kind,
        status: "passed",
        artifact_paths: [artifact.path],
        transcript_path: transcriptPath,
        transcript_sha256: digest(readFileSync(transcriptPath)),
        execution_evidence: {
          marker: "graphcoder-executed-count",
          executed_count: 1,
          minimum_executed: 1,
          raw_exit_code: 0,
          signal: null,
          cases: [{ requirement_id: "CLI-02", assertion: "pty-receipt", status: "passed" }],
        },
      },
      artifacts: [artifact],
    },
  };
}

test("Windows PTY receipts reject fixture recipes and stale artifacts", () => {
  const root = mkdtempSync(join(tmpdir(), "graphcoder-pty-receipt-"));
  try {
    execFileSync("git", ["init", "--quiet", root]);
    writeFileSync(join(root, "marker.txt"), "pty receipt fixture\n");
    execFileSync("git", ["-C", root, "add", "marker.txt"]);
    execFileSync("git", ["-C", root, "-c", "user.name=GraphCoder Test", "-c", "user.email=graphcoder@example.invalid", "commit", "--quiet", "-m", "fixture"]);
    const source = sourceIdentity(root);
    const valid = writePtyReceipt(root, source);
    writeFileSync(valid.path, `${JSON.stringify(valid.record, null, 2)}\n`);
    readLaneReceipt(valid.path, valid.lane, "pty", valid.source, "windows");

    const fixture = writePtyReceipt(root, source, { fixture: true });
    writeFileSync(fixture.path, `${JSON.stringify(fixture.record, null, 2)}\n`);
    assert.throws(
      () => readLaneReceipt(fixture.path, fixture.lane, "pty", fixture.source, "windows"),
      /mock or fixture launch recipe/u,
    );

    const stale = writePtyReceipt(root, source, { fresh: false });
    writeFileSync(stale.path, `${JSON.stringify(stale.record, null, 2)}\n`);
    assert.throws(
      () => readLaneReceipt(stale.path, stale.lane, "pty", stale.source, "windows"),
      /provenance is stale or not fresh/u,
    );
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
