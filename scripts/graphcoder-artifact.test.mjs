import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { describeArtifact } from "./graphcoder-artifact.mjs";

test("artifact descriptor binds exact bytes and source provenance", () => {
  const directory = mkdtempSync(join(tmpdir(), "graphcoder-artifact-"));
  try {
    const path = join(directory, "graphcoder.tgz");
    const bytes = "fresh package bytes\n";
    writeFileSync(path, bytes);
    const record = describeArtifact({
      path,
      sourceCwd: directory,
      buildId: "build-1",
      builtAt: "2026-10-03T00:00:00.123Z",
      gitOps: { git: (_cwd, ...args) => args.at(-1) === "HEAD" ? "a".repeat(40) : "b".repeat(40) },
    });
    assert.equal(record.path, path);
    assert.equal(record.sha256, createHash("sha256").update(bytes).digest("hex"));
    assert.equal(record.source_commit, "a".repeat(40));
    assert.equal(record.source_tree, "b".repeat(40));
    assert.equal(record.built_at, "2026-10-03T00:00:00.123Z");
    assert.equal(record.fresh, false);
    assert.equal(record.producer_receipt.origin, "direct-observation");
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("artifact descriptor rejects missing or symlinked files", () => {
  const directory = mkdtempSync(join(tmpdir(), "graphcoder-artifact-"));
  try {
    assert.throws(() => describeArtifact({ path: join(directory, "missing.tgz"), buildId: "build-1" }), /does not exist/);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("producer receipt paths resolve from the declared source checkout", () => {
  const directory = mkdtempSync(join(tmpdir(), "graphcoder-artifact-receipt-"));
  try {
    const path = join(directory, "runtime.exe");
    const bytes = "runtime bytes\n";
    writeFileSync(path, bytes);
    const sha256 = createHash("sha256").update(bytes).digest("hex");
    writeFileSync(join(directory, "producer.json"), JSON.stringify({
      protocol: "acyclic.graphcoder.producer-receipt.v1",
      producer_id: "test-producer",
      artifact_path: path,
      sha256,
      source_commit: "a".repeat(40),
      source_tree: "b".repeat(40),
      observed_after_dispatch: true,
      attempt_nonce: "fixture-attempt",
      invocation_sha256: "c".repeat(64),
      source_working_tree_sha256: "d".repeat(64),
    }));
    const record = describeArtifact({
      path,
      sourceCwd: directory,
      producerReceipt: "producer.json",
      buildId: "build-1",
      builtAt: "2026-10-03T00:00:00.123Z",
      workingTreeDigestFn: () => "d".repeat(64),
      gitOps: { git: (_cwd, ...args) => args.at(-1) === "HEAD" ? "a".repeat(40) : "b".repeat(40) },
    });
    assert.equal(record.producer_receipt.producer_id, "test-producer");
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
