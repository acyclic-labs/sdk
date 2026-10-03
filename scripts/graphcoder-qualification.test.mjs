import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { loadMatrix, makePendingReceipt, validateReceipt } from "./graphcoder-qualification.mjs";

const matrix = loadMatrix();
const digest = value => createHash("sha256").update(value).digest("hex");

function suiteFixture(directory, executionKind = "native") {
  const descriptorPath = join(directory, `${executionKind}.descriptor.json`);
  const transcriptPath = join(directory, `${executionKind}.transcript.log`);
  const descriptor = `{"suite":"${executionKind}","revision":1}\n`;
  const transcript = `suite ${executionKind} passed\n`;
  writeFileSync(descriptorPath, descriptor);
  writeFileSync(transcriptPath, transcript);
  return {
    id: `suite-${executionKind}`,
    descriptor: `${executionKind} fixture`,
    descriptor_path: descriptorPath,
    descriptor_sha256: digest(descriptor),
    platform: "windows-x86_64",
    execution_kind: executionKind,
    status: "passed",
    started_at: "2026-10-03T00:00:00.000Z",
    completed_at: "2026-10-03T00:00:01.000Z",
    artifact_paths: [],
    transcript_path: transcriptPath,
    transcript_sha256: digest(transcript),
  };
}

test("the locked matrix has unique coverage for every requirement", () => {
  assert.equal(matrix.entries.length, 68);
  assert.equal(new Set(matrix.entries.map(entry => entry.id)).size, matrix.entries.length);
  assert.ok(matrix.entries.every(entry => entry.contract && entry.verification && entry.modes.length > 0));
});

test("a pending receipt is structurally valid but cannot be final", () => {
  const receipt = makePendingReceipt();
  const result = validateReceipt(matrix, receipt);
  assert.equal(result.requirements, 68);
  assert.equal(result.counts.missing, 68);
  assert.throws(() => validateReceipt(matrix, { ...receipt, gate: { ...receipt.gate, final: true } }, { final: true }), /required cases must be passed|missing/);
});

test("gate.final applies final-case and artifact rules even without --final", () => {
  const receipt = makePendingReceipt();
  receipt.gate.final = true;
  assert.throws(() => validateReceipt(matrix, receipt), /is pending, required cases must be passed/);
});

test("gate.final refuses a missing artifact even without --final", () => {
  const receipt = makePendingReceipt();
  receipt.gate.final = true;
  receipt.artifacts = [{
    path: join(tmpdir(), "graphcoder-qualification-artifact-that-does-not-exist.tgz"),
    sha256: "0".repeat(64),
    source_commit: receipt.source.commit,
    source_tree: execFileSync("git", ["rev-parse", "HEAD^{tree}"], { encoding: "utf8" }).trim(),
    built_at: "2026-10-03T00:00:00.000Z",
    build_id: "build-missing",
    fresh: true,
  }];
  assert.throws(() => validateReceipt(matrix, receipt), /artifact is missing/);
});

test("evidence cannot relabel a compile suite as native", () => {
  const directory = mkdtempSync(join(tmpdir(), "graphcoder-qualification-suite-"));
  try {
    const suite = suiteFixture(directory, "compile");
    const receipt = makePendingReceipt();
    receipt.suites = [suite];
    receipt.cases[0] = {
      id: receipt.cases[0].id,
      status: "passed",
      evidence: [{ suite: suite.id, descriptor_sha256: suite.descriptor_sha256, execution_kind: "native", artifact_paths: [] }],
    };
    receipt.gate.missing--;
    assert.throws(() => validateReceipt(matrix, receipt), /execution kind does not match suite/);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("suite evidence is bound to the descriptor and transcript bytes on disk", () => {
  const directory = mkdtempSync(join(tmpdir(), "graphcoder-qualification-suite-"));
  try {
    const suite = suiteFixture(directory);
    suite.descriptor_sha256 = "0".repeat(64);
    const receipt = makePendingReceipt();
    receipt.suites = [suite];
    assert.throws(() => validateReceipt(matrix, receipt), /descriptor digest mismatch/);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("final package suites must identify the installed artifact used by their evidence", () => {
  const directory = mkdtempSync(join(tmpdir(), "graphcoder-qualification-suite-"));
  try {
    const suite = suiteFixture(directory, "package");
    const receipt = makePendingReceipt();
    receipt.suites = [suite];
    receipt.gate.final = true;
    assert.throws(() => validateReceipt(matrix, receipt), /final package suite .*must reference/);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("a bystander artifact cannot satisfy a suite's consuming-artifact evidence", () => {
  const directory = mkdtempSync(join(tmpdir(), "graphcoder-qualification-artifact-use-"));
  try {
    const suite = suiteFixture(directory, "package");
    const consumedPath = join(directory, "consumed.tgz");
    const bystanderPath = join(directory, "bystander.tgz");
    writeFileSync(consumedPath, "consumed package\\n");
    writeFileSync(bystanderPath, "bystander package\\n");
    suite.artifact_paths = [consumedPath];
    const receipt = makePendingReceipt();
    const sourceTree = execFileSync("git", ["rev-parse", "HEAD^{tree}"], { encoding: "utf8" }).trim();
    const artifact = (path, buildId) => ({
      path,
      sha256: digest(readFileSync(path)),
      source_commit: receipt.source.commit,
      source_tree: sourceTree,
      built_at: "2026-10-02T23:59:00.000Z",
      build_id: buildId,
      fresh: true,
    });
    receipt.suites = [suite];
    receipt.artifacts = [artifact(consumedPath, "consumed-build"), artifact(bystanderPath, "bystander-build")];
    receipt.cases[0] = {
      id: receipt.cases[0].id,
      status: "passed",
      evidence: [{
        suite: suite.id,
        descriptor_sha256: suite.descriptor_sha256,
        execution_kind: "package",
        artifact_paths: [bystanderPath],
      }],
    };
    receipt.gate.missing--;
    assert.throws(() => validateReceipt(matrix, receipt), /artifact use does not match suite/);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("suite execution cannot precede the build of its referenced artifact", () => {
  const directory = mkdtempSync(join(tmpdir(), "graphcoder-qualification-suite-"));
  try {
    const suite = suiteFixture(directory);
    const artifactPath = join(directory, "built-package.tgz");
    const bytes = "built package\n";
    writeFileSync(artifactPath, bytes);
    suite.artifact_paths = [artifactPath];
    const receipt = makePendingReceipt();
    receipt.suites = [suite];
    receipt.artifacts = [{
      path: artifactPath,
      sha256: digest(bytes),
      source_commit: receipt.source.commit,
      source_tree: execFileSync("git", ["rev-parse", "HEAD^{tree}"], { encoding: "utf8" }).trim(),
      built_at: "2026-10-03T00:00:02.000Z",
      build_id: "build-after-suite",
      fresh: true,
    }];
    assert.throws(() => validateReceipt(matrix, receipt), /started before artifact .* was built/);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("artifact provenance is bound to the qualified source instead of a freshness boolean", () => {
  const directory = mkdtempSync(join(tmpdir(), "graphcoder-qualification-artifact-"));
  try {
    const artifactPath = join(directory, "package.tgz");
    const bytes = "package bytes\n";
    writeFileSync(artifactPath, bytes);
    const receipt = makePendingReceipt();
    receipt.artifacts = [{
      path: artifactPath,
      sha256: digest(bytes),
      source_commit: "deadbeef",
      source_tree: "0".repeat(40),
      built_at: "2026-10-03T00:00:00.000Z",
      build_id: "build-1",
      fresh: true,
    }];
    assert.throws(() => validateReceipt(matrix, receipt), /not built from the qualified source commit/);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("source claims are checked against the actual checkout", () => {
  const receipt = makePendingReceipt();
  receipt.source.worktree = "C:\\definitely-not-the-qualified-worktree";
  assert.throws(() => validateReceipt(matrix, receipt), /source worktree does not match/);
});

test("matrix digest prevents a receipt from silently changing its requirements", () => {
  const receipt = makePendingReceipt();
  const directory = mkdtempSync(join(tmpdir(), "graphcoder-qualification-"));
  const matrixCopy = join(directory, "requirements.json");
  const receiptPath = join(directory, "receipt.json");
  try {
    writeFileSync(matrixCopy, `${readFileSync("docs/graphcoder-swarm/requirements.json", "utf8")}\n`);
    writeFileSync(receiptPath, JSON.stringify({ ...receipt, matrix: { ...receipt.matrix, path: matrixCopy } }));
    assert.throws(() => validateReceipt(matrix, JSON.parse(readFileSync(receiptPath, "utf8"))), /locked matrix path/);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
