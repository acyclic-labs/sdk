import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { assemble } from "./assemble-graphcoder-receipt.mjs";

const digest = value => createHash("sha256").update(value).digest("hex");
const TEST_COMMIT = "f".repeat(40);
const TEST_TREE = "e".repeat(40);
const testGitOps = {
  currentCommit: () => TEST_COMMIT,
  currentBranch: () => "codex/graphcoder-validation",
  gitStatus: () => "",
  gitRoot: () => process.cwd(),
  gitTree: () => TEST_TREE,
  gitIsAncestor: () => true,
  gitMergeCount: () => 0,
};

function run(configPath) {
  return assemble(configPath, { gitOps: testGitOps });
}

function suiteRecord(directory, id = "mock-suite") {
  const descriptorPath = join(directory, `${id}.descriptor.json`);
  const transcriptPath = join(directory, `${id}.transcript.log`);
  const descriptor = `${JSON.stringify({
    protocol: "acyclic.graphcoder.suite-descriptor.v1",
    id,
    descriptor: id,
    source_commit: TEST_COMMIT,
    source_tree: TEST_TREE,
    source_clean: true,
    source_working_tree_sha256: "d".repeat(64),
    coverage: [{ requirement_id: "SCOPE-03", assertion: "assembler-binding" }],
    execution_assertion: { marker: "graphcoder-executed-count", minimum_executed: 1 },
    platform: "windows-x86_64",
    execution_kind: "mock",
    consumed_artifacts: [],
  })}\n`;
  const transcript = `suite ${id} passed\n`;
  writeFileSync(descriptorPath, descriptor);
  writeFileSync(transcriptPath, transcript);
  return {
    suite: {
      id,
      descriptor: id,
      descriptor_path: descriptorPath,
      descriptor_sha256: digest(descriptor),
      platform: "windows-x86_64",
      execution_kind: "mock",
      status: "passed",
      started_at: "2026-10-03T00:00:00.000Z",
      completed_at: "2026-10-03T00:00:01.000Z",
      artifact_paths: [],
      transcript_path: transcriptPath,
      transcript_sha256: digest(transcript),
    },
    artifacts: [],
  };
}

test("assembler emits every matrix ID and binds only explicit suite cases", () => {
  const directory = mkdtempSync(join(tmpdir(), "graphcoder-receipt-assembler-"));
  try {
    const recordPath = join(directory, "suite.record.json");
    const receiptPath = join(directory, "receipt.json");
    const configPath = join(directory, "config.json");
    writeFileSync(recordPath, JSON.stringify(suiteRecord(directory)));
    writeFileSync(configPath, JSON.stringify({
      suite_records: [recordPath],
      cases: [{ id: "SCOPE-03", suites: ["mock-suite"] }],
      output: receiptPath,
    }));
    run(configPath);
    const receipt = JSON.parse(readFileSync(receiptPath, "utf8"));
    assert.equal(receipt.cases.length, 68);
    assert.equal(receipt.cases.find(item => item.id === "SCOPE-03").status, "passed");
    assert.equal(receipt.cases.find(item => item.id === "SCOPE-03").evidence[0].suite, "mock-suite");
    assert.equal(receipt.cases.find(item => item.id === "SCOPE-03").evidence[0].assertion, "assembler-binding");
    assert.equal(receipt.gate.missing, 67);
    assert.equal(receipt.gate.failed, 0);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("assembler refuses a final receipt while any matrix case is unbound", () => {
  const directory = mkdtempSync(join(tmpdir(), "graphcoder-receipt-assembler-"));
  try {
    const receiptPath = join(directory, "final-receipt.json");
    const configPath = join(directory, "final-config.json");
    writeFileSync(configPath, JSON.stringify({ final: true, output: receiptPath }));
    assert.throws(() => run(configPath), /required cases must be passed|missing/);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
