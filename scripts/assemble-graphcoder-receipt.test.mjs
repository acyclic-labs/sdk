import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

const ROOT = process.cwd();
const ASSEMBLER = join(ROOT, "scripts", "assemble-graphcoder-receipt.mjs");
const digest = value => createHash("sha256").update(value).digest("hex");

function run(directory, configPath) {
  return execFileSync(process.execPath, [ASSEMBLER, "assemble", configPath], {
    cwd: ROOT,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });
}

function suiteRecord(directory, id = "mock-suite") {
  const descriptorPath = join(directory, `${id}.descriptor.json`);
  const transcriptPath = join(directory, `${id}.transcript.log`);
  const descriptor = `{"suite":"${id}"}\n`;
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
      cases: [{ id: "SCOPE-01", suites: ["mock-suite"] }],
      output: receiptPath,
    }));
    run(directory, configPath);
    const receipt = JSON.parse(readFileSync(receiptPath, "utf8"));
    assert.equal(receipt.cases.length, 68);
    assert.equal(receipt.cases.find(item => item.id === "SCOPE-01").status, "passed");
    assert.equal(receipt.cases.find(item => item.id === "SCOPE-01").evidence[0].suite, "mock-suite");
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
    assert.throws(() => run(directory, configPath), error => {
      assert.equal(error.status, 1);
      return /required cases must be passed|missing/.test(String(error.stderr));
    });
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
