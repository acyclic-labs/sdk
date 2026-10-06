#!/usr/bin/env node

// Focused local checks for with-rust-fixture.mjs. These intentionally exercise
// process lifecycle and manifest validation rather than any SDK semantics.

import assert from "node:assert/strict";
import { mkdtempSync, existsSync, readFileSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

const repo = resolve(fileURLToPath(new URL("..", import.meta.url)));
const runner = join(repo, "scripts", "with-rust-fixture.mjs");
const fixture = process.env.FIXTURE_SERVER_BIN ?? join(
  repo,
  "rust",
  "crates",
  "sdk-examples",
  "target",
  "debug",
  process.platform === "win32" ? "fixture-server.exe" : "fixture-server",
);
const temp = mkdtempSync(join(tmpdir(), "acyclic-rust-fixture-test-"));

function runNode(args) {
  return new Promise((resolveRun, reject) => {
    // Some Windows runners reject anonymous pipe handles. The assertions use
    // exit status and the receipt, so inherit no handles for this subprocess.
    const child = spawn(process.execPath, args, { cwd: repo, windowsHide: true, stdio: "ignore" });
    child.once("error", reject);
    child.once("exit", (status, signal) => resolveRun({ status, signal }));
  });
}

try {
  const invalid = join(temp, "invalid.json");
  writeFileSync(invalid, JSON.stringify({ complete: false, execution_plan: [] }));
  const invalidRun = await runNode([
    runner,
    "--manifest", invalid,
    "--language", "python",
    "--fixture", fixture,
    "--", process.execPath, "-e", "process.exit(0)",
  ]);
  assert.notEqual(invalidRun.status, 0, "invalid manifests must be rejected");

  if (!existsSync(fixture)) {
    console.log("PASS: invalid manifest rejection (fixture binary absent; lifecycle check skipped)");
    process.exit(0);
  }

  const manifest = join(temp, "manifest.json");
  writeFileSync(manifest, JSON.stringify({
    complete: true,
    execution_plan: [{ rpc: "test/Fake", request_base64: "", response_frames: [] }],
    execution_plan_count: 1,
    source_revision: "fixture-test",
    execution_plan_sha256: "sha256:fixture-test",
  }));
  const mismatchedBuild = join(temp, "mismatched-build.json");
  writeFileSync(mismatchedBuild, JSON.stringify({ source_revision: "different-source", binary_sha256: "sha256:different-binary" }));
  const bindingRun = await runNode([
    runner,
    "--manifest", manifest,
    "--language", "python",
    "--fixture", fixture,
    "--build-receipt", mismatchedBuild,
    "--require-source-binding",
    "--", process.execPath, "-e", "process.exit(0)",
  ]);
  assert.notEqual(bindingRun.status, 0, "mismatched build provenance must be rejected");
  const receipt = join(temp, "receipt.json");
  const failedRun = await runNode([
    runner,
    "--manifest", manifest,
    "--language", "python",
    "--fixture", fixture,
    "--receipt", receipt,
    "--max-requests", "8",
    "--", process.execPath, "-e", "process.exit(7)",
  ]);
  assert.equal(failedRun.status, 7, "consumer exit status must propagate");
  assert.ok(existsSync(receipt), "consumer failure must still produce a receipt");
  const evidence = JSON.parse(readFileSync(receipt, "utf8"));
  assert.equal(evidence.status, "failed");
  assert.equal(evidence.consumer_exit_code, 7);
  assert.ok(Number.isInteger(evidence.fixture_pid) && evidence.fixture_pid > 0);
  await new Promise((resolveDelay) => setTimeout(resolveDelay, 300));
  assert.throws(() => process.kill(evidence.fixture_pid, 0), "fixture child must be cleaned up");
  console.log("PASS: invalid manifest rejection, consumer failure propagation, and fixture cleanup");
} finally {
  rmSync(temp, { recursive: true, force: true });
}
