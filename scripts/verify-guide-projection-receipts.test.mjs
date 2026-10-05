import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { verifyQualificationSummary } from "./verify-guide-projection-receipts.mjs";
const source_sha256 = "sha256:" + "a".repeat(64);
const source_revision = "source-sha256:" + source_sha256;
const command = { command: "package-manager", exitCode: 0 };
const expectedProjections = Array.from({ length: 54 }, (_, index) => ({
  scenario_id: `scenario-${index}`, language: "rust", family: "fixture", operation: "roundtrip", mode: "remote", source: "crate-owned.rs", package_manager: "cargo", package_name: "fixture", artifact_path: "package.crate", source_sha256, code: `fn main() { println!("${index}"); }`, qualification: { install: "install", compile: "compile", execute: "execute" },
}));
const receipts = expectedProjections.map(projection => ({ ...projection, source_revision, install_status: "installed", install: command, compile: command, execution: command, status: "executed", package_artifact: "package.crate", package_sha256: "b".repeat(64), snippet_sha256: createHash("sha256").update(projection.code).digest("hex") }));
const valid = { schema: "acyclic.sdk.guide-projection-qualification.v1", projection_count: 54, source_revision, source_sha256, receipts };
const verify = summary => verifyQualificationSummary(summary, { expectedProjections });
assert.equal(verify(valid).valid, true);
assert.equal(verifyQualificationSummary(valid).valid, false);
for (const bad of [
  { install: { command: "artifact present", exitCode: 0 } },
  { compile: null }, { status: "compiled", execution: null },
  { scenario_id: "scenario-0" }, { scenario_id: "invented" }, { language: "" },
  { package_sha256: "sha256:package" }, { package_artifact: "" },
  { source_revision: "another-source" }, { source_sha256: "sha256:source" },
  { snippet_sha256: "c".repeat(64) }, { operation: "invented" },
  { qualification: { install: "invented", compile: "compile", execute: "execute" } },
]) {
  const changed = receipts.map((receipt, index) => index === 3 ? { ...receipt, ...bad } : receipt);
  assert.equal(verify({ ...valid, receipts: changed }).valid, false, JSON.stringify(bad));
}
assert.equal(verify({ ...valid, source_sha256: "sha256:source" }).valid, false);
assert.equal(verify({ ...valid, source_revision: "source-sha256:another-source" }).valid, false);
assert.equal(verifyQualificationSummary(valid, { expectedProjections: Array(54).fill(expectedProjections[0]) }).valid, false);
assert.equal(verifyQualificationSummary(valid, { expectedProjections: expectedProjections.map((p,i) => i === 3 ? { ...p, code: "altered" } : p) }).valid, false);
console.log("Rust projection coverage, source, snippet and execution regressions passed");