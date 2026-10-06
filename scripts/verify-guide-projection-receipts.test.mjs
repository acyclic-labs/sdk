import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { gzipSync } from "node:zlib";
import { verifyQualificationSummary } from "./verify-guide-projection-receipts.mjs";
const source_sha256 = "sha256:" + "a".repeat(64);
const source_revision = "source-sha256:" + "a".repeat(64);
const command = { command: "package-manager", exitCode: 0 };
const expectedProjections = Array.from({ length: 54 }, (_, index) => ({
  scenario_id: `scenario-${index}`, language: "rust", family: "fixture", operation: "roundtrip", mode: "remote", source: "crate-owned.rs", package_manager: "cargo", package_name: "fixture", artifact_path: "package.crate", source_sha256, code: `fn main() { println!("${index}"); }`, qualification: { install: "install", compile: "compile", execute: "execute" },
}));
const receipts = expectedProjections.map(projection => ({ ...projection, source_revision, install_status: "installed", install: command, compile: command, execution: command, status: "executed", package_artifact: "package.crate", package_sha256: "b".repeat(64), package_archive_sha256: "b".repeat(64), package_archive_format: "gzip+ustar", package_tree_sha256: "c".repeat(64), resolved_package_root: "installed/package", snippet_sha256: createHash("sha256").update(projection.code).digest("hex") }));
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
function packageArchiveEntry(path, body) {
  const header = Buffer.alloc(512, 0);
  header.write(path, 0, 100, "utf8");
  header.write("00000000000\0", 100, 8, "ascii");
  header.write("00000000000\0", 108, 8, "ascii");
  header.write("00000000000\0", 116, 8, "ascii");
  header.write(`${body.length.toString(8).padStart(11, "0")}\0`, 124, 12, "ascii");
  header.write("00000000000\0", 136, 12, "ascii");
  header.fill(0x20, 148, 156);
  header[156] = 0x30;
  header.write("ustar\0", 257, 6, "ascii");
  const checksum = header.reduce((sum, byte) => sum + byte, 0);
  header.write(`${checksum.toString(8).padStart(6, "0")}\0 `, 148, 8, "ascii");
  const padding = Buffer.alloc((512 - (body.length % 512)) % 512);
  return Buffer.concat([header, body, padding]);
}
const packageBody = Buffer.from("installed package bytes");
const packageTar = Buffer.concat([packageArchiveEntry("package/index.js", packageBody), Buffer.alloc(1024)]);
const packageBytes = gzipSync(packageTar, { mtime: 0 });
const packageTreeSha256 = "sha256:" + createHash("sha256")
  .update("index.js")
  .update(Buffer.from([0]))
  .update(packageBody)
  .update(Buffer.from([0]))
  .digest("hex");
const packageDigest = createHash("sha256").update(packageBytes).digest("hex");
const bytesReceipts = receipts.map(receipt => ({ ...receipt, package_sha256: packageDigest, package_archive_sha256: packageDigest, package_tree_sha256: packageTreeSha256 }));
const bytesSummary = { ...valid, receipts: bytesReceipts };
const verifyBytes = (summary, readArtifact) => verifyQualificationSummary(summary, { expectedProjections, readArtifact });
assert.equal(verifyBytes(bytesSummary, path => { assert.equal(path, "package.crate"); return packageBytes; }).valid, true);
assert.equal(verifyBytes(bytesSummary, () => Buffer.from("tampered package bytes")).valid, false);
assert.equal(verifyBytes(bytesSummary, () => { throw new Error("missing package"); }).valid, false);
assert.equal(verifyBytes({ ...bytesSummary, receipts: bytesReceipts.map(receipt => ({ ...receipt, package_sha256: "sha256:" + receipt.package_sha256, package_archive_sha256: "sha256:" + receipt.package_archive_sha256 })) }, () => packageBytes).valid, true);
console.log("Package byte integrity regressions passed");

const embeddedCode = `import { Harness } from "@acyclic-labs/harness";
const client = await Harness.create({ authority: { kind: "task", id: "fixture" }, issuerId: "fixture", issuerKey: new Uint8Array(32) });
const response = client.issueScope("guide", ["event:append"]);`;
const embeddedProjection = {
  scenario_id: "harness-admission-recovery-cancel",
  language: "typescript",
  family: "harness",
  operation: "acyclic_sdk_examples::harness_scenarios::execute_harness_scenario",
  mode: "embedded",
  source: "rust/crates/sdk-examples/src/harness_scenarios.rs",
  source_sha256,
  package_manager: "npm",
  package_name: "@acyclic-labs/harness",
  artifact_path: "harness.tgz",
  qualification: { install: "install", compile: "compile", execute: "execute" },
  code: embeddedCode,
};
const embeddedReceipt = {
  ...embeddedProjection,
  source_revision,
  install_status: "installed",
  install: command,
  compile: command,
  execution: command,
  status: "executed",
  package_artifact: "harness.tgz",
  package_sha256: "b".repeat(64),
  package_archive_sha256: "b".repeat(64),
  package_archive_format: "gzip+ustar",
  package_tree_sha256: "c".repeat(64),
  resolved_package_root: "installed/harness",
  snippet_sha256: createHash("sha256").update(embeddedCode).digest("hex"),
};
const embeddedSummary = {
  schema: "acyclic.sdk.guide-projection-qualification.v1",
  projection_count: 1,
  source_revision,
  source_sha256,
  receipts: [embeddedReceipt],
};
assert.equal(verifyQualificationSummary(embeddedSummary, { expectedProjections: [embeddedProjection], expectedProjectionCount: 1 }).valid, true);
const remoteMarkerCode = `${embeddedCode}\n// CommandEnvelope`;
const remoteMarkerProjection = { ...embeddedProjection, code: remoteMarkerCode };
const remoteMarkerReceipt = { ...embeddedReceipt, code: remoteMarkerCode, snippet_sha256: createHash("sha256").update(remoteMarkerCode).digest("hex") };
assert.equal(verifyQualificationSummary({ ...embeddedSummary, receipts: [remoteMarkerReceipt] }, { expectedProjections: [remoteMarkerProjection], expectedProjectionCount: 1 }).valid, false);
const wrongOperationProjection = { ...embeddedProjection, operation: "acyclic.harness.v2.HarnessService/Submit" };
const wrongOperationReceipt = { ...embeddedReceipt, operation: wrongOperationProjection.operation };
assert.equal(verifyQualificationSummary({ ...embeddedSummary, receipts: [wrongOperationReceipt] }, { expectedProjections: [wrongOperationProjection], expectedProjectionCount: 1 }).valid, false);
console.log("Embedded Harness identity and remote-marker regressions passed");
