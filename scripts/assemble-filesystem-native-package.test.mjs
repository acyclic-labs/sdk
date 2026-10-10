import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import test from "node:test";
import { nativeFamily } from "./native-family.mjs";
import { createNativeAssembler } from "./assemble-native-family.mjs";

const family = nativeFamily("filesystem");
const assembler = createNativeAssembler(family);
const source = "a".repeat(40);
const hash = bytes => `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
// Admission controls only: these fixtures are never claimed as compiler/runtime proof.
const receipt = Buffer.from("synthetic compiler receipt for mutation controls");
const artifact = { path: "generated/native/acyclic-fs-0.2.0.linux-x64-gnu.node", sha256: hash("synthetic artifact"), bytes: 18 };
const metadata = { source_revision: source, source_sha256: hash("source"), selected_target: family.targets[0], version: family.version, artifact };
const companion = { name: "@acyclic-labs/fs-linux-x64-gnu", os: ["linux"], cpu: ["x64"], main: artifact.path.slice("generated/native/".length) };
const proof = () => ({
  schema: "acyclic.filesystem.native-runtime-qualification.v1",
  source_commit: source, source_sha256: metadata.source_sha256,
  target: metadata.selected_target, platform: "linux", arch: "x64", runtime: "node", node: "v24.15.0",
  artifact: { ...artifact }, producer_receipt_sha256: hash(receipt),
  retained_artifact: { path: "acyclic-fs-0.2.0-linux-x64.node", sha256: artifact.sha256, bytes: artifact.bytes },
  archives: [{ path: "acyclic-labs-fs-0.2.0.tgz", sha256: hash("parent") }, { path: "acyclic-labs-fs-linux-x64-gnu-0.2.0.tgz", sha256: hash("companion") }],
});

test("filesystem runtime admission binds source, filename, bytes, triple and actual architecture", () => {
  assembler.assertNativeRuntimeQualification(proof(), metadata, companion, receipt);
  for (const mutation of [
    { source_commit: "b".repeat(40) }, { source_sha256: hash("other-source") },
    { target: family.targets[1] }, { platform: "darwin" }, { arch: "arm64" },
    { artifact: { ...artifact, sha256: hash("other bytes") } },
    { artifact: { ...artifact, path: "generated/native/wrong.node" } },
    { producer_receipt_sha256: hash("wrong compiler") }, { archives: [] },
    { runtime: "bun" }, { retained_artifact: { ...artifact, path: "wrong.node" } },
    { archives: [proof().archives[0], proof().archives[0]] },
    { archives: [{ ...proof().archives[0], path: "wrong.tgz" }, proof().archives[1]] },
  ]) {
    assert.throws(() => assembler.assertNativeRuntimeQualification({ ...proof(), ...mutation }, metadata, companion, receipt), /runtime qualification/);
  }
  assert.throws(() => assembler.assertNativeRuntimeQualification(proof(), metadata, { ...companion, main: "wrong.node" }, receipt), /runtime qualification/);
  assert.throws(() => assembler.assertNativeRuntimeQualification(proof(), metadata, companion, Buffer.from("modified receipt")), /runtime qualification/);
});

test("filesystem release requires every Rust-qualified target at one exact source", () => {
  const bundles = family.targets.map(selected_target => ({ ...metadata, selected_target, targets: family.targets, source_files: [], package: family.rustPackageName, version: family.version }));
  assembler.assertNativeSet(bundles, family.targets, source);
  assert.throws(() => assembler.assertNativeSet(bundles.slice(1), family.targets, source), /exactly one bundle/);
  assert.throws(() => assembler.assertNativeSet([...bundles, bundles[0]], family.targets, source), /exactly one bundle/);
  assert.throws(() => assembler.assertNativeSet(bundles.map((entry, index) => index ? entry : { ...entry, source_revision: "b".repeat(40) }), family.targets, source), /source revision/);
});

test("filesystem parent freezes six canonical NAPI dependencies at the Rust release version", () => {
  const manifest = JSON.parse(readFileSync(new URL("../typescript/packages/filesystem/package.json", import.meta.url), "utf8"));
  assert.equal(manifest.version, family.version);
  assert.deepEqual(manifest.optionalDependencies, {
    "@acyclic-labs/fs-darwin-arm64": family.version, "@acyclic-labs/fs-darwin-x64": family.version,
    "@acyclic-labs/fs-linux-arm64-gnu": family.version, "@acyclic-labs/fs-linux-x64-gnu": family.version,
    "@acyclic-labs/fs-win32-arm64-msvc": family.version, "@acyclic-labs/fs-win32-x64-msvc": family.version,
  });
  assert.equal(family.npmEntrypoint, "browser.js");
  const qualified = assembler.qualifiedCompanionManifest({ files: ["addon.node"] });
  assert.ok(qualified.files.includes("runtime-qualification.json"));
});
