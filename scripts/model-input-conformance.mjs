import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { modelInputVector, packagedSourceCopies } from "./generated-bindings.mjs";

const root = fileURLToPath(new URL("../", import.meta.url));
const vectorPath = join(root, modelInputVector);
const crateVectorPath = join(root, "rust/crates/harness/conformance/model-input-v3.json");
const nativeTestPath = join(root, "rust/crates/harness/tests/model_input_conformance_v3.rs");
const rejectionTestPath = join(root, "rust/crates/harness/tests/model_input_rejection_conformance_v3.rs");
const typescriptTestPath = join(root, "typescript/packages/harness/test/model-input-conformance-v3.test.ts");
const bytes = path => readFileSync(path);
const sha256 = value => createHash("sha256").update(value).digest("hex");
const vector = JSON.parse(bytes(vectorPath));
if (vector.version !== 3 || vector.children?.length !== 2 || vector.grandchild === undefined) {
  throw new Error("model-input-v3 must retain root, two sibling, and grandchild cases");
}
if (!Array.isArray(vector.rejections) || vector.rejections.length !== 4
  || vector.rejections.some(entry => typeof entry.name !== "string"
    || !["conflict", "invalid"].includes(entry.error_kind)
    || typeof entry.error_message !== "string")) {
  throw new Error("model-input-v3 must retain its unknown-result and schema-result rejection cases");
}
for (const entry of [vector.root, ...vector.children, vector.grandchild]) {
  for (const field of ["request_json", "manifest_json", "request_digest", "binding_digest", "manifest_digest", "prefix_digest"]) {
    const value = entry.expected?.[field];
    if (value === undefined || (typeof value === "string" && value.length === 0) || (Array.isArray(value) && value.length !== 32)) {
      throw new Error(`model-input-v3 expected.${field} is incomplete`);
    }
  }
}
if (!bytes(vectorPath).equals(bytes(crateVectorPath))) {
  throw new Error("Harness crate model-input-v3 copy drifted from the canonical vector");
}
const nativeTest = bytes(nativeTestPath).toString("utf8");
const rejectionTest = bytes(rejectionTestPath).toString("utf8");
const typescriptTest = bytes(typescriptTestPath).toString("utf8");
if (!nativeTest.includes("model-input-v3.json")
  || !rejectionTest.includes("model-input-v3.json")
  || !typescriptTest.includes("model-input-v3.json")) {
  throw new Error("native, rejection, and TypeScript conformance tests must consume the frozen vector");
}
if (!packagedSourceCopies.some(([source, destination]) => source === modelInputVector
  && destination === "rust/crates/harness/conformance/model-input-v3.json")) {
  throw new Error("generated binding manifest does not package model-input-v3");
}

const runNative = process.argv.includes("--run-native");
if (runNative) {
  const cargo = process.env.ACYCLIC_CARGO_BIN || "cargo";
  const native = spawnSync(cargo, [
    "test", "--locked", "-p", "acyclic-harness", "--test", "model_input_conformance_v3",
    "--", "--nocapture",
  ], { cwd: root, encoding: "utf8", windowsHide: true });
  if (native.status !== 0) {
    process.stderr.write(native.stdout ?? "");
    process.stderr.write(native.stderr ?? "");
    throw new Error(`native model-input conformance failed (${native.status ?? "unknown"})`);
  }
  const marker = (native.stdout ?? "").match(/MODEL_INPUT_NATIVE_FIXTURE_V3 (\{.*\})/);
  if (marker === null) throw new Error("native model-input fixture emission produced no report");
  const emitted = JSON.parse(marker[1]);
  const expectedCases = [vector.root, ...vector.children, vector.grandchild].map((entry, index) => ({
    name: entry.name ?? (index === 0 ? "root" : index === vector.children.length + 1 ? "grandchild" : "child"),
    expected: entry.expected,
  }));
  if (emitted.version !== vector.version || emitted.cases?.length !== expectedCases.length) {
    throw new Error("native model-input fixture report has the wrong version or case count");
  }
  for (const [index, expected] of expectedCases.entries()) {
    const actual = emitted.cases[index];
    if (actual?.name !== expected.name) throw new Error(`native model-input case name drift at ${index}`);
    for (const field of ["request_json", "manifest_json", "request_digest", "binding_digest", "manifest_digest", "prefix_digest"]) {
      if (JSON.stringify(actual[field]) !== JSON.stringify(expected.expected[field])) {
        throw new Error(`native model-input ${field} drift at ${expected.name}`);
      }
    }
  }
  if (emitted.rejections?.length !== vector.rejections.length) {
    throw new Error("native model-input rejection report has the wrong case count");
  }
  for (const [index, expected] of vector.rejections.entries()) {
    const actual = emitted.rejections[index];
    if (actual?.name !== expected.name || actual.kind !== expected.error_kind
      || actual.message !== expected.error_message) {
      throw new Error(`native model-input rejection drift at ${expected.name}`);
    }
  }
}

if (process.argv.includes("--run-typescript")) {
  const result = spawnSync("bun", ["test", "typescript/packages/harness/test/model-input-conformance-v3.test.ts"], {
    cwd: root,
    stdio: "inherit",
    windowsHide: true,
  });
  if (result.status !== 0) throw new Error(`TypeScript model-input conformance failed (${result.status ?? "unknown"})`);
}
const report = {
  protocol: "acyclic.harness.model-input-conformance.v3",
  vector: modelInputVector,
  vector_sha256: sha256(bytes(vectorPath)),
  crate_vector_sha256: sha256(bytes(crateVectorPath)),
  native_test_sha256: sha256(bytes(nativeTestPath)),
  rejection_test_sha256: sha256(bytes(rejectionTestPath)),
  typescript_test_sha256: sha256(bytes(typescriptTestPath)),
  native_executed: runNative,
  typescript_executed: process.argv.includes("--run-typescript"),
};
console.log(JSON.stringify(report));
