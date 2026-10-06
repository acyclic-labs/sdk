import assert from "node:assert/strict";
import test from "node:test";
import { companionTarget, parseArgs, validateOptionalCompanion, validateRuntimeIdentity, validateSourceIdentity } from "./qualify-inference-native-wasm.mjs";

test("companion target resolves Linux libc from the diagnostic report", () => {
  assert.equal(companionTarget("linux", "x64", { getReport: () => ({ header: { glibcVersionRuntime: "2.39" } }) }), "linux-x64-gnu");
  assert.equal(companionTarget("linux", "arm64", { getReport: () => ({ header: {} }) }), "linux-arm64-musl");
  assert.equal(companionTarget("win32", "x64", undefined), "win32-x64");
});

test("optional dependency mapping requires Rust-generated companion metadata", () => {
  const facade = {
    name: "@acyclic-labs/inference",
    version: "0.2.0",
    optionalDependencies: { "@acyclic-labs/inference-win32-x64": "0.2.0" },
    acyclicGenerated: {
      family: "inference",
      nativeCompanions: { "@acyclic-labs/inference-win32-x64": "0.2.0" },
      nativeRuntime: { runtime_source_closure_sha256: `sha256:${"a".repeat(64)}`, runtime_build_recipe_sha256: `sha256:${"b".repeat(64)}` },
    },
  };
  const companion = { name: "@acyclic-labs/inference-win32-x64", version: "0.2.0", main: "index.js", files: ["index.js", "acyclic_inference_native.node", "BUILD.json"] };
  assert.deepEqual(validateOptionalCompanion(facade, companion, "win32-x64"), { expectedName: companion.name, facadeVersion: "0.2.0", companionVersion: "0.2.0" });
  assert.throws(() => validateOptionalCompanion(facade, { ...companion, version: "0.3.0" }, "win32-x64"), /optional dependency/);
  assert.throws(() => validateOptionalCompanion({ ...facade, acyclicGenerated: undefined }, companion, "win32-x64"), /generation metadata/);
  assert.throws(() => validateOptionalCompanion({ ...facade, acyclicGenerated: { ...facade.acyclicGenerated, nativeRuntime: undefined } }, companion, "win32-x64"), /runtime identity/);
});

test("source identity validator requires exact facade and native Rust identities", () => {
  const git = "a".repeat(40);
  const model = "b".repeat(64);
  const content = `sha256:${"c".repeat(64)}`;
  const provenance = { sourceGitSha: git, sourceModelSha256: model, sourceContentSha256: content };
  const build = { source_revision: git, source_model_revision: model, source_content_sha256: content };
  assert.deepEqual(validateSourceIdentity(provenance, build), { git, model: `sha256:${model}`, content });
  assert.throws(() => validateSourceIdentity(provenance, { ...build, source_revision: "d".repeat(40) }), /Git OIDs differ/);
  assert.throws(() => validateSourceIdentity(provenance, { ...build, source_model_revision: "e".repeat(64) }), /model identities differ/);
});

test("runtime identity validator rejects missing closure metadata and malformed inventories", () => {
  const build = {
    runtime_source_closure_sha256: `sha256:${"a".repeat(64)}`,
    runtime_build_recipe_sha256: `sha256:${"b".repeat(64)}`,
    runtime_source_files: [{ path: "Cargo.lock", sha256: "c".repeat(64) }],
  };
  assert.deepEqual(validateRuntimeIdentity(build), {
    closure: `sha256:${"a".repeat(64)}`,
    recipe: `sha256:${"b".repeat(64)}`,
    files: build.runtime_source_files,
  });
  assert.throws(() => validateRuntimeIdentity({ ...build, runtime_source_closure_sha256: undefined }), /runtime source closure/);
  assert.throws(() => validateRuntimeIdentity({ ...build, runtime_source_files: [{ path: "../escape", sha256: "c".repeat(64) }] }), /invalid path/);
  assert.throws(() => validateRuntimeIdentity({ ...build, runtime_source_files: [{ path: "Cargo.lock", sha256: "d".repeat(64) }] }, { runtime_source_closure_sha256: `sha256:${"e".repeat(64)}` }), /closures differ/);
});

test("argument parser requires both produced archives and a receipt path", () => {
  const parsed = parseArgs(["--package-archive", "facade.tgz", "--native-archive", "native.tgz", "--output", "receipt.json", "--keep-consumer"]);
  assert.equal(parsed.keepConsumer, true);
  assert.throws(() => parseArgs(["--package-archive", "facade.tgz"]), /usage/);
});
