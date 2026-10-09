import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { assertCommonBindings, assertNativeSet, verifyNativeAssembly } from "./assemble-actors-native-package.mjs";
import { assertSelectedArtifact, normalizeBuildInputs, buildInputsReceipt } from "./build-actors-native.mjs";
import { validBuildInputs } from "./fixtures/native-build-inputs.mjs";

const targets = ["target-a", "target-b", "target-c"];
const source = "a".repeat(40);
const qualified = () => targets.map(selected_target => ({ selected_target, source_revision: source, source_sha256: "sha256:source", source_files: [{ path: "contract.rs", sha256: "sha256:contract", bytes: 10 }], package: "acyclic-actors-napi", version: "0.2.0", targets }));

test("selected native artifact must match the complete generation record", () => {
  const artifact = { path: "generated/native/index.node", sha256: "sha256:binary", bytes: 4096 };
  assert.doesNotThrow(() => assertSelectedArtifact({ ...artifact }, [artifact]));
  for (const mutation of [{ bytes: 4095 }, { sha256: "sha256:changed" }, { path: "generated/native/other.node" }]) {
    assert.throws(() => assertSelectedArtifact({ ...artifact, ...mutation }, [artifact]), /selected artifact attestation differs/);
  }
});

test("assembly requires one bundle for each declared Rust target", () => {
  assert.doesNotThrow(() => assertNativeSet(qualified(), targets, source));
  const missing = qualified().slice(1);
  const duplicate = qualified(); duplicate[1].selected_target = targets[0];
  const unexpected = qualified(); unexpected[1].selected_target = "unqualified";
  for (const metadata of [missing, duplicate, unexpected]) assert.throws(() => assertNativeSet(metadata, targets, source), /exactly one bundle/);
});

test("assembly rejects divergent source, version and Rust target declarations", () => {
  /** @type {Array<[string, any]>} */
  const mutations = [["source_revision", "b".repeat(40)], ["source_sha256", "sha256:changed"], ["source_files", []], ["version", "0.3.0"], ["targets", [...targets, "unqualified"]]];
  for (const [field, value] of mutations) {
    const metadata = qualified(); metadata[1][field] = value;
    assert.throws(() => assertNativeSet(metadata, targets, source), /source|version|targets/);
  }
  assert.throws(() => assertNativeSet(qualified(), [...targets].reverse(), source), /declaration differs/);
});

test("common loader and declarations must match actual bytes across bundles", async () => {
  const root = await mkdtemp(join(tmpdir(), "actors-neutral-bindings-"));
  const bundles = targets.map(target => join(root, target));
  try {
    for (const bundle of bundles) {
      await mkdir(bundle);
      for (const name of ["binding.cjs", "binding.d.ts"]) await writeFile(join(bundle, name), "common bytes\n");
    }
    await assertCommonBindings(bundles);
    for (const name of ["binding.cjs", "binding.d.ts"]) {
      await writeFile(join(bundles[1], name), "different compiled bytes\n");
      await assert.rejects(() => assertCommonBindings(bundles), new RegExp(`${name.replaceAll(".", "\\.")} differs`));
      await writeFile(join(bundles[1], name), "common bytes\n");
    }
    await rm(join(bundles[2], "binding.cjs"));
    await assert.rejects(() => assertCommonBindings(bundles), { code: "ENOENT" });
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("assembly rejects stale output before discovering Cargo", async () => {
  const output = await mkdtemp(join(tmpdir(), "actors-neutral-stale-"));
  try {
    await writeFile(join(output, "unqualified-companion.tgz"), "stale archive");
    const script = fileURLToPath(new URL("./assemble-actors-native-package.mjs", import.meta.url));
    const result = spawnSync(process.execPath, [script, "--bundle", "not-built", "--output", output, "--source-sha", source], { env: { ...process.env, PATH: "" }, encoding: "utf8" });
    assert.equal(result.status, 1);
    assert.match(result.stderr, /unstated files: unqualified-companion/);
    assert.doesNotMatch(result.stderr, /cargo.*ENOENT/i);
  } finally { await rm(output, { recursive: true, force: true }); }
});

test("archive verification binds parent, companion, original source and actual addon bytes", async () => {
  const directory = await mkdtemp(join(tmpdir(), "actors-neutral-archives-"));
  const output = join(directory, "out"), parent = join(directory, "parent/package"), companion = join(directory, "companion/package");
  const target = "fixture-target", version = "0.2.0", name = "@acyclic-labs/actors-fixture", main = "index.fixture.node";
  const hash = bytes => createHash("sha256").update(bytes).digest("hex");
  const json = async (path, value) => writeFile(path, JSON.stringify(value));
  const pack = (kind, asset) => execFileSync("tar", ["-czf", join(output, asset), "-C", join(directory, kind), "package"], { env: { ...process.env, COPYFILE_DISABLE: "1" } });
  try {
    await mkdir(join(output, "actors-native"), { recursive: true });
    await mkdir(join(parent, "generated/native/attestations", target), { recursive: true });
    await mkdir(companion, { recursive: true });
    const artifacts = ["binding.cjs", "binding.d.ts", main].map(path => ({ path: `generated/native/${path}`, sha256: `sha256:${hash(Buffer.from(path))}`, bytes: Buffer.byteLength(path) }));
    const companionManifest = { name, version, private: false, main, os: ["fixture"], cpu: ["fixture"] };
    const parentManifest = { name: "@acyclic-labs/actors", version, private: false, optionalDependencies: { [name]: version } };
    const inventory = { schema: "acyclic.actors.native-source-inventory.v1", source_commit: source, source_sha256: "sha256:source", source_files: [{ path: "contract.rs", sha256: "sha256:contract", bytes: 10 }], parent: parentManifest, targets: [target], companions: [{ selected_target: target, name, main, os: ["fixture"], cpu: ["fixture"], manifest: companionManifest }] };
    const raw = validBuildInputs("actors");
    raw.target = raw.linker.actual.target = raw.generator.options.target = target;
    raw.compiler.rustc.command = raw.linker.actual.rustc;
    raw.linker.actual.args.push("--remap-path-prefix=C:/producer/sdk=/__acyclic_stream_source");
    const build_inputs = normalizeBuildInputs(raw, { sourceRoot: "C:/producer/sdk", platform: "win32", targetDir: raw.target_dir, outputDir: raw.generator.options.output_dir });
    const { options: ignored, ...generator } = raw.generator;
    inventory.generator = generator;
    const compilerReceipt = buildInputsReceipt(raw, build_inputs);
    const generation = { schema: "acyclic.actors.native-generation.v1", package: "acyclic-actors-napi", version, revision: source, selected_target: target, targets: [target], source_sha256: inventory.source_sha256, source_files: inventory.source_files, artifacts, build_inputs };
    const generationBytes = JSON.stringify(generation);
    const meta = { ...generation, schema: "acyclic.actors.native-targets.v1", source_revision: source, artifact: artifacts[2], generation_sha256: `sha256:${hash(generationBytes)}` };
    const entry = { name, version, asset: "actors-native/companion.tgz", selected_target: target, os: ["fixture"], cpu: ["fixture"], artifact: meta.artifact, generation_sha256: meta.generation_sha256 };
    await mkdir(join(parent, "generated/wasm"), { recursive: true });
    const wasm = Buffer.from("synthetic module bytes; receipt admission only");
    await writeFile(join(parent, "generated/wasm/fixture_bg.wasm"), wasm);
    const wasmReceipt = { schema: "acyclic.wasm-build-receipt.v1", family: "actors", package: "acyclic-actors-wasm", version, source_commit: source, source_sha256: inventory.source_sha256, source_files: inventory.source_files, build: { cargo: { version: "synthetic cargo" }, rustc: { invocation: { source: "rustc-invocation", rustc: "fixture-rustc", target: "wasm32-unknown-unknown" }, identity: { command: "fixture-rustc", output: "synthetic rustc", executable_sha256: `sha256:${"f".repeat(64)}` } }, wasm_bindgen: { version: "synthetic bindgen" }, module_sha256: `sha256:${hash(wasm)}` }, artifacts: [{ path: "generated/wasm/fixture_bg.wasm", bytes: wasm.length, sha256: `sha256:${hash(wasm)}` }] };
    const wasmReceiptBytes = JSON.stringify(wasmReceipt);
    await writeFile(join(parent, "generated/wasm/producer-receipt.json"), wasmReceiptBytes);
    const index = { schema: "acyclic.actors.native-package-assembly.v2", source_commit: source, source_sha256: inventory.source_sha256, targets: [target], companions: [entry], wasm_receipt_sha256: `sha256:${hash(wasmReceiptBytes)}` };
    const parentReceipt = { name: "@acyclic-labs/actors", version, asset: "acyclic-labs-actors-0.2.0.tgz" };
    await json(join(parent, "package.json"), parentManifest);
    await json(join(companion, "package.json"), { name, version, private: false, main, os: entry.os, cpu: entry.cpu });
    for (const path of ["binding.cjs", "binding.d.ts"]) await writeFile(join(parent, "generated/native", path), path);
    await writeFile(join(companion, main), main);
    for (const [path, bytes] of [["native-targets.json", JSON.stringify(meta)], ["generation-manifest.json", generationBytes], ["producer-receipt.json", JSON.stringify(compilerReceipt)]]) {
      await writeFile(join(companion, path), bytes);
      await writeFile(join(parent, "generated/native/attestations", target, path), bytes);
    }
    const seal = async () => {
      pack("companion", entry.asset); entry.sha256 = hash(await readFile(join(output, entry.asset)));
      await json(join(parent, "generated/native/native-targets.json"), index);
      pack("parent", parentReceipt.asset); parentReceipt.sha256 = hash(await readFile(join(output, parentReceipt.asset)));
      await json(join(output, "ACTORS_NATIVE_PACKAGE.json"), { ...index, parent: parentReceipt });
      await writeFile(join(output, "SHA256SUMS"), [parentReceipt, entry].map(item => `${item.sha256}  ${item.asset}`).join("\n"));
    };
    const verify = () => verifyNativeAssembly(output, source, version, inventory);
    await seal(); await verify();
    await writeFile(join(parent, "generated/wasm/fixture_bg.wasm"), "tampered WASM"); await seal();
    await assert.rejects(verify(), /WASM artifact digest differs/);
    await writeFile(join(parent, "generated/wasm/fixture_bg.wasm"), wasm);
    await writeFile(join(parent, "generated/wasm/extra.js"), "unattested"); await seal();
    await assert.rejects(verify(), /WASM file inventory differs/);
    await rm(join(parent, "generated/wasm/extra.js")); await seal(); await verify();
    for (const mutation of [{ source_commit: "b".repeat(40) }, { source_sha256: "sha256:other" }, { source_files: [] }]) {
      const bytes = JSON.stringify({ ...wasmReceipt, ...mutation });
      await writeFile(join(parent, "generated/wasm/producer-receipt.json"), bytes);
      index.wasm_receipt_sha256 = `sha256:${hash(bytes)}`;
      await seal();
      await assert.rejects(verify(), /WASM receipt source or package identity differs/);
    }
    await writeFile(join(parent, "generated/wasm/producer-receipt.json"), wasmReceiptBytes);
    index.wasm_receipt_sha256 = `sha256:${hash(wasmReceiptBytes)}`;
    await seal(); await verify();
    for (const mutation of [{ scripts: { install: "node -e process.exit(1)" } }, { dependencies: { "unqualified-runtime-dependency": "*" } }]) {
      await json(join(parent, "package.json"), { ...parentManifest, ...mutation }); await seal();
      await assert.rejects(verify(), /neutral parent manifest/);
    }
    await json(join(parent, "package.json"), parentManifest); await seal(); await verify();
    // Rehash both archives and receipts so admission cannot rely on outer digests.
    for (const mutation of [{ scripts: { install: "node -e process.exit(1)" } }, { dependencies: { "unqualified-runtime-dependency": "*" } }]) {
      await json(join(companion, "package.json"), { ...companionManifest, ...mutation }); await seal();
      await assert.rejects(verify(), /maintained source target mapping/);
    }
    await json(join(companion, "package.json"), companionManifest);
    await writeFile(join(parent, "generated/native/unqualified-receipt.json"), "{}"); await seal();
    await assert.rejects(verify(), /neutral parent native file inventory differs/);
    await rm(join(parent, "generated/native/unqualified-receipt.json")); await seal(); await verify();
    await assert.rejects(verifyNativeAssembly(output, source, version), /trusted native source inventory/);
    await writeFile(join(companion, main), "tampered binary"); await seal();
    await assert.rejects(verify(), /original native artifact digest differs/);
    await writeFile(join(companion, main), main);
    await writeFile(join(parent, "generated/native/binding.cjs"), "changed loader"); await seal();
    await assert.rejects(verify(), /original native artifact digest differs/);
    await writeFile(join(parent, "generated/native/binding.cjs"), "binding.cjs");
    await json(join(parent, "package.json"), { ...parentManifest, optionalDependencies: {} }); await seal();
    await assert.rejects(verify(), /neutral parent manifest/);
    await json(join(parent, "package.json"), parentManifest);
    await writeFile(join(companion, "unqualified.node"), ""); await seal();
    await assert.rejects(verify(), /unstated files/);
    await rm(join(companion, "unqualified.node")); await seal(); await verify();
    await writeFile(join(output, "actors-native/stale.tgz"), "stale");
    await assert.rejects(verify(), /missing or extra files/);
    await rm(join(output, "actors-native/stale.tgz"));
    await rm(join(output, entry.asset));
    await assert.rejects(verify(), { code: "ENOENT" });
  } finally { await rm(directory, { recursive: true, force: true }); }
});
