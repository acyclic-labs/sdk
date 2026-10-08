import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, mkdir, readFile, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFileSync } from "node:child_process";
import test from "node:test";
import { assertNativeReceipt, assertNativeSet, verifyNativeAssembly } from "./assemble-stream-native-package.mjs";
import { normalizeBuildInputs } from "./build-stream-native.mjs";

const sha = `sha256:${"a".repeat(64)}`;
const revision = "1".repeat(40);
const target = "x86_64-pc-windows-msvc";
const hash = value => `sha256:${createHash("sha256").update(JSON.stringify(value)).digest("hex")}`;
function inputs() {
  return {
    schema: "acyclic.stream.native-build-inputs.v3", target, target_dir: "C:/target",
    runtime: { node: "24.15.0", node_path: "node", platform: "win32", arch: "x64", bun: { maintained: "1.4.2", actual: null } },
    invocation: { script: "scripts/build-stream-native.mjs", runtime: "node", args: ["build"] },
    compiler: { rustc: { command: "rustc", args: ["--version", "--verbose"], output: `rustc 1.98.1\nhost: ${target}`, executable_sha256: sha }, cargo: { command: "cargo", args: ["--version", "--verbose"], output: "cargo 1.98.1" } },
    generator: { package: "@napi-rs/cli", version: "3.10.5", package_sha256: sha, entry_sha256: sha, lock_sha256: sha, options: { release: true, platform: true, target, output_dir: "C:/native", target_dir: "C:/target", js_package_name: "@acyclic-labs/stream", js_binding: "binding.cjs", dts: "binding.d.ts", cargo_options: ["--locked"] } },
    linker: { configured: { target: null }, environment: { LINK: null, CC: null, AR: null, RUSTC_LINKER: null, DYLD_LIBRARY_PATH: null, SDKROOT: null, VCINSTALLDIR: null, VCToolsInstallDir: null, WindowsSdkDir: null, VisualStudioVersion: null }, actual: { source: "rustc-invocation", rustc: "rustc", target, linker: null, args: ["--crate-name", "acyclic_stream_napi", "--emit=dep-info,link", "--remap-path-prefix=C:/producer/sdk=/__acyclic_stream_source"] } },
    profile: { name: "release", cargo_incremental: "0", release_incremental: "false", manifest_sha256: sha, config_sha256: sha },
    environment: { RUSTFLAGS: null, CARGO_ENCODED_RUSTFLAGS: null, RUSTC_WRAPPER: null, RUSTC_WORKSPACE_WRAPPER: null, CARGO_TARGET_DIR: null },
    cache: { wrapper: null, wrapper_version: null, directory: null, size: null },
  };
}
function metadata(selected_target = target) {
  const raw = inputs();
  return { selected_target, source_revision: revision, source_sha256: sha, source_files: [{ path: "source.rs", sha256: sha, bytes: 1 }], version: "0.2.0", targets: [target, "aarch64-apple-darwin"], build_inputs: normalizeBuildInputs(raw, { sourceRoot: "C:/producer/sdk", platform: "win32", targetDir: raw.target_dir, outputDir: raw.generator.options.output_dir }) };
}

test("neutral assembly requires complete unique Rust targets and a common source closure", () => {
  const bundles = [metadata(), metadata("aarch64-apple-darwin")];
  assertNativeSet(bundles, bundles[0].targets, revision);
  assert.throws(() => assertNativeSet(bundles.slice(0, 1), bundles[0].targets, revision), /exactly one bundle/);
  assert.throws(() => assertNativeSet([bundles[0], bundles[0]], bundles[0].targets, revision), /exactly one bundle/);
  for (const field of ["source_revision", "source_sha256", "source_files", "version", "targets"]) {
    const changed = structuredClone(bundles);
    Object.assign(changed[1], { [field]: "different" });
    assert.throws(() => assertNativeSet(changed, bundles[0].targets, revision), /differs/);
  }
});

test("publication verifies all companion archives and retained attestations before accepting inventory", async () => {
  const directory = await mkdtemp(join(tmpdir(), "stream-native-assembly-test-"));
  const parent = join(directory, "parent/package"), companion = join(directory, "companion/package"), output = join(directory, "out");
  const bytesHash = value => createHash("sha256").update(value).digest("hex");
  const put = async (path, bytes) => { await mkdir(join(path, ".."), { recursive: true }); await writeFile(path, bytes); };
  const json = (path, value) => put(path, JSON.stringify(value));
  const companionName = "@acyclic-labs/stream-win32-x64-msvc";
  const addon = "index.win32-x64-msvc.node";
  const artifacts = ["binding.cjs", "binding.d.ts", addon].map(name => ({ path: `generated/native/${name}`, sha256: `sha256:${bytesHash(name)}`, bytes: Buffer.byteLength(name) }));
  const meta = { ...metadata(), schema: "acyclic.stream.native-targets.v1", targets: [target], artifacts, artifact: artifacts[2] };
  const generation = { ...meta, schema: "acyclic.stream.native-generation.v1", revision };
  const generationBytes = JSON.stringify(generation);
  Object.assign(meta, { generation_sha256: `sha256:${bytesHash(generationBytes)}` });
  const receipt = { schema: "acyclic.stream.native-build-inputs-receipt.v1", published_build_inputs_sha256: hash(meta.build_inputs), raw_build_inputs: inputs() };
  const entry = { name: companionName, version: "0.2.0", asset: "native/companion.tgz", selected_target: target, os: ["win32"], cpu: ["x64"], artifact: meta.artifact, sha256: "", metadata_sha256: bytesHash(JSON.stringify(meta)), receipt_sha256: bytesHash(JSON.stringify(receipt)) };
  const expectedInventory = { schema: "acyclic.stream.native-source-inventory.v1", source_commit: revision, parent: { name: "@acyclic-labs/stream", version: "0.2.0", private: false }, targets: [target], generator: Object.fromEntries(["package", "version", "package_sha256", "entry_sha256", "lock_sha256"].map(field => [field, meta.build_inputs.generator[field]])), companions: [{ selected_target: target, name: companionName, main: addon, os: ["win32"], cpu: ["x64"] }] };
  try {
    await mkdir(join(output, "native"), { recursive: true });
    for (const name of ["binding.cjs", "binding.d.ts"]) await put(join(parent, "generated/native", name), name);
    await put(join(companion, addon), addon);
    for (const [name, bytes] of [["native-targets.json", JSON.stringify(meta)], ["generation-manifest.json", generationBytes], ["producer-receipt.json", JSON.stringify(receipt)]]) {
      await put(join(companion, name), bytes);
      await put(join(parent, "generated/native/attestations", target, name), bytes);
    }
    await json(join(companion, "package.json"), { name: companionName, version: "0.2.0", private: false, main: addon, os: entry.os, cpu: entry.cpu });
    await json(join(parent, "package.json"), { name: "@acyclic-labs/stream", version: "0.2.0", private: false, optionalDependencies: { [companionName]: "0.2.0" } });
    const pack = (source, asset) => execFileSync("tar", ["-czf", join(output, asset), "-C", source, "package"], { env: { ...process.env, COPYFILE_DISABLE: "1" } });
    pack(join(directory, "companion"), entry.asset);
    entry.sha256 = bytesHash(await readFile(join(output, entry.asset)));
    const index = { schema: "acyclic.stream.native-package-assembly.v2", source_commit: revision, source_sha256: sha, targets: [target], companions: [entry] };
    await json(join(parent, "generated/native/native-targets.json"), index);
    const parentAsset = "acyclic-labs-stream-0.2.0.tgz";
    pack(join(directory, "parent"), parentAsset);
    const parentReceipt = { name: "@acyclic-labs/stream", version: "0.2.0", asset: parentAsset, sha256: bytesHash(await readFile(join(output, parentAsset))) };
    await json(join(output, "STREAM_NATIVE_PACKAGE.json"), { ...index, parent: parentReceipt });
    await put(join(output, "SHA256SUMS"), `${parentReceipt.sha256}  ${parentAsset}\n${entry.sha256}  ${entry.asset}\n`);
    await verifyNativeAssembly(output, revision, "0.2.0", expectedInventory);
    await assert.rejects(verifyNativeAssembly(output, revision, "0.2.0"), /trusted native source inventory/);
    await assert.rejects(verifyNativeAssembly(output, revision, "0.2.0", { ...expectedInventory, targets: [target, "aarch64-apple-darwin"] }), /trusted Rust source inventory/);
    const originalParent = JSON.parse(await readFile(join(parent, "package.json"), "utf8"));
    for (const [field, value] of Object.entries({ name: "@wrong/parent", version: "9.9.9", private: true })) {
      await json(join(parent, "package.json"), { ...originalParent, [field]: value });
      pack(join(directory, "parent"), parentAsset);
      parentReceipt.sha256 = bytesHash(await readFile(join(output, parentAsset)));
      await json(join(output, "STREAM_NATIVE_PACKAGE.json"), { ...index, parent: parentReceipt });
      await assert.rejects(verifyNativeAssembly(output, revision, "0.2.0", expectedInventory), /parent manifest differs/);
    }
    await json(join(parent, "package.json"), originalParent);
    pack(join(directory, "parent"), parentAsset);
    parentReceipt.sha256 = bytesHash(await readFile(join(output, parentAsset)));
    await json(join(output, "STREAM_NATIVE_PACKAGE.json"), { ...index, parent: parentReceipt });
    await put(join(output, "SHA256SUMS"), `${parentReceipt.sha256}  ${parentAsset}\n${entry.sha256}  ${entry.asset}\n`);
    const originalCompanion = JSON.parse(await readFile(join(companion, "package.json"), "utf8"));
    const originalEntry = { ...entry };
    const seal = async () => {
      pack(join(directory, "companion"), entry.asset);
      entry.sha256 = bytesHash(await readFile(join(output, entry.asset)));
      await json(join(parent, "generated/native/native-targets.json"), index);
      pack(join(directory, "parent"), parentAsset);
      parentReceipt.sha256 = bytesHash(await readFile(join(output, parentAsset)));
      await json(join(output, "STREAM_NATIVE_PACKAGE.json"), { ...index, parent: parentReceipt });
      await put(join(output, "SHA256SUMS"), `${parentReceipt.sha256}  ${parentAsset}\n${entry.sha256}  ${entry.asset}\n`);
    };
    for (const mutation of [{ name: "@acyclic-labs/stream-darwin-arm64", os: ["darwin"], cpu: ["arm64"] }, { main: "index.darwin-arm64.node" }, { libc: ["musl"] }]) {
      Object.assign(entry, originalEntry, mutation);
      await json(join(companion, "package.json"), { ...originalCompanion, ...mutation });
      await json(join(parent, "package.json"), { ...originalParent, optionalDependencies: { [entry.name]: "0.2.0" } });
      await seal();
      await assert.rejects(verifyNativeAssembly(output, revision, "0.2.0", expectedInventory), /maintained source target mapping/);
      for (const field of Object.keys(mutation)) delete entry[field];
    }
    Object.assign(entry, originalEntry);
    await json(join(companion, "package.json"), originalCompanion);
    await json(join(parent, "package.json"), originalParent);
    await seal();
    await verifyNativeAssembly(output, revision, "0.2.0", expectedInventory);
    await assert.rejects(verifyNativeAssembly(output, "2".repeat(40), "0.2.0", expectedInventory), /release/);
    await put(join(output, "native/stale.tgz"), "stale");
    await assert.rejects(verifyNativeAssembly(output, revision, "0.2.0", expectedInventory), /missing or extra files/);
    await rm(join(output, "native/stale.tgz"));
    const saved = await readFile(join(output, entry.asset));
    await put(join(output, entry.asset), "corrupt");
    await assert.rejects(verifyNativeAssembly(output, revision, "0.2.0", expectedInventory), /archive digest differs/);
    await put(join(output, entry.asset), saved);
    await put(join(output, "SHA256SUMS"), `${parentReceipt.sha256}  ${parentAsset}\n`);
    await assert.rejects(verifyNativeAssembly(output, revision, "0.2.0", expectedInventory), /checksum inventory/);
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test("neutral assembly binds original compiler receipts to published inputs", () => {
  const meta = metadata();
  const receipt = { schema: "acyclic.stream.native-build-inputs-receipt.v1", published_build_inputs_sha256: hash(meta.build_inputs), raw_build_inputs: inputs() };
  assertNativeReceipt(receipt, meta);
  const wrongHash = { ...receipt, published_build_inputs_sha256: `sha256:${"b".repeat(64)}` };
  assert.throws(() => assertNativeReceipt(wrongHash, meta), /published build inputs/);
  for (const field of ["output", "executable_sha256"]) {
    const changed = structuredClone(receipt);
    Object.assign(changed.raw_build_inputs.compiler.rustc, { [field]: field === "output" ? `rustc 1.98.2\nhost: ${target}` : `sha256:${"b".repeat(64)}` });
    assert.throws(() => assertNativeReceipt(changed, meta), /reconstructed build inputs differ/);
  }
  const changed = structuredClone(receipt);
  changed.raw_build_inputs.linker.actual.rustc = "other-rustc";
  assert.throws(() => assertNativeReceipt(changed, meta), /captured compiler or target/);
  for (const mutate of [
    value => value.linker.actual.args.push("-Clink-arg=changed"),
    value => { value.profile.manifest_sha256 = `sha256:${"b".repeat(64)}`; },
    value => { value.environment.RUSTFLAGS = "-C opt-level=2"; },
  ]) {
    const altered = structuredClone(receipt);
    mutate(altered.raw_build_inputs);
    assert.throws(() => assertNativeReceipt(altered, meta), /reconstructed build inputs differ/);
  }
  for (const roots of [[], ["relative"], ["C:/producer/sdk", "C:/other/sdk"]]) {
    const altered = structuredClone(receipt);
    altered.raw_build_inputs.linker.actual.args = altered.raw_build_inputs.linker.actual.args.filter(arg => !arg.startsWith("--remap-path-prefix="));
    altered.raw_build_inputs.linker.actual.args.push(...roots.map(root => `--remap-path-prefix=${root}=/__acyclic_stream_source`));
    assert.throws(() => assertNativeReceipt(altered, meta), /one absolute producer source root/);
  }
});
