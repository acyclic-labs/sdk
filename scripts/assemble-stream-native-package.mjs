import { createHash } from "node:crypto";
import { copyFileSync, existsSync, readFileSync } from "node:fs";
import { cp, mkdir, mkdtemp, readdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import { gunzipSync } from "node:zlib";
import { tarEntries } from "./archive-utils.mjs";
import { spawnSync } from "node:child_process";
import { assertBuildInputs, assertBundle, assertSourceSnapshot, normalizeBuildInputs, rustMetadata, sourceSnapshot } from "./build-stream-native.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const require = createRequire(import.meta.url);

/** @returns {never} */
function fail(message) {
  throw new Error(message);
}

function run(command, args, options = {}) {
  const npmCli = join(dirname(process.execPath), "node_modules/npm/bin/npm-cli.js");
  const executable = process.platform === "win32" && command === "npm" && existsSync(npmCli) ? process.execPath : command;
  const commandArgs = executable === process.execPath && command === "npm" ? [npmCli, ...args] : args;
  const result = spawnSync(executable, commandArgs, {
    cwd: options.cwd ?? root,
    encoding: "utf8",
    stdio: options.stdio ?? ["ignore", "pipe", "pipe"],
    windowsHide: true,
  });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(" ")} failed: ${(result.stderr || result.stdout || "").trim()}`);
  }
  return result.stdout ?? "";
}

async function nativeInputs(directory, /** @type {{bundles: string[], receipts: string[]}} */ found = { bundles: [], receipts: [] }) {
  const entries = await readdir(directory, { withFileTypes: true });
  for (const entry of entries) {
    const path = join(directory, entry.name);
    if (entry.isSymbolicLink()) fail(`native input is a symbolic link: ${path}`);
    if (entry.isFile() && entry.name === "native-targets.json") found.bundles.push(dirname(path));
    if (entry.isFile() && ["producer-receipt.json", "stream-native-build-inputs.receipt.json"].includes(entry.name)) found.receipts.push(path);
    if (entry.isDirectory()) await nativeInputs(path, found);
  }
  return found;
}

function digest(path) {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}

const objectDigest = value => `sha256:${createHash("sha256").update(JSON.stringify(value)).digest("hex")}`;

export function assertNativeSet(metadata, targets, sourceSha) {
  const selected = metadata.map(item => item.selected_target);
  if (selected.length !== targets.length || new Set(selected).size !== selected.length || targets.some(target => !selected.includes(target))) fail("native assembly requires exactly one bundle for every Rust target");
  const first = metadata[0];
  for (const item of metadata) {
    if (item.source_revision !== sourceSha) fail("native bundle source revision differs from release source");
    for (const field of ["source_sha256", "source_files", "version", "targets"]) {
      if (JSON.stringify(item[field]) !== JSON.stringify(first[field])) fail(`native bundle ${field} differs across targets`);
    }
    for (const field of ["package", "version", "package_sha256", "entry_sha256", "lock_sha256"]) {
      if (item.build_inputs.generator[field] !== first.build_inputs.generator[field]) fail(`native generator ${field} differs across targets`);
    }
  }
}

export function assertNativeReceipt(receipt, metadata) {
  if (receipt.schema !== "acyclic.stream.native-build-inputs-receipt.v1" || receipt.published_build_inputs_sha256 !== objectDigest(metadata.build_inputs)) fail("native receipt does not attest the published build inputs");
  const raw = receipt.raw_build_inputs;
  assertBuildInputs(raw);
  if (raw.target !== metadata.selected_target || raw.compiler.rustc.command !== raw.linker.actual.rustc) fail("native receipt captured compiler or target differs");
  const prefix = "--remap-path-prefix=";
  const destination = "=/__acyclic_stream_source";
  const roots = [...new Set(raw.linker.actual.args.filter(arg => arg.startsWith(prefix) && arg.endsWith(destination)).map(arg => arg.slice(prefix.length, -destination.length)))];
  if (roots.length !== 1 || !/^(?:[A-Za-z]:[\\/]|\/)/u.test(roots[0])) fail("native receipt must capture one absolute producer source root");
  const reconstructed = normalizeBuildInputs(raw, { sourceRoot: roots[0], platform: raw.runtime.platform, targetDir: raw.target_dir, outputDir: raw.generator.options.output_dir });
  if (JSON.stringify(reconstructed) !== JSON.stringify(metadata.build_inputs) || objectDigest(reconstructed) !== receipt.published_build_inputs_sha256) fail("native receipt reconstructed build inputs differ from published recipe");
}

function archiveFiles(path) {
  const files = new Map();
  for (const entry of tarEntries(gunzipSync(readFileSync(path)))) {
    if (!entry.path.startsWith("package/") || entry.path.split("/").some(part => part === ".." || part === ".") || !["0", "\0", "5"].includes(entry.type) || files.has(entry.path)) fail("native package archive has an unsafe or duplicate entry");
    files.set(entry.path, entry.body);
  }
  return files;
}

export async function sourceNativeInventory(sourceSha) {
  if (run("git", ["rev-parse", "HEAD"]).trim() !== sourceSha) fail("native inventory source differs from checkout");
  run("git", ["diff", "--quiet", "HEAD", "--", "rust/crates/stream-napi/Cargo.toml", "typescript/packages/stream/package.json", "bun.lock"]);
  const source = await sourceSnapshot();
  const { rustPackage, targets } = rustMetadata();
  const manifest = JSON.parse(await readFile(join(root, "typescript/packages/stream/package.json"), "utf8"));
  if (manifest.version !== rustPackage.version || manifest.private !== false) fail("native source package identity differs from Rust");
  const cliPackage = JSON.parse(await readFile(require.resolve("@napi-rs/cli/package.json"), "utf8"));
  const generator = { package: cliPackage.name, version: cliPackage.version, package_sha256: `sha256:${digest(require.resolve("@napi-rs/cli/package.json"))}`, entry_sha256: `sha256:${digest(require.resolve("@napi-rs/cli"))}`, lock_sha256: `sha256:${digest(join(root, "bun.lock"))}` };
  const temporary = await mkdtemp(join(tmpdir(), "acyclic-stream-native-inventory-"));
  try {
    await writeFile(join(temporary, "package.json"), JSON.stringify({ ...manifest, napi: { ...manifest.napi, targets } }));
    const { NapiCli, parseTriple } = await import("@napi-rs/cli");
    const npmDir = join(temporary, "npm");
    await new NapiCli().createNpmDirs({ cwd: temporary, npmDir });
    const companions = await Promise.all(targets.map(async selected_target => {
      const generated = JSON.parse(await readFile(join(npmDir, parseTriple(selected_target).platformArchABI, "package.json"), "utf8"));
      return { selected_target, name: generated.name, main: generated.main, os: generated.os, cpu: generated.cpu, libc: generated.libc };
    }));
    await assertSourceSnapshot(source);
    if (run("git", ["rev-parse", "HEAD"]).trim() !== sourceSha) fail("native inventory source changed during generation");
    return { schema: "acyclic.stream.native-source-inventory.v1", source_commit: sourceSha, parent: { name: manifest.name, version: manifest.version, private: manifest.private }, targets, generator, companions };
  } finally { await rm(temporary, { recursive: true, force: true }); }
}

export async function verifyNativeAssembly(output, sourceSha, version, expectedInventory) {
  if (!expectedInventory || expectedInventory.schema !== "acyclic.stream.native-source-inventory.v1" || expectedInventory.source_commit !== sourceSha || expectedInventory.parent.version !== version || expectedInventory.parent.private !== false) fail("trusted native source inventory is required for release");
  const assembly = JSON.parse(await readFile(join(output, "STREAM_NATIVE_PACKAGE.json"), "utf8"));
  if (assembly.schema !== "acyclic.stream.native-package-assembly.v2" || assembly.source_commit !== sourceSha || assembly.parent.version !== version || assembly.parent.name !== "@acyclic-labs/stream") fail("native assembly release identity differs");
  const parentAsset = `acyclic-labs-stream-${version}.tgz`;
  if (assembly.parent.asset !== parentAsset || digest(join(output, parentAsset)) !== assembly.parent.sha256) fail("native assembly parent digest differs");
  const parent = archiveFiles(join(output, parentAsset));
  const neutral = JSON.parse(parent.get("package/package.json").toString("utf8"));
  if (neutral.name !== assembly.parent.name || neutral.version !== version || neutral.private !== false) fail("neutral parent manifest differs from release identity");
  if (neutral.name !== expectedInventory.parent.name || JSON.stringify(assembly.targets) !== JSON.stringify(expectedInventory.targets)) fail("native assembly differs from trusted Rust source inventory");
  const { parent: ignored, ...index } = assembly;
  if (JSON.stringify(JSON.parse(parent.get("package/generated/native/native-targets.json").toString("utf8"))) !== JSON.stringify(index)) fail("neutral parent assembly index differs");
  if ([...parent.keys()].some(path => path.endsWith(".node"))) fail("neutral parent contains a native binary");
  const metadata = [];
  const dependencies = {};
  for (const entry of assembly.companions) {
    if (!/^native\/[^/\\]+\.tgz$/u.test(entry.asset) || entry.version !== version || !entry.name.startsWith(`${assembly.parent.name}-`)) fail("native companion identity or path differs");
    const archive = join(output, entry.asset);
    if (digest(archive) !== entry.sha256) fail("native companion archive digest differs");
    const files = archiveFiles(archive);
    const manifest = JSON.parse(files.get("package/package.json").toString("utf8"));
    const expected = expectedInventory.companions.find(item => item.selected_target === entry.selected_target);
    if (!expected || ["name", "main", "os", "cpu", "libc"].some(field => JSON.stringify(manifest[field]) !== JSON.stringify(expected[field]))) fail("native companion differs from maintained source target mapping");
    if (manifest.name !== entry.name || manifest.version !== version || manifest.private !== false || JSON.stringify(manifest.os) !== JSON.stringify(entry.os) || JSON.stringify(manifest.cpu) !== JSON.stringify(entry.cpu) || JSON.stringify(manifest.libc) !== JSON.stringify(entry.libc)) fail("native companion manifest differs");
    const originals = {};
    for (const name of ["native-targets.json", "generation-manifest.json", "producer-receipt.json"]) {
      const bytes = files.get(`package/${name}`);
      const retained = parent.get(`package/generated/native/attestations/${entry.selected_target}/${name}`);
      if (!bytes || !retained || !bytes.equals(retained)) fail("original native attestation differs between parent and companion");
      originals[name] = JSON.parse(bytes.toString("utf8"));
    }
    const meta = originals["native-targets.json"];
    if (Object.keys(expectedInventory.generator).some(field => meta.build_inputs.generator[field] !== expectedInventory.generator[field])) fail("native companion generator differs from trusted source inventory");
    const generation = originals["generation-manifest.json"];
    if (meta.schema !== "acyclic.stream.native-targets.v1" || generation.schema !== "acyclic.stream.native-generation.v1" || meta.version !== version || generation.version !== version || meta.selected_target !== entry.selected_target || generation.selected_target !== entry.selected_target || meta.source_revision !== sourceSha || generation.revision !== sourceSha || meta.source_sha256 !== assembly.source_sha256 || generation.source_sha256 !== assembly.source_sha256 || JSON.stringify(meta.targets) !== JSON.stringify(assembly.targets) || JSON.stringify(generation.targets) !== JSON.stringify(assembly.targets) || JSON.stringify(meta.source_files) !== JSON.stringify(generation.source_files) || JSON.stringify(meta.build_inputs) !== JSON.stringify(generation.build_inputs) || JSON.stringify(meta.artifact) !== JSON.stringify(entry.artifact)) fail("native companion original source or target differs");
    if (!meta.artifact.path.endsWith(".node") || JSON.stringify(meta.artifacts) !== JSON.stringify(generation.artifacts) || JSON.stringify(meta.artifacts.map(artifact => artifact.path).sort()) !== JSON.stringify(["generated/native/binding.cjs", "generated/native/binding.d.ts", meta.artifact.path].sort()) || JSON.stringify(meta.artifacts.find(artifact => artifact.path === meta.artifact.path)) !== JSON.stringify(meta.artifact)) fail("native companion artifact inventory differs");
    assertNativeReceipt(originals["producer-receipt.json"], meta);
    const bytesHash = bytes => createHash("sha256").update(bytes).digest("hex");
    if (bytesHash(files.get("package/native-targets.json")) !== entry.metadata_sha256 || bytesHash(files.get("package/producer-receipt.json")) !== entry.receipt_sha256 || `sha256:${bytesHash(files.get("package/generation-manifest.json"))}` !== meta.generation_sha256) fail("original native attestation digest differs");
    for (const artifact of meta.artifacts) {
      const name = artifact.path.slice("generated/native/".length);
      const bytes = name.endsWith(".node") ? files.get(`package/${name}`) : parent.get(`package/generated/native/${name}`);
      if (!bytes || bytes.length !== artifact.bytes || `sha256:${bytesHash(bytes)}` !== artifact.sha256) fail("original native artifact digest differs");
    }
    if (manifest.main !== meta.artifact.path.split("/").at(-1) || [...files.keys()].filter(path => path.endsWith(".node")).length !== 1) fail("native companion does not contain its selected addon");
    dependencies[entry.name] = version;
    metadata.push(meta);
  }
  assertNativeSet(metadata, expectedInventory.targets, sourceSha);
  if (JSON.stringify(neutral.optionalDependencies) !== JSON.stringify(dependencies)) fail("neutral parent optional dependencies differ");
  const observed = (await readdir(join(output, "native"))).sort();
  const expected = assembly.companions.map(entry => entry.asset.slice("native/".length)).sort();
  if (JSON.stringify(observed) !== JSON.stringify(expected)) fail("native publication inventory contains missing or extra files");
  const checksums = new Map((await readFile(join(output, "SHA256SUMS"), "utf8")).trim().split("\n").map(line => { const [sha256, asset] = line.trim().split(/  /u); return [asset, sha256]; }));
  for (const entry of [assembly.parent, ...assembly.companions]) {
    if (checksums.get(entry.asset) !== entry.sha256) fail("native assembly checksum inventory differs");
  }
  return assembly;
}

async function main() {
  if (process.argv[2] === "inventory") {
    const [, output, sourceSha] = process.argv.slice(2);
    if (!output || !/^[0-9a-f]{40}$/u.test(sourceSha ?? "")) fail("usage: assemble-stream-native-package.mjs inventory OUTPUT SOURCE_SHA");
    await writeFile(resolve(output), `${JSON.stringify(await sourceNativeInventory(sourceSha), null, 2)}\n`);
    return;
  }
  const [typescriptOutput, bundleArgument, sourceSha] = process.argv.slice(2);
  if (!typescriptOutput || !bundleArgument || !/^[0-9a-f]{40}$/.test(sourceSha ?? "")) {
    fail("usage: assemble-stream-native-package.mjs TYPESCRIPT_OUTPUT NATIVE_INPUT_ROOT SOURCE_SHA");
  }
  const expectedSource = run("git", ["rev-parse", "HEAD"]).trim();
  if (expectedSource !== sourceSha) fail(`native package source ${sourceSha} differs from checkout ${expectedSource}`);

  const output = resolve(typescriptOutput);
  const bundleArgumentPath = resolve(bundleArgument);
  const source = await sourceSnapshot();
  const inputs = await nativeInputs(bundleArgumentPath);
  const expectedInventory = await sourceNativeInventory(sourceSha);
  const targets = expectedInventory.targets;
  const bundles = await Promise.all(inputs.bundles.map(async path => ({ path, receipt: "", ...await assertBundle(path) })));
  assertNativeSet(bundles.map(item => item.metadata), targets, sourceSha);
  const generator = bundles[0].metadata.build_inputs.generator;
  const loadedCli = JSON.parse(await readFile(require.resolve("@napi-rs/cli/package.json"), "utf8"));
  if (generator.package !== loadedCli.name || generator.version !== loadedCli.version) fail("assembly NAPI package or version differs from producer");
  for (const [field, path] of [["package_sha256", require.resolve("@napi-rs/cli/package.json")], ["entry_sha256", require.resolve("@napi-rs/cli")], ["lock_sha256", join(root, "bun.lock")]]) {
    if (`sha256:${digest(path)}` !== generator[field]) fail(`assembly NAPI generator ${field} differs from producer`);
  }
  const receipts = await Promise.all(inputs.receipts.map(async path => ({ path, value: JSON.parse(await readFile(path, "utf8")) })));
  for (const bundle of bundles) {
    const matching = receipts.filter(item => item.value.published_build_inputs_sha256 === objectDigest(bundle.metadata.build_inputs));
    if (matching.length !== 1) fail(`native target ${bundle.metadata.selected_target} requires one original compiler receipt`);
    assertNativeReceipt(matching[0].value, bundle.metadata);
    bundle.receipt = matching[0].path;
  }

  const packageManifest = JSON.parse(await readFile(join(root, "typescript/packages/stream/package.json"), "utf8"));
  const archive = join(output, `acyclic-labs-stream-${packageManifest.version}.tgz`);
  await stat(archive);
  const temporary = await mkdtemp(join(tmpdir(), "acyclic-stream-native-package-"));
  try {
    const unpacked = join(temporary, "unpacked");
    const repacked = join(temporary, "repacked");
    await mkdir(unpacked, { recursive: true });
    await mkdir(repacked, { recursive: true });
    archiveFiles(archive);
    run("tar", ["-xzf", archive, "-C", unpacked]);
    const packageRoot = join(unpacked, "package");
    const native = join(packageRoot, "generated/native");
    await rm(native, { recursive: true, force: true });
    await mkdir(native, { recursive: true });
    for (const name of ["binding.cjs", "binding.d.ts"]) {
      const sha = digest(join(bundles[0].path, name));
      if (bundles.some(bundle => digest(join(bundle.path, name)) !== sha)) fail(`native ${name} differs across targets`);
      await cp(join(bundles[0].path, name), join(native, name));
    }
    const artifacts = join(temporary, "artifacts");
    await mkdir(artifacts);
    for (const bundle of bundles) await cp(join(bundle.path, bundle.metadata.artifact.path.slice("generated/native/".length)), join(artifacts, bundle.metadata.artifact.path.split("/").at(-1)));
    const neutralManifest = JSON.parse(await readFile(join(packageRoot, "package.json"), "utf8"));
    if (neutralManifest.name !== packageManifest.name || neutralManifest.version !== packageManifest.version) fail("neutral parent identity differs from source package");
    neutralManifest.napi = { ...neutralManifest.napi, targets };
    await writeFile(join(packageRoot, "package.json"), `${JSON.stringify(neutralManifest, null, 2)}\n`);
    const { NapiCli } = await import("@napi-rs/cli");
    const npmDir = join(packageRoot, "npm");
    const napi = new NapiCli();
    await napi.createNpmDirs({ cwd: packageRoot, npmDir });
    await napi.artifacts({ cwd: packageRoot, npmDir, outputDir: artifacts });
    neutralManifest.optionalDependencies = {};
    const companions = [];
    const companionOutput = join(repacked, "native");
    await mkdir(companionOutput);
    for (const entry of await readdir(npmDir, { withFileTypes: true })) {
      if (!entry.isDirectory()) continue;
      const companion = join(npmDir, entry.name);
      const manifest = JSON.parse(await readFile(join(companion, "package.json"), "utf8"));
      const bundle = bundles.find(item => item.metadata.artifact.path.endsWith(`/${manifest.main}`));
      if (!bundle || digest(join(companion, manifest.main)) !== digest(join(artifacts, manifest.main))) fail("generated companion differs from its qualified artifact");
      const attestation = join(native, "attestations", bundle.metadata.selected_target);
      await mkdir(attestation, { recursive: true });
      for (const name of ["native-targets.json", "generation-manifest.json", "producer-receipt.json"]) {
        const original = name === "producer-receipt.json" ? bundle.receipt : join(bundle.path, name);
        await cp(original, join(companion, name));
        await cp(original, join(attestation, name));
        manifest.files.push(name);
      }
      await writeFile(join(companion, "package.json"), `${JSON.stringify(manifest, null, 2)}\n`);
      neutralManifest.optionalDependencies[manifest.name] = manifest.version;
      const asset = run("npm", ["pack", "--ignore-scripts", "--pack-destination", companionOutput, "--silent"], { cwd: companion }).trim();
      companions.push({ name: manifest.name, version: manifest.version, asset: `native/${asset}`, selected_target: bundle.metadata.selected_target, os: manifest.os, cpu: manifest.cpu, libc: manifest.libc, artifact: bundle.metadata.artifact, sha256: digest(join(companionOutput, asset)), metadata_sha256: digest(join(companion, "native-targets.json")), receipt_sha256: digest(join(companion, "producer-receipt.json")) });
    }
    if (companions.length !== targets.length) fail("generated companion inventory differs from Rust targets");
    // artifacts() also copies binaries to its package root, outside generated.
    for (const bundle of bundles) await rm(join(packageRoot, bundle.metadata.artifact.path.split("/").at(-1)));
    await rm(npmDir, { recursive: true, force: true });
    await writeFile(join(packageRoot, "package.json"), `${JSON.stringify(neutralManifest, null, 2)}\n`);
    const assembly = { schema: "acyclic.stream.native-package-assembly.v2", source_commit: sourceSha, source_sha256: bundles[0].metadata.source_sha256, targets, companions };
    await writeFile(join(native, "native-targets.json"), `${JSON.stringify(assembly, null, 2)}\n`);
    const packedName = run("npm", ["pack", "--ignore-scripts", "--pack-destination", repacked, "--silent"], { cwd: packageRoot }).trim();
    if (packedName !== `acyclic-labs-stream-${packageManifest.version}.tgz`) fail(`repacked Stream archive has unexpected name ${packedName}`);
    await assertSourceSnapshot(source);
    if (run("git", ["rev-parse", "HEAD"]).trim() !== sourceSha) fail("source revision changed during native assembly");
    copyFileSync(join(repacked, packedName), archive);
    await cp(companionOutput, join(output, "native"), { recursive: true });
    await writeFile(join(output, "STREAM_NATIVE_PACKAGE.json"), `${JSON.stringify({ ...assembly, parent: { name: packageManifest.name, version: packageManifest.version, asset: packedName, sha256: digest(archive) } }, null, 2)}\n`);
  } finally {
    await rm(temporary, { recursive: true, force: true });
  }

  const receipt = join(output, "QUALIFICATION.json");
  await rm(receipt, { force: true });
  run(process.execPath, ["scripts/typescript-qualification.mjs", "create", output, sourceSha], { stdio: "inherit" });
  const archives = [...(await readdir(output)).filter(name => name.endsWith(".tgz")), ...(await readdir(join(output, "native"))).filter(name => name.endsWith(".tgz")).map(name => `native/${name}`)].sort();
  const checksums = archives.map(name => `${digest(join(output, name))}  ${name}`).join("\n") + "\n";
  await writeFile(join(output, "SHA256SUMS"), checksums);
  await verifyNativeAssembly(output, sourceSha, packageManifest.version, expectedInventory);
  console.log(JSON.stringify({
    schema: "acyclic.stream.native-package-assembly.v2",
    source_commit: sourceSha,
    archive: `acyclic-labs-stream-${packageManifest.version}.tgz`,
    native_bundles: bundles.map(item => item.path),
  }));
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await main();
