import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { cp, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { gunzipSync } from "node:zlib";
import { isDeepStrictEqual } from "node:util";
import { tarEntries } from "./archive-utils.mjs";
import { createNativeProducer } from "./build-native-family.mjs";

/** Shared exact assembly specialized only by immutable Rust-generated family facts. */
export function createNativeAssembler(family) {
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const packagePath = resolve(root, family.packageDirectory);
const companionDirectory = family.companionDirectory;
const assemblyFile = `${family.key.toUpperCase()}_NATIVE_PACKAGE.json`;
const entryScript = resolve(root, `scripts/assemble-${family.key}-native-package.mjs`);
const parentAsset = version => `${family.npmPackage.replace(/^@/u, "").replace("/", "-")}-${version}.tgz`;
const { assertBundle, assertExactInventory, assertSelectedArtifact, assertSourceSnapshot, rustMetadata, sourceSnapshot, assertCurrentNativeFamily, assertBuildInputs, normalizeBuildInputs, napiGeneratorIdentity } = createNativeProducer(family);
const packageTsconfig = resolve(packagePath, "tsconfig.json");
const attestationNames = ["native-targets.json", "generation-manifest.json", "producer-receipt.json", ...(family.key === "filesystem" ? ["runtime-qualification.json"] : [])];

/** @param {string} message @returns {never} */
function fail(message) { throw new Error(message); }

/** @param {string} command @param {string[]} args @param {{cwd?: string, stdio?: import("node:child_process").StdioOptions}} [options] */
function run(command, args, options = {}) {
  const npmCli = join(dirname(process.execPath), "node_modules/npm/bin/npm-cli.js");
  const executable = process.platform === "win32" && command === "npm" ? process.execPath : command;
  const commandArgs = executable === process.execPath && command === "npm" ? [npmCli, ...args] : args;
  const result = execFileSync(executable, commandArgs, {
    cwd: options.cwd ?? root,
    encoding: "utf8",
    stdio: options.stdio ?? ["ignore", "pipe", "pipe"],
    windowsHide: true,
  });
  return result.trim();
}
/** Pack the exact maintained parent or companion tree without lifecycle scripts. */
async function packArchive(directory, output) {
  await mkdir(output, { recursive: true });
  return run("npm", ["pack", "--ignore-scripts", "--pack-destination", output, "--silent"], { cwd: directory });
}


/** @param {string} directory */
async function nativeInputs(directory, /** @type {{bundles: string[], receipts: string[]}} */ found = { bundles: [], receipts: [] }) {
  const entries = await readdir(directory, { withFileTypes: true });
  for (const entry of entries) {
    const path = join(directory, entry.name);
    if (entry.isSymbolicLink()) fail(`native input is a symbolic link: ${path}`);
    if (entry.isFile() && entry.name === "native-targets.json") found.bundles.push(dirname(path));
    if (entry.isFile() && ["producer-receipt.json", `${family.key}-native-build-inputs.receipt.json`].includes(entry.name)) found.receipts.push(path);
    if (entry.isDirectory()) await nativeInputs(path, found);
  }
  return found;
}

function digest(path) {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}


/** @param {Record<string, any>[]} metadata @param {string[]} targets @param {string} sourceSha */
function assertNativeSet(metadata, targets, sourceSha) {
  const selected = metadata.map(item => item.selected_target);
  if (selected.length !== targets.length || new Set(selected).size !== selected.length || targets.some(target => !selected.includes(target))) fail("native assembly requires exactly one bundle for every Rust target");
  const first = metadata[0];
  for (const item of metadata) {
    if (item.source_revision !== sourceSha) fail("native bundle source revision differs from release source");
    for (const field of ["source_sha256", "source_files", "package", "version", "targets"]) {
      if (JSON.stringify(item[field]) !== JSON.stringify(first[field])) fail(`native bundle ${field} differs across targets`);
    }
    if (JSON.stringify(item.targets) !== JSON.stringify(targets)) fail("native bundle target declaration differs from Rust");
  }
}

/** @param {string[]} bundles */
async function assertCommonBindings(bundles) {
  for (const name of ["binding.cjs", "binding.d.ts"]) {
    const first = await readFile(join(bundles[0], name));
    for (const bundle of bundles.slice(1)) {
      if (!first.equals(await readFile(join(bundle, name)))) fail(`native ${name} differs across targets`);
    }
  }
}

function qualifiedCompanionManifest(manifest) {
  return { ...manifest, files: [...manifest.files, ...attestationNames], private: false };
}

/** Preserve the existing Stream helper API; actual assembly uses the qualified projection below. */
async function writeCompanionManifest(directory, manifest) {
  manifest.private = false;
  await writeFile(join(directory, "package.json"), `${JSON.stringify(manifest, null, 2)}\n`);
}

function qualifiedParentManifest(manifest, targets, companions) {
  return { ...manifest, napi: { ...manifest.napi, targets }, optionalDependencies: Object.fromEntries(companions.map(companion => [companion.name, companion.version])) };
}

async function sourceNativeInventory(sourceSha, { refresh = false } = {}) {
  if (run("git", ["rev-parse", "HEAD"]) !== sourceSha) fail("native inventory source differs from checkout");
  if (refresh) assertCurrentNativeFamily();
  run("git", ["diff", "--quiet", "HEAD", "--", ...family.sourceRoots]);
  const source = await sourceSnapshot();
  const { rustPackage, targets } = rustMetadata();
  const manifest = JSON.parse(await readFile(join(packagePath, "package.json"), "utf8"));
  if (manifest.version !== rustPackage.version || manifest.private !== false) fail("native source package identity differs from Rust");
  const temporary = await mkdtemp(join(tmpdir(), "acyclic-actors-inventory-"));
  try {
    await writeFile(join(temporary, "package.json"), JSON.stringify({ ...manifest, napi: { ...manifest.napi, targets } }));
    const { NapiCli, parseTriple } = await import("@napi-rs/cli");
    const npmDir = join(temporary, "npm");
    await new NapiCli().createNpmDirs({ cwd: temporary, npmDir });
    const companions = await Promise.all(targets.map(async selected_target => {
      const generated = JSON.parse(await readFile(join(npmDir, parseTriple(selected_target).platformArchABI, "package.json"), "utf8"));
      return { selected_target, name: generated.name, version: generated.version, main: generated.main, os: generated.os, cpu: generated.cpu, libc: generated.libc, manifest: qualifiedCompanionManifest(generated) };
    }));
    await assertSourceSnapshot(source);
    if (run("git", ["rev-parse", "HEAD"]) !== sourceSha) fail("native inventory source changed during generation");
    return { generator: await napiGeneratorIdentity(), schema: `acyclic.${family.key}.native-source-inventory.v1`, source_commit: sourceSha, source_sha256: source.sha256, source_files: source.files, parent: qualifiedParentManifest(manifest, targets, companions), targets, companions };
  } finally { await rm(temporary, { recursive: true, force: true }); }
}

const objectDigest = value => `sha256:${createHash("sha256").update(JSON.stringify(value)).digest("hex")}`;

function assertWasmReceipt(receipt, files, sourceSha, expectedSource, version) {
  if (receipt.schema !== "acyclic.wasm-build-receipt.v1" || receipt.family !== family.key || receipt.package !== family.wasmPackageName || receipt.version !== version || receipt.source_commit !== sourceSha || receipt.source_sha256 !== expectedSource.source_sha256 || JSON.stringify(receipt.source_files) !== JSON.stringify(expectedSource.source_files)) fail("WASM receipt source or package identity differs");
  if (!receipt.build?.cargo?.version || !receipt.build?.rustc?.identity?.output || !/^sha256:[0-9a-f]{64}$/u.test(receipt.build.rustc.identity.executable_sha256 ?? "") || receipt.build.rustc.identity.command !== receipt.build.rustc.invocation?.rustc || receipt.build.rustc.invocation?.source !== "rustc-invocation" || receipt.build.rustc.invocation.target !== "wasm32-unknown-unknown" || !receipt.build?.wasm_bindgen?.version || !/^sha256:[0-9a-f]{64}$/u.test(receipt.build?.module_sha256 ?? "")) fail("WASM captured compiler recipe is missing");
  if (!Array.isArray(receipt.artifacts) || receipt.artifacts.length === 0) fail("WASM artifact inventory is missing");
  const expected = new Set(["package/generated/wasm/producer-receipt.json"]);
  for (const artifact of receipt.artifacts) {
    if (!/^generated\/wasm\/[^/\\]+$/u.test(artifact.path)) fail("WASM artifact path is invalid");
    const path = `package/${artifact.path}`;
    if (expected.has(path)) fail("WASM artifact inventory contains duplicates");
    expected.add(path);
    const bytes = files.get(path);
    if (!bytes || bytes.length !== artifact.bytes || `sha256:${createHash("sha256").update(bytes).digest("hex")}` !== artifact.sha256) fail("WASM artifact digest differs");
  }
  if (receipt.artifacts.filter(artifact => artifact.path.endsWith(".wasm")).length !== 1) fail("WASM inventory must contain one compiled module");
  const actual = [...files.keys()].filter(path => path.startsWith("package/generated/wasm/"));
  if (actual.length !== expected.size || actual.some(path => !expected.has(path))) fail("WASM file inventory differs");
}

function assertNativeReceipt(receipt, metadata) {
  if (receipt.schema !== `acyclic.${family.key}.native-build-inputs-receipt.v1` || receipt.published_build_inputs_sha256 !== objectDigest(metadata.build_inputs)) fail("native receipt does not attest the published build inputs");
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
// Bun 1.4.2 has no native Windows ARM64 runtime; x64 Bun there is installer tooling.
function nativeBunSupported(target) {
  return target !== "aarch64-pc-windows-msvc";
}

function assertNativeRuntimeQualification(proof, metadata, companion, receiptBytes) {
  const companionAsset = `${companion.name.replace(/^@/u, "").replace("/", "-")}-${metadata.version}.tgz`;
  const expectedArchives = [parentAsset(metadata.version), companionAsset].sort();
  const retainedPath = `acyclic-fs-${metadata.version}-${proof.platform}-${proof.arch}.node`;
  if (proof.schema !== "acyclic.filesystem.native-runtime-qualification.v1"
      || proof.source_commit !== metadata.source_revision || proof.source_sha256 !== metadata.source_sha256
      || proof.target !== metadata.selected_target || !companion.os.includes(proof.platform)
      || !companion.cpu.includes(proof.arch) || proof.runtime !== "node" || !/^v24\./u.test(proof.node ?? "")
      || proof.bun?.version !== "1.4.2" || proof.bun?.platform !== proof.platform
      || (nativeBunSupported(metadata.selected_target)
        ? proof.bun.arch !== proof.arch || proof.bun.consumer !== "passed"
        : proof.bun.arch !== "x64" || proof.bun.consumer !== "unsupported-native-architecture")
      || metadata.artifact.path !== `generated/native/${companion.main}`
      || !isDeepStrictEqual(proof.artifact, metadata.artifact)
      || proof.producer_receipt_sha256 !== `sha256:${createHash("sha256").update(receiptBytes).digest("hex")}`
      || !isDeepStrictEqual(proof.retained_artifact, { path: retainedPath, sha256: metadata.artifact.sha256, bytes: metadata.artifact.bytes })
      || !Array.isArray(proof.archives) || !isDeepStrictEqual(proof.archives.map(entry => entry.path).sort(), expectedArchives)
      || proof.archives.some(entry => !/^[^/\\]+\.tgz$/u.test(entry.path ?? "") || !/^sha256:[0-9a-f]{64}$/u.test(entry.sha256 ?? ""))) {
    fail("filesystem native runtime qualification source, artifact, compiler receipt or architecture differs");
  }
}


function archiveFiles(path) {
  const files = new Map();
  for (const entry of tarEntries(gunzipSync(readFileSync(path)))) {
    if (!entry.path.startsWith("package/") || entry.path.split("/").some(part => part === ".." || part === ".") || !["0", "\0", "5"].includes(entry.type) || files.has(entry.path)) fail("native package archive has an unsafe or duplicate entry");
    if (entry.type !== "5") files.set(entry.path, entry.body);
  }
  return files;
}

async function verifyNativeAssembly(output, sourceSha, version, expectedInventory) {
  if (!expectedInventory || expectedInventory.schema !== `acyclic.${family.key}.native-source-inventory.v1` || expectedInventory.source_commit !== sourceSha || expectedInventory.parent.version !== version || expectedInventory.parent.private !== false) fail("trusted native source inventory is required for release");
  const assembly = JSON.parse(await readFile(join(output, assemblyFile), "utf8"));
  if (assembly.schema !== `acyclic.${family.key}.native-package-assembly.v2` || assembly.source_commit !== sourceSha || assembly.parent.version !== version || assembly.parent.name !== expectedInventory.parent.name || assembly.parent.asset !== parentAsset(version) || assembly.source_sha256 !== expectedInventory.source_sha256 || JSON.stringify(assembly.targets) !== JSON.stringify(expectedInventory.targets)) fail("native assembly release identity or source differs");
  if (digest(join(output, assembly.parent.asset)) !== assembly.parent.sha256) fail("native assembly parent digest differs");
  const parent = archiveFiles(join(output, assembly.parent.asset));
  const wasmBytes = parent.get("package/generated/wasm/producer-receipt.json");
  if (!wasmBytes || `sha256:${createHash("sha256").update(wasmBytes).digest("hex")}` !== assembly.wasm_receipt_sha256) fail("WASM producer receipt digest differs");
  assertWasmReceipt(JSON.parse(wasmBytes.toString("utf8")), parent, sourceSha, expectedInventory, version);
  const neutral = JSON.parse(parent.get("package/package.json").toString("utf8"));
  if (!isDeepStrictEqual(neutral, expectedInventory.parent) || [...parent.keys()].some(path => path.endsWith(".node"))) fail("neutral parent manifest or binary inventory differs");
  const { parent: ignored, ...index } = assembly;
  if (JSON.stringify(JSON.parse(parent.get("package/generated/native/native-targets.json").toString("utf8"))) !== JSON.stringify(index)) fail("neutral parent assembly index differs");
  const nativeFiles = new Set(["binding.cjs", "binding.d.ts", "native-targets.json"].map(name => `package/generated/native/${name}`));
  for (const target of expectedInventory.targets) for (const name of attestationNames) nativeFiles.add(`package/generated/native/attestations/${target}/${name}`);
  const retainedFiles = [...parent.keys()].filter(path => path.startsWith("package/generated/native/"));
  if (retainedFiles.length !== nativeFiles.size || retainedFiles.some(path => !nativeFiles.has(path))) fail("neutral parent native file inventory differs");
  const metadata = [];
  const dependencies = {};
  for (const entry of assembly.companions) {
    if (!entry.asset.startsWith(`${companionDirectory}/`) || !/^[^/\\]+\.tgz$/u.test(entry.asset.slice(companionDirectory.length + 1)) || entry.version !== version || digest(join(output, entry.asset)) !== entry.sha256) fail("native companion archive identity or digest differs");
    const files = archiveFiles(join(output, entry.asset));
    const manifest = JSON.parse(files.get("package/package.json").toString("utf8"));
    const expected = expectedInventory.companions.find(item => item.selected_target === entry.selected_target);
    if (!expected || !isDeepStrictEqual(manifest, expected.manifest) || manifest.name !== entry.name || manifest.version !== version || manifest.private !== false || ["os", "cpu", "libc"].some(field => JSON.stringify(manifest[field]) !== JSON.stringify(entry[field]))) fail("native companion differs from maintained source target mapping");
    const originals = {};
    for (const name of attestationNames) {
      const bytes = files.get(`package/${name}`);
      const retained = parent.get(`package/generated/native/attestations/${entry.selected_target}/${name}`);
      if (!bytes || !retained || !bytes.equals(retained)) fail("original native attestation differs between parent and companion");
      originals[name] = JSON.parse(bytes.toString("utf8"));
    }
    const meta = originals["native-targets.json"], generation = originals["generation-manifest.json"];
    if (meta.schema !== `acyclic.${family.key}.native-targets.v1` || generation.schema !== `acyclic.${family.key}.native-generation.v1` || meta.package !== family.rustPackageName || generation.package !== meta.package || meta.version !== version || generation.version !== version || meta.selected_target !== entry.selected_target || generation.selected_target !== entry.selected_target || meta.source_revision !== sourceSha || generation.revision !== sourceSha || meta.source_sha256 !== assembly.source_sha256 || generation.source_sha256 !== assembly.source_sha256 || JSON.stringify(meta.targets) !== JSON.stringify(assembly.targets) || JSON.stringify(generation.targets) !== JSON.stringify(assembly.targets) || JSON.stringify(meta.source_files) !== JSON.stringify(expectedInventory.source_files) || JSON.stringify(generation.source_files) !== JSON.stringify(meta.source_files) || JSON.stringify(meta.artifact) !== JSON.stringify(entry.artifact)) fail("native companion original source or target differs");
    if (JSON.stringify(meta.build_inputs) !== JSON.stringify(generation.build_inputs)) fail("original native build recipes differ");
    if (Object.keys(expectedInventory.generator).some(field => meta.build_inputs.generator[field] !== expectedInventory.generator[field])) fail("native generator differs from trusted source inventory");
    assertNativeReceipt(originals["producer-receipt.json"], meta);
    if (family.key === "filesystem") assertNativeRuntimeQualification(originals["runtime-qualification.json"], meta, expected, files.get("package/producer-receipt.json"));
    const bytesHash = bytes => createHash("sha256").update(bytes).digest("hex");
    if (`sha256:${bytesHash(files.get("package/generation-manifest.json"))}` !== meta.generation_sha256 || meta.generation_sha256 !== entry.generation_sha256 || JSON.stringify(meta.artifacts) !== JSON.stringify(generation.artifacts)) fail("original native generation attestation differs");
    assertSelectedArtifact(meta.artifact, generation.artifacts);
    if (JSON.stringify(meta.artifacts.map(artifact => artifact.path).sort()) !== JSON.stringify(["generated/native/binding.cjs", "generated/native/binding.d.ts", meta.artifact.path].sort()) || manifest.main !== meta.artifact.path.slice("generated/native/".length)) fail("native companion artifact inventory differs");
    for (const artifact of meta.artifacts) {
      const name = artifact.path.slice("generated/native/".length);
      const bytes = name.endsWith(".node") ? files.get(`package/${name}`) : parent.get(`package/generated/native/${name}`);
      if (!bytes || bytes.length !== artifact.bytes || `sha256:${bytesHash(bytes)}` !== artifact.sha256) fail("original native artifact digest differs");
    }
    const allowed = new Set(["package/package.json", "package/README.md", `package/${manifest.main}`, ...attestationNames.map(name => `package/${name}`)]);
    if ([...files.keys()].some(path => !allowed.has(path))) fail("native companion contains unstated files");
    dependencies[entry.name] = version;
    metadata.push(meta);
  }
  assertNativeSet(metadata, expectedInventory.targets, sourceSha);
  if (JSON.stringify(neutral.optionalDependencies) !== JSON.stringify(dependencies)) fail("neutral parent optional dependencies differ");
  const observed = (await readdir(join(output, companionDirectory))).sort();
  if (JSON.stringify(observed) !== JSON.stringify(assembly.companions.map(entry => entry.asset.slice(`${companionDirectory}/`.length)).sort())) fail("native publication inventory contains missing or extra files");
  const checksums = new Map((await readFile(join(output, "SHA256SUMS"), "utf8")).trim().split("\n").map(line => { const [sha256, asset] = line.trim().split(/  /u); return [asset, sha256]; }));
  for (const entry of [assembly.parent, ...assembly.companions]) if (checksums.get(entry.asset) !== entry.sha256) fail("native assembly checksum inventory differs");
  return assembly;
}

/** @param {string[]} argv */
function parseArgs(argv) {
  /** @type {{bundles: string[], receipts: string[], output?: string, sourceSha?: string, help?: boolean}} */
  const options = { bundles: [], receipts: [] };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--bundle") options.bundles.push(argv[++index]);
    else if (arg === "--receipt") options.receipts.push(argv[++index]);
    else if (arg === "--output") options.output = argv[++index];
    else if (arg === "--source-sha") options.sourceSha = argv[++index];
    else if (arg === "--help" || arg === "-h") options.help = true;
    else fail(`unknown option ${arg}`);
  }
  return options;
}

async function main() {
  if (process.argv[2] === "inventory") {
    const [, output, sourceSha] = process.argv.slice(2);
    if (!output || !/^[0-9a-f]{40,64}$/u.test(sourceSha ?? "")) fail(`usage: assemble-${family.key}-native-package.mjs inventory OUTPUT SOURCE_SHA`);
    await writeFile(resolve(output), `${JSON.stringify(await sourceNativeInventory(sourceSha, { refresh: true }), null, 2)}\n`);
    return;
  }
  if (process.argv[2] === "release") {
    const [, outputArgument, inputsArgument, sourceSha, qualifiedArgument] = process.argv.slice(2);
    if (!outputArgument || !inputsArgument || !qualifiedArgument || !/^[0-9a-f]{40,64}$/u.test(sourceSha ?? "")) fail(`usage: assemble-${family.key}-native-package.mjs release OUTPUT INPUTS SOURCE_SHA ORIGINAL_QUALIFIED_DIRECTORY`);
    const output = resolve(outputArgument), inputs = resolve(inputsArgument);
    const compilerReceipt = join(output, "BUILD.json");
    await readFile(compilerReceipt);
    const found = await nativeInputs(inputs);
    const bundles = found.bundles;
    const temporary = await mkdtemp(join(tmpdir(), "acyclic-actors-release-"));
    try {
      const assembled = join(temporary, "assembled");
      run(process.execPath, [entryScript, ...bundles.flatMap(bundle => ["--bundle", bundle]), ...found.receipts.flatMap(receipt => ["--receipt", receipt]), "--output", assembled, "--source-sha", sourceSha]);
      const receipt = JSON.parse(await readFile(join(assembled, assemblyFile), "utf8"));
      await mkdir(join(output, companionDirectory), { recursive: true });
      await assertExactInventory(join(output, companionDirectory), new Set());
      for (const entry of [receipt.parent, ...receipt.companions]) await cp(join(assembled, entry.asset), join(output, entry.asset));
      await cp(join(assembled, assemblyFile), join(output, assemblyFile));
      await rm(join(output, "QUALIFICATION.json"), { force: true });
      run(process.execPath, ["scripts/typescript-qualification.mjs", "reassemble", output, sourceSha, compilerReceipt, resolve(qualifiedArgument)]);
      const archives = (await readdir(output)).filter(name => name.endsWith(".tgz"));
      for (const directory of await readdir(output, { withFileTypes: true })) {
        if (!directory.isDirectory()) continue;
        for (const name of await readdir(join(output, directory.name))) if (name.endsWith(".tgz")) archives.push(`${directory.name}/${name}`);
      }
      await writeFile(join(output, "SHA256SUMS"), archives.sort().map(asset => `${digest(join(output, asset))}  ${asset}`).join("\n") + "\n");
      await verifyNativeAssembly(output, sourceSha, receipt.parent.version, await sourceNativeInventory(sourceSha));
    } finally { await rm(temporary, { recursive: true, force: true }); }
    return;
  }
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    console.log(`usage: assemble-${family.key}-native-package.mjs --bundle BUNDLE [--bundle BUNDLE ...] --output OUTPUT --source-sha SIGNED_HEAD`);
    return;
  }
  if (options.bundles.length === 0 || options.bundles.some(bundle => !bundle) || !options.output || !/^[0-9a-f]{40,64}$/.test(options.sourceSha ?? "")) {
    fail(`usage: assemble-${family.key}-native-package.mjs --bundle BUNDLE [--bundle BUNDLE ...] --output OUTPUT --source-sha SIGNED_HEAD`);
  }
  const output = resolve(options.output);
  await mkdir(output, { recursive: true });
  await assertExactInventory(output, new Set());
  const sourceSha = run("git", ["rev-parse", "HEAD"]);
  if (sourceSha !== options.sourceSha) fail(`source ${options.sourceSha} differs from checkout ${sourceSha}`);
  assertCurrentNativeFamily();
  run("git", ["diff", "--quiet", "HEAD", "--", ...family.sourceRoots]);
  const source = await sourceSnapshot();
  const { rustPackage, targets } = rustMetadata();
  const manifest = JSON.parse(await readFile(join(packagePath, "package.json"), "utf8"));
  if (manifest.version !== rustPackage.version || manifest.private !== false) fail("parent package identity differs from Rust");
  const bundles = await Promise.all(options.bundles.map(async path => ({ path: resolve(path), receipt: "", ...await assertBundle(resolve(path)) })));
  const receipts = await Promise.all(options.receipts.map(async path => ({ path: resolve(path), value: JSON.parse(await readFile(resolve(path), "utf8")) })));
  if (receipts.length !== bundles.length) fail("assembly requires exactly one original compiler receipt per bundle");
  for (const bundle of bundles) {
    const matching = receipts.filter(item => item.value.published_build_inputs_sha256 === objectDigest(bundle.metadata.build_inputs));
    if (matching.length !== 1) fail("native bundle requires one matching original compiler receipt");
    assertNativeReceipt(matching[0].value, bundle.metadata);
    bundle.receipt = matching[0].path;
  }
  assertNativeSet(bundles.map(bundle => bundle.metadata), targets, sourceSha);
  await assertCommonBindings(bundles.map(bundle => bundle.path));
  const temporary = await mkdtemp(join(tmpdir(), "acyclic-actors-package-"));
  try {
    // A clean source checkout may not contain the ignored TypeScript output.
    // Compile it before packing so the neutral parent archive is reproducible
    // from source rather than depending on a stale or empty dist directory.
    const compiled = join(temporary, "dist");
    run("bun", ["x", "tsc", "-p", packageTsconfig, "--outDir", compiled, "--pretty", "false"]);
    const compiledEntrypoint = join(compiled, family.npmEntrypoint ?? "index.js");
    try { await readFile(compiledEntrypoint); }
    catch { fail(`TypeScript compilation did not produce ${compiledEntrypoint}`); }
    const parentRoot = join(temporary, "parent");
    const napiConfigPath = join(temporary, "napi-package.json");
    // Keep the maintained loader and target manifest in the neutral parent;
    // only the platform binary belongs in the optional companion package.
    // Removing the whole directory makes every installed Node consumer use
    // WASM and prevents a corrupt companion from failing closed.
    await cp(packagePath, parentRoot, {
      recursive: true,
      filter: (source) => source !== join(packagePath, "generated/native") && source !== join(packagePath, "generated/wasm") && source !== join(packagePath, "dist") && source !== join(packagePath, "node_modules") && !source.endsWith(".node") && !source.endsWith(".tgz"),
    });
    await cp(compiled, join(parentRoot, "dist"), { recursive: true });
    const native = join(parentRoot, "generated/native");
    await mkdir(native, { recursive: true });
    for (const name of ["binding.cjs", "binding.d.ts"]) await cp(join(bundles[0].path, name), join(native, name));
    const artifacts = join(temporary, "artifacts");
    await mkdir(artifacts);
    for (const bundle of bundles) {
      const name = bundle.metadata.artifact.path.slice("generated/native/".length);
      await cp(join(bundle.path, name), join(artifacts, name));
    }
    run(process.execPath, [
      join(root, "scripts/build-wasm.mjs"), family.key,
      join(parentRoot, "generated/wasm"),
    ]);
    const wasmReceiptPath = join(parentRoot, "generated/wasm/producer-receipt.json");
    const wasmReceipt = JSON.parse(await readFile(wasmReceiptPath, "utf8"));
    const wasmFiles = new Map();
    for (const name of await readdir(join(parentRoot, "generated/wasm"))) wasmFiles.set(`package/generated/wasm/${name}`, await readFile(join(parentRoot, "generated/wasm", name)));
    assertWasmReceipt(wasmReceipt, wasmFiles, sourceSha, { source_sha256: source.sha256, source_files: source.files }, manifest.version);
    await mkdir(output, { recursive: true });
    const companionOutput = join(output, companionDirectory);
    await mkdir(companionOutput);

    // Let the maintained NAPI-RS package APIs own target naming,
    // package metadata, cpu/os/libc selectors, and the binary file list. The
    // Rust bundle remains the attested input; the temporary config supplies
    // the Rust-qualified target set without a second checked-in target list.
    const napiConfig = { ...manifest, napi: { ...manifest.napi, targets } };
    await writeFile(napiConfigPath, `${JSON.stringify(napiConfig, null, 2)}\n`);
    const { NapiCli } = await import("@napi-rs/cli");
    const napi = new NapiCli();
    const npmDir = join(temporary, "npm");
    await napi.createNpmDirs({ cwd: temporary, packageJsonPath: napiConfigPath, npmDir });
    await napi.artifacts({ cwd: temporary, packageJsonPath: napiConfigPath, npmDir, outputDir: artifacts });
    const companionEntries = (await readdir(npmDir, { withFileTypes: true })).filter(entry => entry.isDirectory()).sort((a, b) => a.name.localeCompare(b.name));
    if (companionEntries.length !== targets.length) fail("NAPI-RS companion inventory differs from Rust targets");
    const companions = [];
    for (const entry of companionEntries) {
      const companionRoot = join(npmDir, entry.name);
      const companionManifest = JSON.parse(await readFile(join(companionRoot, "package.json"), "utf8"));
      const bundle = bundles.find(bundle => bundle.metadata.artifact.path === `generated/native/${companionManifest.main}`);
      if (!bundle || companionManifest.version !== manifest.version || digest(join(companionRoot, companionManifest.main)) !== bundle.metadata.artifact.sha256.slice("sha256:".length)) fail("generated companion differs from its qualified artifact");
      const attestation = join(native, "attestations", bundle.metadata.selected_target);
      await mkdir(attestation, { recursive: true });
      for (const name of attestationNames) {
        const original = name === "producer-receipt.json" ? bundle.receipt : name === "runtime-qualification.json" ? join(bundle.path, "..", "qualification", name) : join(bundle.path, name);
        if (name === "runtime-qualification.json") {
          assertNativeRuntimeQualification(JSON.parse(await readFile(original, "utf8")), bundle.metadata, companionManifest, await readFile(bundle.receipt));
          const proof = JSON.parse(await readFile(original, "utf8"));
          for (const archive of proof.archives) {
            const archivePath = join(bundle.path, "..", "qualification", archive.path);
            if (`sha256:${digest(archivePath)}` !== archive.sha256) fail("filesystem tested archive digest differs");
          }
          const retainedPath = join(bundle.path, "..", "qualification", proof.retained_artifact.path);
          if (`sha256:${digest(retainedPath)}` !== proof.retained_artifact.sha256) fail("filesystem tested retained ABI asset digest differs");
        }
        await cp(original, join(attestation, name));
        await cp(original, join(companionRoot, name));
      }
      await writeFile(join(companionRoot, "package.json"), `${JSON.stringify(qualifiedCompanionManifest(companionManifest), null, 2)}\n`);
      const archive = `${companionDirectory}/${await packArchive(companionRoot, companionOutput)}`;
      companions.push({ name: companionManifest.name, version: companionManifest.version, asset: archive, selected_target: bundle.metadata.selected_target, os: companionManifest.os, cpu: companionManifest.cpu, libc: companionManifest.libc, artifact: bundle.metadata.artifact, generation_sha256: bundle.metadata.generation_sha256, sha256: digest(join(output, archive)) });
    }
    const assembly = { schema: `acyclic.${family.key}.native-package-assembly.v2`, source_commit: sourceSha, source_sha256: bundles[0].metadata.source_sha256, targets, companions, wasm_receipt_sha256: `sha256:${digest(wasmReceiptPath)}` };
    await writeFile(join(native, "native-targets.json"), `${JSON.stringify(assembly, null, 2)}\n`);
    const parentManifest = qualifiedParentManifest(manifest, targets, companions);
    await writeFile(join(parentRoot, "package.json"), `${JSON.stringify(parentManifest, null, 2)}\n`);
    const parentArchive = await packArchive(parentRoot, output);
    await assertSourceSnapshot(source);
    if (run("git", ["rev-parse", "HEAD"]) !== sourceSha) fail("source revision changed during package assembly");
    const receipt = { ...assembly, parent: { name: manifest.name, version: manifest.version, asset: parentArchive, sha256: digest(join(output, parentArchive)) } };
    await writeFile(join(output, assemblyFile), `${JSON.stringify(receipt, null, 2)}\n`);
    await writeFile(join(output, "SHA256SUMS"), [receipt.parent, ...companions].map(entry => `${entry.sha256}  ${entry.asset}`).sort().join("\n") + "\n");
    await assertExactInventory(output, new Set([parentArchive, companionDirectory, assemblyFile, "SHA256SUMS"]), new Set([companionDirectory]));
    await assertExactInventory(companionOutput, new Set(companions.map(companion => companion.asset.slice(`${companionDirectory}/`.length))));
    console.log(JSON.stringify(receipt));
  } finally {
    await rm(temporary, { recursive: true, force: true });
  }
}


return { assertNativeSet, assertCommonBindings, qualifiedCompanionManifest, qualifiedParentManifest, sourceNativeInventory, verifyNativeAssembly, assertNativeReceipt, assertNativeRuntimeQualification, nativeBunSupported, writeCompanionManifest, packArchive, main };
}
