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
import { assertBundle, assertExactInventory, assertSelectedArtifact, assertSourceSnapshot, rustMetadata, sourceSnapshot } from "./build-actors-native.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const packagePath = resolve(root, "typescript/packages/actors");
const packageTsconfig = resolve(packagePath, "tsconfig.json");

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

/** @param {string} path */
function digest(path) {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}


/** @param {Record<string, any>[]} metadata @param {string[]} targets @param {string} sourceSha */
export function assertNativeSet(metadata, targets, sourceSha) {
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
export async function assertCommonBindings(bundles) {
  for (const name of ["binding.cjs", "binding.d.ts"]) {
    const first = await readFile(join(bundles[0], name));
    for (const bundle of bundles.slice(1)) {
      if (!first.equals(await readFile(join(bundle, name)))) fail(`native ${name} differs across targets`);
    }
  }
}

export function qualifiedCompanionManifest(manifest) {
  return { ...manifest, files: [...manifest.files, "native-targets.json", "generation-manifest.json"], private: false };
}

export async function sourceNativeInventory(sourceSha) {
  if (run("git", ["rev-parse", "HEAD"]) !== sourceSha) fail("native inventory source differs from checkout");
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
      return { selected_target, name: generated.name, main: generated.main, os: generated.os, cpu: generated.cpu, libc: generated.libc, manifest: qualifiedCompanionManifest(generated) };
    }));
    await assertSourceSnapshot(source);
    if (run("git", ["rev-parse", "HEAD"]) !== sourceSha) fail("native inventory source changed during generation");
    return { schema: "acyclic.actors.native-source-inventory.v1", source_commit: sourceSha, source_sha256: source.sha256, source_files: source.files, parent: { name: manifest.name, version: manifest.version, private: manifest.private }, targets, companions };
  } finally { await rm(temporary, { recursive: true, force: true }); }
}

function archiveFiles(path) {
  const files = new Map();
  for (const entry of tarEntries(gunzipSync(readFileSync(path)))) {
    if (!entry.path.startsWith("package/") || entry.path.split("/").some(part => part === ".." || part === ".") || !["0", "\0", "5"].includes(entry.type) || files.has(entry.path)) fail("native package archive has an unsafe or duplicate entry");
    if (entry.type !== "5") files.set(entry.path, entry.body);
  }
  return files;
}

export async function verifyNativeAssembly(output, sourceSha, version, expectedInventory) {
  if (!expectedInventory || expectedInventory.schema !== "acyclic.actors.native-source-inventory.v1" || expectedInventory.source_commit !== sourceSha || expectedInventory.parent.version !== version || expectedInventory.parent.private !== false) fail("trusted native source inventory is required for release");
  const assembly = JSON.parse(await readFile(join(output, "ACTORS_NATIVE_PACKAGE.json"), "utf8"));
  if (assembly.schema !== "acyclic.actors.native-package-assembly.v2" || assembly.source_commit !== sourceSha || assembly.parent.version !== version || assembly.parent.name !== expectedInventory.parent.name || assembly.parent.asset !== `acyclic-labs-actors-${version}.tgz` || assembly.source_sha256 !== expectedInventory.source_sha256 || JSON.stringify(assembly.targets) !== JSON.stringify(expectedInventory.targets)) fail("native assembly release identity or source differs");
  if (digest(join(output, assembly.parent.asset)) !== assembly.parent.sha256) fail("native assembly parent digest differs");
  const parent = archiveFiles(join(output, assembly.parent.asset));
  const neutral = JSON.parse(parent.get("package/package.json").toString("utf8"));
  if (neutral.name !== assembly.parent.name || neutral.version !== version || neutral.private !== false || [...parent.keys()].some(path => path.endsWith(".node"))) fail("neutral parent manifest or binary inventory differs");
  const { parent: ignored, ...index } = assembly;
  if (JSON.stringify(JSON.parse(parent.get("package/generated/native/native-targets.json").toString("utf8"))) !== JSON.stringify(index)) fail("neutral parent assembly index differs");
  const nativeFiles = new Set(["binding.cjs", "binding.d.ts", "native-targets.json"].map(name => `package/generated/native/${name}`));
  for (const target of expectedInventory.targets) for (const name of ["native-targets.json", "generation-manifest.json"]) nativeFiles.add(`package/generated/native/attestations/${target}/${name}`);
  const retainedFiles = [...parent.keys()].filter(path => path.startsWith("package/generated/native/"));
  if (retainedFiles.length !== nativeFiles.size || retainedFiles.some(path => !nativeFiles.has(path))) fail("neutral parent native file inventory differs");
  const metadata = [];
  const dependencies = {};
  for (const entry of assembly.companions) {
    if (!/^actors-native\/[^/\\]+\.tgz$/u.test(entry.asset) || entry.version !== version || digest(join(output, entry.asset)) !== entry.sha256) fail("native companion archive identity or digest differs");
    const files = archiveFiles(join(output, entry.asset));
    const manifest = JSON.parse(files.get("package/package.json").toString("utf8"));
    const expected = expectedInventory.companions.find(item => item.selected_target === entry.selected_target);
    if (!expected || !isDeepStrictEqual(manifest, expected.manifest) || manifest.name !== entry.name || manifest.version !== version || manifest.private !== false || ["os", "cpu", "libc"].some(field => JSON.stringify(manifest[field]) !== JSON.stringify(entry[field]))) fail("native companion differs from maintained source target mapping");
    const originals = {};
    for (const name of ["native-targets.json", "generation-manifest.json"]) {
      const bytes = files.get(`package/${name}`);
      const retained = parent.get(`package/generated/native/attestations/${entry.selected_target}/${name}`);
      if (!bytes || !retained || !bytes.equals(retained)) fail("original native attestation differs between parent and companion");
      originals[name] = JSON.parse(bytes.toString("utf8"));
    }
    const meta = originals["native-targets.json"], generation = originals["generation-manifest.json"];
    if (meta.schema !== "acyclic.actors.native-targets.v1" || generation.schema !== "acyclic.actors.native-generation.v1" || meta.package !== "acyclic-actors-napi" || generation.package !== meta.package || meta.version !== version || generation.version !== version || meta.selected_target !== entry.selected_target || generation.selected_target !== entry.selected_target || meta.source_revision !== sourceSha || generation.revision !== sourceSha || meta.source_sha256 !== assembly.source_sha256 || generation.source_sha256 !== assembly.source_sha256 || JSON.stringify(meta.targets) !== JSON.stringify(assembly.targets) || JSON.stringify(generation.targets) !== JSON.stringify(assembly.targets) || JSON.stringify(meta.source_files) !== JSON.stringify(expectedInventory.source_files) || JSON.stringify(generation.source_files) !== JSON.stringify(meta.source_files) || JSON.stringify(meta.artifact) !== JSON.stringify(entry.artifact)) fail("native companion original source or target differs");
    const bytesHash = bytes => createHash("sha256").update(bytes).digest("hex");
    if (`sha256:${bytesHash(files.get("package/generation-manifest.json"))}` !== meta.generation_sha256 || meta.generation_sha256 !== entry.generation_sha256 || JSON.stringify(meta.artifacts) !== JSON.stringify(generation.artifacts)) fail("original native generation attestation differs");
    assertSelectedArtifact(meta.artifact, generation.artifacts);
    if (JSON.stringify(meta.artifacts.map(artifact => artifact.path).sort()) !== JSON.stringify(["generated/native/binding.cjs", "generated/native/binding.d.ts", meta.artifact.path].sort()) || manifest.main !== meta.artifact.path.slice("generated/native/".length)) fail("native companion artifact inventory differs");
    for (const artifact of meta.artifacts) {
      const name = artifact.path.slice("generated/native/".length);
      const bytes = name.endsWith(".node") ? files.get(`package/${name}`) : parent.get(`package/generated/native/${name}`);
      if (!bytes || bytes.length !== artifact.bytes || `sha256:${bytesHash(bytes)}` !== artifact.sha256) fail("original native artifact digest differs");
    }
    const allowed = new Set(["package/package.json", "package/README.md", `package/${manifest.main}`, "package/native-targets.json", "package/generation-manifest.json"]);
    if ([...files.keys()].some(path => !allowed.has(path))) fail("native companion contains unstated files");
    dependencies[entry.name] = version;
    metadata.push(meta);
  }
  assertNativeSet(metadata, expectedInventory.targets, sourceSha);
  if (JSON.stringify(neutral.optionalDependencies) !== JSON.stringify(dependencies)) fail("neutral parent optional dependencies differ");
  const observed = (await readdir(join(output, "actors-native"))).sort();
  if (JSON.stringify(observed) !== JSON.stringify(assembly.companions.map(entry => entry.asset.slice("actors-native/".length)).sort())) fail("native publication inventory contains missing or extra files");
  const checksums = new Map((await readFile(join(output, "SHA256SUMS"), "utf8")).trim().split("\n").map(line => { const [sha256, asset] = line.trim().split(/  /u); return [asset, sha256]; }));
  for (const entry of [assembly.parent, ...assembly.companions]) if (checksums.get(entry.asset) !== entry.sha256) fail("native assembly checksum inventory differs");
  return assembly;
}

/** @param {string[]} argv */
function parseArgs(argv) {
  /** @type {{bundles: string[], output?: string, sourceSha?: string, help?: boolean}} */
  const options = { bundles: [] };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--bundle") options.bundles.push(argv[++index]);
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
    if (!output || !/^[0-9a-f]{40,64}$/u.test(sourceSha ?? "")) fail("usage: assemble-actors-native-package.mjs inventory OUTPUT SOURCE_SHA");
    await writeFile(resolve(output), `${JSON.stringify(await sourceNativeInventory(sourceSha), null, 2)}\n`);
    return;
  }
  if (process.argv[2] === "release") {
    const [, outputArgument, inputsArgument, sourceSha] = process.argv.slice(2);
    if (!outputArgument || !inputsArgument || !/^[0-9a-f]{40,64}$/u.test(sourceSha ?? "")) fail("usage: assemble-actors-native-package.mjs release OUTPUT INPUTS SOURCE_SHA");
    const output = resolve(outputArgument), inputs = resolve(inputsArgument);
    const bundles = [];
    for (const entry of await readdir(inputs, { withFileTypes: true })) {
      if (!entry.isDirectory()) fail("native release inputs must contain only platform directories");
      bundles.push(join(inputs, entry.name, "bundle"));
    }
    const temporary = await mkdtemp(join(tmpdir(), "acyclic-actors-release-"));
    try {
      const assembled = join(temporary, "assembled");
      run(process.execPath, [fileURLToPath(import.meta.url), ...bundles.flatMap(bundle => ["--bundle", bundle]), "--output", assembled, "--source-sha", sourceSha]);
      const receipt = JSON.parse(await readFile(join(assembled, "ACTORS_NATIVE_PACKAGE.json"), "utf8"));
      await mkdir(join(output, "actors-native"), { recursive: true });
      await assertExactInventory(join(output, "actors-native"), new Set());
      for (const entry of [receipt.parent, ...receipt.companions]) await cp(join(assembled, entry.asset), join(output, entry.asset));
      await cp(join(assembled, "ACTORS_NATIVE_PACKAGE.json"), join(output, "ACTORS_NATIVE_PACKAGE.json"));
      await rm(join(output, "QUALIFICATION.json"), { force: true });
      run(process.execPath, ["scripts/typescript-qualification.mjs", "create", output, sourceSha]);
      const archives = (await readdir(output)).filter(name => name.endsWith(".tgz"));
      for (const directory of ["native", "actors-native"]) {
        for (const name of await readdir(join(output, directory))) if (name.endsWith(".tgz")) archives.push(`${directory}/${name}`);
      }
      await writeFile(join(output, "SHA256SUMS"), archives.sort().map(asset => `${digest(join(output, asset))}  ${asset}`).join("\n") + "\n");
      await verifyNativeAssembly(output, sourceSha, receipt.parent.version, await sourceNativeInventory(sourceSha));
    } finally { await rm(temporary, { recursive: true, force: true }); }
    return;
  }
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    console.log("usage: assemble-actors-native-package.mjs --bundle BUNDLE [--bundle BUNDLE ...] --output OUTPUT --source-sha SIGNED_HEAD");
    return;
  }
  if (options.bundles.length === 0 || options.bundles.some(bundle => !bundle) || !options.output || !/^[0-9a-f]{40,64}$/.test(options.sourceSha ?? "")) {
    fail("usage: assemble-actors-native-package.mjs --bundle BUNDLE [--bundle BUNDLE ...] --output OUTPUT --source-sha SIGNED_HEAD");
  }
  const output = resolve(options.output);
  await mkdir(output, { recursive: true });
  await assertExactInventory(output, new Set());
  const sourceSha = run("git", ["rev-parse", "HEAD"]);
  if (sourceSha !== options.sourceSha) fail(`source ${options.sourceSha} differs from checkout ${sourceSha}`);
  const source = await sourceSnapshot();
  const { rustPackage, targets } = rustMetadata();
  const manifest = JSON.parse(await readFile(join(packagePath, "package.json"), "utf8"));
  if (manifest.version !== rustPackage.version || manifest.private !== false) fail("parent package identity differs from Rust");
  const bundles = await Promise.all(options.bundles.map(async path => ({ path: resolve(path), ...await assertBundle(resolve(path)) })));
  assertNativeSet(bundles.map(bundle => bundle.metadata), targets, sourceSha);
  await assertCommonBindings(bundles.map(bundle => bundle.path));
  const temporary = await mkdtemp(join(tmpdir(), "acyclic-actors-package-"));
  try {
    // A clean source checkout may not contain the ignored TypeScript output.
    // Compile it before packing so the neutral parent archive is reproducible
    // from source rather than depending on a stale or empty dist directory.
    const compiled = join(temporary, "dist");
    run("bun", ["x", "tsc", "-p", packageTsconfig, "--outDir", compiled, "--pretty", "false"]);
    const compiledEntrypoint = join(compiled, "index.js");
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
      join(root, "scripts/build-wasm.mjs"), "actors",
      join(parentRoot, "generated/wasm"),
    ]);
    await mkdir(output, { recursive: true });
    const companionOutput = join(output, "actors-native");
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
      for (const name of ["native-targets.json", "generation-manifest.json"]) {
        await cp(join(bundle.path, name), join(attestation, name));
        await cp(join(bundle.path, name), join(companionRoot, name));
      }
      await writeFile(join(companionRoot, "package.json"), `${JSON.stringify(qualifiedCompanionManifest(companionManifest), null, 2)}\n`);
      const archive = `actors-native/${run("npm", ["pack", "--ignore-scripts", "--pack-destination", companionOutput, "--silent"], { cwd: companionRoot })}`;
      companions.push({ name: companionManifest.name, version: companionManifest.version, asset: archive, selected_target: bundle.metadata.selected_target, os: companionManifest.os, cpu: companionManifest.cpu, libc: companionManifest.libc, artifact: bundle.metadata.artifact, generation_sha256: bundle.metadata.generation_sha256, sha256: digest(join(output, archive)) });
    }
    const assembly = { schema: "acyclic.actors.native-package-assembly.v2", source_commit: sourceSha, source_sha256: bundles[0].metadata.source_sha256, targets, companions };
    await writeFile(join(native, "native-targets.json"), `${JSON.stringify(assembly, null, 2)}\n`);
    const parentManifest = { ...manifest, napi: { ...manifest.napi, targets }, optionalDependencies: Object.fromEntries(companions.map(companion => [companion.name, companion.version])) };
    await writeFile(join(parentRoot, "package.json"), `${JSON.stringify(parentManifest, null, 2)}\n`);
    const parentArchive = run("npm", ["pack", "--ignore-scripts", "--pack-destination", output, "--silent"], { cwd: parentRoot });
    await assertSourceSnapshot(source);
    if (run("git", ["rev-parse", "HEAD"]) !== sourceSha) fail("source revision changed during package assembly");
    const receipt = { ...assembly, parent: { name: manifest.name, version: manifest.version, asset: parentArchive, sha256: digest(join(output, parentArchive)) } };
    await writeFile(join(output, "ACTORS_NATIVE_PACKAGE.json"), `${JSON.stringify(receipt, null, 2)}\n`);
    await writeFile(join(output, "SHA256SUMS"), [receipt.parent, ...companions].map(entry => `${entry.sha256}  ${entry.asset}`).sort().join("\n") + "\n");
    await assertExactInventory(output, new Set([parentArchive, "actors-native", "ACTORS_NATIVE_PACKAGE.json", "SHA256SUMS"]));
    await assertExactInventory(companionOutput, new Set(companions.map(companion => companion.asset.slice("actors-native/".length))));
    console.log(JSON.stringify(receipt));
  } finally {
    await rm(temporary, { recursive: true, force: true });
  }
}

if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await main();
