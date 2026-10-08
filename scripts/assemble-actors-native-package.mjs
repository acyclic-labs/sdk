import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { cp, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { assertBundle, assertExactInventory, assertSourceSnapshot, rustMetadata, sourceSnapshot } from "./build-actors-native.mjs";

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
      filter: (source) => source !== join(packagePath, "generated/native") && source !== join(packagePath, "dist") && !source.endsWith(".node"),
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
        companionManifest.files.push(name);
      }
      companionManifest.private = false;
      await writeFile(join(companionRoot, "package.json"), `${JSON.stringify(companionManifest, null, 2)}\n`);
      const archive = run("npm", ["pack", "--ignore-scripts", "--pack-destination", output, "--silent"], { cwd: companionRoot });
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
    await assertExactInventory(output, new Set([parentArchive, ...companions.map(companion => companion.asset), "ACTORS_NATIVE_PACKAGE.json"]));
    console.log(JSON.stringify(receipt));
  } finally {
    await rm(temporary, { recursive: true, force: true });
  }
}

if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await main();
