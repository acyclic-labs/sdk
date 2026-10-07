import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { cp, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const packagePath = resolve(root, "typescript/packages/actors");
const packageTsconfig = resolve(packagePath, "tsconfig.json");

function fail(message) { throw new Error(message); }

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

function digest(path) {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}

function parseArgs(argv) {
  const options = {};
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--bundle") options.bundle = argv[++index];
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
    console.log("usage: assemble-actors-native-package.mjs --bundle BUNDLE --output OUTPUT --source-sha SIGNED_HEAD");
    return;
  }
  if (!options.bundle || !options.output || !/^[0-9a-f]{40,64}$/.test(options.sourceSha ?? "")) {
    fail("usage: assemble-actors-native-package.mjs --bundle BUNDLE --output OUTPUT --source-sha SIGNED_HEAD");
  }
  const sourceSha = run("git", ["rev-parse", "HEAD"]);
  if (sourceSha !== options.sourceSha) fail(`source ${options.sourceSha} differs from checkout ${sourceSha}`);
  const bundle = resolve(options.bundle);
  const output = resolve(options.output);
  const metadata = JSON.parse(await readFile(join(bundle, "native-targets.json"), "utf8"));
  if (metadata.source_revision !== sourceSha) fail("native bundle source revision differs from signed source");
  for (const name of ["acyclic_actors_wasm.js", "acyclic_actors_wasm_bg.wasm"]) {
    await readFile(join(packagePath, "generated", "wasm", name));
  }
  const manifest = JSON.parse(await readFile(join(packagePath, "package.json"), "utf8"));
  const temporary = await mkdtemp(join(tmpdir(), "acyclic-actors-package-"));
  try {
    // A clean source checkout may not contain the ignored TypeScript output.
    // Compile it before packing so the neutral parent archive is reproducible
    // from source rather than depending on a stale or empty dist directory.
    run("bun", ["x", "tsc", "-p", packageTsconfig, "--pretty", "false"]);
    const compiledEntrypoint = join(packagePath, "dist", "index.js");
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
      filter: (source) => !source.endsWith(".node"),
    });
    run(process.execPath, [
      join(root, "scripts/build-actors-native.mjs"), "stage", "--bundle", bundle,
      "--output", join(parentRoot, "generated/native"),
    ]);
    await mkdir(output, { recursive: true });
    const parentArchive = run("npm", ["pack", "--ignore-scripts", "--pack-destination", output, "--silent"], { cwd: parentRoot });

    // Let the maintained NAPI-RS 3.10.5 package APIs own target naming,
    // package metadata, cpu/os/libc selectors, and the binary file list. The
    // Rust bundle remains the attested input; the temporary config supplies
    // its one selected target without adding a second checked-in target list.
    const napiConfig = { ...manifest, napi: { ...manifest.napi, targets: [metadata.selected_target] } };
    await writeFile(napiConfigPath, `${JSON.stringify(napiConfig, null, 2)}\n`);
    const { NapiCli } = await import("@napi-rs/cli");
    const napi = new NapiCli();
    const npmDir = join(temporary, "npm");
    await napi.createNpmDirs({ cwd: temporary, packageJsonPath: napiConfigPath, npmDir });
    await napi.artifacts({ cwd: temporary, packageJsonPath: napiConfigPath, npmDir, outputDir: bundle });
    const companionEntries = (await readdir(npmDir, { withFileTypes: true })).filter(entry => entry.isDirectory());
    if (companionEntries.length !== 1) fail(`NAPI-RS produced ${companionEntries.length} companion package directories`);
    const companionRoot = join(npmDir, companionEntries[0].name);
    const companionManifest = JSON.parse(await readFile(join(companionRoot, "package.json"), "utf8"));
    const companionArchive = run("npm", ["pack", "--ignore-scripts", "--pack-destination", output, "--silent"], { cwd: companionRoot });
    const receipt = {
      schema: "acyclic.actors.native-package-assembly.v1",
      source_commit: sourceSha,
      parent: parentArchive,
      companion: companionArchive,
      companion_package: companionManifest.name,
      selected_target: metadata.selected_target,
      native_generation: metadata.generation_sha256,
      archive_sha256: {
        parent: digest(join(output, parentArchive)),
        companion: digest(join(output, companionArchive)),
      },
    };
    await writeFile(join(output, "ACTORS_NATIVE_PACKAGE.json"), `${JSON.stringify(receipt, null, 2)}\n`);
    console.log(JSON.stringify(receipt));
  } finally {
    await rm(temporary, { recursive: true, force: true });
  }
}

await main();
