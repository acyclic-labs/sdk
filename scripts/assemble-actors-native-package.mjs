import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { cp, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const packagePath = resolve(root, "typescript/packages/actors");

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

function companionName(target) {
  const names = {
    "x86_64-unknown-linux-gnu": "linux-x64-gnu",
    "x86_64-unknown-linux-musl": "linux-x64-musl",
    "aarch64-unknown-linux-gnu": "linux-arm64-gnu",
    "aarch64-unknown-linux-musl": "linux-arm64-musl",
    "x86_64-apple-darwin": "darwin-x64",
    "aarch64-apple-darwin": "darwin-arm64",
    "x86_64-pc-windows-msvc": "win32-x64-msvc",
    "aarch64-pc-windows-msvc": "win32-arm64-msvc",
  };
  const suffix = names[target];
  if (suffix === undefined) fail(`unsupported Actors N-API target ${target}`);
  return `@acyclic-labs/actors-${suffix}`;
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
  const manifest = JSON.parse(await readFile(join(packagePath, "package.json"), "utf8"));
  const temporary = await mkdtemp(join(tmpdir(), "acyclic-actors-package-"));
  try {
    const parentRoot = join(temporary, "parent");
    const companionRoot = join(temporary, "companion");
    await cp(packagePath, parentRoot, { recursive: true, filter: (source) => !source.includes(`${join("generated", "native")}`) });
    await rm(join(parentRoot, "generated/native"), { recursive: true, force: true });
    await mkdir(output, { recursive: true });
    const parentArchive = run("npm", ["pack", "--ignore-scripts", "--pack-destination", output, "--silent"], { cwd: parentRoot });

    await mkdir(join(companionRoot, "generated/native"), { recursive: true });
    run(process.execPath, ["scripts/build-actors-native.mjs", "stage", "--bundle", bundle, "--output", join(companionRoot, "generated/native")]);
    const companionManifest = {
      name: companionName(metadata.selected_target),
      version: manifest.version,
      private: false,
      description: `Native Actors Rust transport for ${metadata.selected_target}`,
      type: "module",
      files: ["generated/native"],
      main: "generated/native/binding.cjs",
      exports: {
        ".": { types: "./generated/native/binding.d.ts", default: "./generated/native/binding.cjs" },
        "./native-targets.json": "./generated/native/native-targets.json",
      },
    };
    await writeFile(join(companionRoot, "package.json"), `${JSON.stringify(companionManifest, null, 2)}\n`);
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
