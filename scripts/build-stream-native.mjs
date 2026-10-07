import { createHash, randomUUID } from "node:crypto";
import { execFileSync } from "node:child_process";
import { chmod, cp, lstat, mkdir, mkdtemp, readdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { createRequire } from "node:module";
import { dirname, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const manifestRelative = "rust/crates/stream-napi/Cargo.toml";
const packageRelative = "typescript/packages/stream/package.json";
const defaultOutput = resolve(root, "typescript/packages/stream/generated/native");
const nativeTargetsSchema = "acyclic.stream.native-targets.v1";
const generationSchema = "acyclic.stream.native-generation.v1";
const buildInputsSchema = "acyclic.stream.native-build-inputs.v3";
const generationManifestName = "generation-manifest.json";
const require = createRequire(import.meta.url);
const sourceRoots = [
  "Cargo.toml",
  "Cargo.lock",
  "rust-toolchain.toml",
  ".cargo/config.toml",
  // acyclic-stream links this crate under non-WASM targets. Keep its source
  // in the attestation so a native build cannot silently use another tree.
  "rust/crates/native-runtime",
  "rust/crates/stream",
  "rust/crates/stream-napi",
  "package.json",
  "bun.lock",
  "typescript/packages/stream/package.json",
  "typescript/packages/stream/src",
  "typescript/packages/stream/generated/proto",
  "scripts/build-stream-native.mjs",
  "scripts/ensure-bun.ps1",
  "scripts/ensure-bun.sh",
];

function usage() {
  return `usage:
  node scripts/build-stream-native.mjs build --target <rust-triple> [--output <native-bundle>] [--target-dir <cargo-target-dir>]
  node scripts/build-stream-native.mjs stage --bundle <native-bundle> [--output <package-native-dir>]
  node scripts/build-stream-native.mjs check [--output <package-native-dir>]

build requires an explicit Rust target and a clean source closure. Release and
manual matrix jobs build into a bundle, then stage copies that attested bundle
into the package. check is cheap and never invokes Cargo or NAPI-RS.`;
}

function parseArgs(argv) {
  const command = argv[0]?.startsWith("-") ? "build" : (argv[0] ?? "check");
  const rest = argv[0]?.startsWith("-") ? argv : argv.slice(1);
  const options = { command };
  for (let index = 0; index < rest.length; index += 1) {
    const arg = rest[index];
    if (arg === "--target") options.target = rest[++index];
    else if (arg === "--output") options.output = rest[++index];
    else if (arg === "--bundle") options.bundle = rest[++index];
    else if (arg === "--target-dir") options.targetDir = rest[++index];
    else if (arg === "--help" || arg === "-h") options.help = true;
    else throw new Error(`unknown option ${arg}\n\n${usage()}`);
  }
  if (options.target === undefined && rest.includes("--target")) throw new Error("--target requires a Rust target triple");
  if (options.output === undefined && rest.includes("--output")) throw new Error("--output requires a directory");
  if (options.bundle === undefined && rest.includes("--bundle")) throw new Error("--bundle requires a directory");
  if (options.targetDir === undefined && rest.includes("--target-dir")) throw new Error("--target-dir requires a directory");
  return options;
}

function digest(bytes) {
  return `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
}

function canonicalSha256(value) {
  if (typeof value !== "string") return undefined;
  if (/^[0-9a-f]{64}$/.test(value)) return `sha256:${value}`;
  if (/^sha256:[0-9a-f]{64}$/.test(value)) return value;
  return undefined;
}

function compareCodepoints(left, right) {
  const leftPoints = [...left];
  const rightPoints = [...right];
  const length = Math.min(leftPoints.length, rightPoints.length);
  for (let index = 0; index < length; index += 1) {
    if (leftPoints[index] === rightPoints[index]) continue;
    return leftPoints[index] < rightPoints[index] ? -1 : 1;
  }
  return leftPoints.length - rightPoints.length;
}

function treeDigest(files) {
  const encoded = files.map(file => `${file.path}\0${file.sha256}\0${file.bytes}\n`).join("");
  return digest(Buffer.from(encoded));
}

async function collectFiles(relativePath, files) {
  const absolute = resolve(root, relativePath);
  const metadata = await lstat(absolute);
  if (metadata.isSymbolicLink()) throw new Error(`source closure contains a symlink or reparse point: ${relativePath}`);
  if (metadata.isFile()) {
    const bytes = await readFile(absolute);
    files.push({ path: relativePath.replaceAll(sep, "/"), sha256: digest(bytes), bytes: bytes.length });
    return;
  }
  if (!metadata.isDirectory()) throw new Error(`source closure entry is not a file or directory: ${relativePath}`);
  const entries = (await readdir(absolute, { withFileTypes: true })).sort((left, right) => compareCodepoints(left.name, right.name));
  for (const entry of entries) {
    if (entry.isSymbolicLink()) throw new Error(`source closure contains a symlink or reparse point: ${relativePath}/${entry.name}`);
    await collectFiles(`${relativePath}/${entry.name}`, files);
  }
}

export async function sourceSnapshot() {
  const files = [];
  for (const sourceRoot of sourceRoots) await collectFiles(sourceRoot, files);
  files.sort((left, right) => compareCodepoints(left.path, right.path));
  return { files, sha256: treeDigest(files) };
}

export async function assertSourceSnapshot(expected) {
  const current = await sourceSnapshot();
  if (current.sha256 !== expected.sha256 || JSON.stringify(current.files) !== JSON.stringify(expected.files)) {
    throw new Error("Stream native source closure changed after qualification");
  }
  return current;
}

function sourceRevision() {
  const revision = execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim();
  if (!/^[0-9a-f]{40,64}$/.test(revision)) throw new Error("git source revision is unavailable for native artifact provenance");
  return revision;
}

function commandOutput(command, args) {
  return execFileSync(command, args, {
    cwd: root,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  }).trim();
}

function requiredCommandIdentity(command, args) {
  const output = commandOutput(command, args);
  if (output.length === 0) throw new Error(`${command} did not report a version`);
  return { command, args, output };
}

function optionalCommandIdentity(command, args) {
  try {
    return requiredCommandIdentity(command, args);
  } catch (error) {
    const output = `${error.stdout ?? ""}${error.stderr ?? ""}`.trim();
    return output.length === 0 ? null : { command, args, output };
  }
}

function envValue(name, environment = process.env) {
  return Object.prototype.hasOwnProperty.call(environment, name) ? environment[name] : null;
}

function targetEnvName(target, suffix) {
  return `CARGO_TARGET_${target.replaceAll("-", "_").toUpperCase()}_${suffix}`;
}

export function linkerInputs(target, environment = process.env) {
  const targetLinkerName = targetEnvName(target, "LINKER");
  const configured = {
    target: envValue(targetLinkerName, environment),
    rustc: null,
  };
  const linkerCommand = configured.target ?? configured.rustc;
  const defaultCommand = linkerCommand === null
    ? (target.endsWith("-msvc") ? "link.exe" : target.endsWith("-gnu") ? "cc" : null)
    : null;
  const command = linkerCommand ?? defaultCommand;
  return {
    configured,
    environment: {
      LINK: envValue("LINK", environment),
      CC: envValue("CC", environment),
      AR: envValue("AR", environment),
      RUSTC_LINKER: envValue("RUSTC_LINKER", environment),
      VCINSTALLDIR: envValue("VCINSTALLDIR", environment),
      VCToolsInstallDir: envValue("VCToolsInstallDir", environment),
      WindowsSdkDir: envValue("WindowsSdkDir", environment),
      VisualStudioVersion: envValue("VisualStudioVersion", environment),
    },
    default: command,
  };
}

function linkerFromRustcArgs(args) {
  let linker = null;
  for (let index = 0; index < args.length; index += 1) {
    if (args[index] === "-C" && typeof args[index + 1] === "string" && args[index + 1].startsWith("linker=")) linker = args[index + 1].slice("linker=".length);
    if (typeof args[index] === "string" && args[index].startsWith("-Clinker=")) linker = args[index].slice("-Clinker=".length);
  }
  return linker;
}

function rustcEmitArgs(args) {
  for (let index = 0; index < args.length; index += 1) {
    if (args[index] === "--emit") return args[index + 1] ?? "";
    if (typeof args[index] === "string" && args[index].startsWith("--emit=")) return args[index].slice("--emit=".length);
  }
  return "";
}

function captureWrapperSource(delegate, captureDirectory) {
  return `import { randomUUID } from "node:crypto";
import { writeFileSync } from "node:fs";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
const args = process.argv.slice(2);
const rustc = args.shift();
if (!rustc) process.exit(1);
writeFileSync(join(${JSON.stringify(captureDirectory)}, \`${"${process.pid}"}-${randomUUID()}.json\`), JSON.stringify({ rustc, args }));
const environment = { ...process.env };
${delegate === null ? "delete environment.RUSTC_WRAPPER;" : `environment.RUSTC_WRAPPER = ${JSON.stringify(delegate)};`}
const command = ${delegate === null ? "rustc" : JSON.stringify(delegate)};
const commandArgs = ${delegate === null ? "args" : "[rustc, ...args]"};
const batch = process.platform === "win32" && /\\.(?:cmd|bat)$/iu.test(command);
const quoteCommandArg = value => {
  const text = String(value);
  return /[\\s"&|<>^]/u.test(text) ? "\\\"" + text.replaceAll("\\\"", "\\\"\\\"") + "\\\"" : text;
};
const commandLine = ["call", command, ...commandArgs].map(quoteCommandArg).join(" ");
const result = batch
  ? spawnSync(process.env.ComSpec ?? "cmd.exe", ["/d", "/s", "/c", commandLine], { env: environment, stdio: "inherit", windowsVerbatimArguments: true })
  : spawnSync(command, commandArgs, { env: environment, stdio: "inherit" });
if (result.error) throw result.error;
process.exit(result.status ?? 1);
`;
}

export async function createRustcInvocationCapture() {
  const directory = await mkdtemp(resolve(tmpdir(), "acyclic-stream-rustc-capture-"));
  const invocations = resolve(directory, "invocations");
  await mkdir(invocations);
  const delegate = envValue("RUSTC_WRAPPER");
  const source = resolve(directory, process.platform === "win32" ? "capture.mjs" : "capture");
  await writeFile(source, captureWrapperSource(delegate, invocations), { mode: 0o700 });
  let wrapper = source;
  if (process.platform === "win32") {
    wrapper = resolve(directory, "capture.cmd");
    await writeFile(wrapper, `@echo off\r\n"${process.execPath}" "${source}" %*\r\nexit /b %errorlevel%\r\n`);
  } else {
    await writeFile(source, `#!/usr/bin/env node\n${captureWrapperSource(delegate, invocations)}`, { mode: 0o700 });
    await chmod(source, 0o700);
  }
  process.env.RUSTC_WRAPPER = wrapper;
  return {
    wrapper,
    async read(target) {
      const entries = [];
      for (const name of await readdir(invocations)) {
        if (!name.endsWith(".json")) continue;
        entries.push(JSON.parse((await readFile(resolve(invocations, name))).toString("utf8")));
      }
      const matches = entries.filter(entry => {
        const crateIndex = entry.args.indexOf("--crate-name");
        const emit = rustcEmitArgs(entry.args);
        return entry.args[crateIndex + 1] === "acyclic_stream_napi" && typeof emit === "string" && emit.split(",").includes("link");
      });
      if (matches.length !== 1) throw new Error(`native build captured ${matches.length} Stream rustc link invocations`);
      const match = matches[0];
      return {
        source: "rustc-invocation",
        rustc: match.rustc,
        target,
        linker: linkerFromRustcArgs(match.args),
        args: match.args,
      };
    },
    async close() {
      if (delegate === null) delete process.env.RUSTC_WRAPPER;
      else process.env.RUSTC_WRAPPER = delegate;
      await rm(directory, { recursive: true, force: true });
    },
  };
}

async function napiGeneratorIdentity() {
  const packagePath = require.resolve("@napi-rs/cli/package.json");
  const entryPath = require.resolve("@napi-rs/cli");
  const packageBytes = await readFile(packagePath);
  const entryBytes = await readFile(entryPath);
  const packageManifest = JSON.parse(packageBytes.toString("utf8"));
  if (typeof packageManifest.version !== "string" || packageManifest.version.length === 0) {
    throw new Error("@napi-rs/cli package version is unavailable");
  }
  return {
    package: "@napi-rs/cli",
    version: packageManifest.version,
    package_sha256: digest(packageBytes),
    entry_sha256: digest(entryBytes),
    lock_sha256: digest(await readFile(resolve(root, "bun.lock"))),
  };
}

async function maintainedBunVersion() {
  const versions = [];
  for (const relativePath of ["scripts/ensure-bun.ps1", "scripts/ensure-bun.sh"]) {
    const source = await readFile(resolve(root, relativePath), "utf8");
    const match = source.match(/(?:\$?version)\s*=\s*["']?([0-9]+(?:\.[0-9]+)+)/iu);
    if (match === null) throw new Error(`${relativePath} does not declare a Bun version`);
    versions.push(match[1]);
  }
  if (new Set(versions).size !== 1) throw new Error("Bun bootstrap scripts disagree on their maintained version");
  return versions[0];
}

export async function buildInputs(target, targetDir, outputDir, packageName) {
  if (typeof target !== "string" || target.length === 0) throw new Error("native build inputs require a target");
  const wrapper = envValue("RUSTC_WRAPPER");
  const wrapperCommand = wrapper === null ? null : wrapper.trim();
  const maintainedBun = await maintainedBunVersion();
  const bunIdentity = optionalCommandIdentity("bun", ["--version"]);
  if (bunIdentity !== null && bunIdentity.output !== maintainedBun) throw new Error(`loaded Bun ${bunIdentity.output} does not match maintained version ${maintainedBun}`);
  const configBytes = await readFile(resolve(root, ".cargo/config.toml"));
  return {
    schema: buildInputsSchema,
    target,
    target_dir: resolve(targetDir),
    runtime: {
      node: process.version,
      node_path: process.execPath,
      platform: process.platform,
      arch: process.arch,
      bun: { maintained: maintainedBun, actual: bunIdentity },
    },
    invocation: {
      script: "scripts/build-stream-native.mjs",
      runtime: process.execPath,
      args: process.argv.slice(2),
    },
    compiler: {
      rustc: requiredCommandIdentity("rustc", ["--version", "--verbose"]),
      cargo: requiredCommandIdentity("cargo", ["--version", "--verbose"]),
    },
    generator: {
      ...await napiGeneratorIdentity(),
      options: {
        release: true,
        platform: true,
        target,
        output_dir: resolve(outputDir),
        target_dir: resolve(targetDir),
        js_package_name: packageName,
        js_binding: "binding.cjs",
        dts: "binding.d.ts",
      },
    },
    linker: linkerInputs(target),
    profile: {
      name: "release",
      cargo_incremental: envValue("CARGO_INCREMENTAL"),
      release_incremental: envValue("CARGO_PROFILE_RELEASE_INCREMENTAL"),
      manifest_sha256: digest(await readFile(resolve(root, "Cargo.toml"))),
      config_sha256: digest(configBytes),
    },
    environment: {
      RUSTFLAGS: envValue("RUSTFLAGS"),
      CARGO_ENCODED_RUSTFLAGS: envValue("CARGO_ENCODED_RUSTFLAGS"),
      RUSTC_WRAPPER: wrapper,
      CARGO_TARGET_DIR: envValue("CARGO_TARGET_DIR"),
    },
    cache: {
      wrapper,
      wrapper_command: wrapperCommand,
      wrapper_version: wrapperCommand === null ? null : optionalCommandIdentity(wrapperCommand, ["--version"]),
      directory: envValue("SCCACHE_DIR"),
      size: envValue("SCCACHE_CACHE_SIZE"),
    },
  };
}

function assertString(value, label) {
  if (typeof value !== "string" || value.length === 0) throw new Error(`native build input ${label} is missing`);
}

function assertDigest(value, label) {
  if (canonicalSha256(value) === undefined) throw new Error(`native build input ${label} is invalid`);
}

export function assertBuildInputs(value) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new Error("native build inputs are invalid");
  if (value.schema !== buildInputsSchema) throw new Error("native build inputs have unsupported schema");
  assertString(value.target, "target");
  assertString(value.target_dir, "target_dir");
  for (const label of ["node", "node_path", "platform", "arch"]) assertString(value.runtime?.[label], `runtime.${label}`);
  if (value.runtime?.bun === null || typeof value.runtime?.bun !== "object") throw new Error("native build Bun identity is missing");
  assertString(value.runtime.bun.maintained, "runtime.bun.maintained");
  if (value.runtime.bun.actual !== null && (typeof value.runtime.bun.actual !== "object" || typeof value.runtime.bun.actual.output !== "string")) throw new Error("native build Bun identity is invalid");
  assertString(value.invocation?.script, "invocation.script");
  assertString(value.invocation?.runtime, "invocation.runtime");
  if (!Array.isArray(value.invocation.args) || value.invocation.args.some(item => typeof item !== "string")) throw new Error("native build invocation arguments are invalid");
  for (const compiler of ["rustc", "cargo"]) {
    if (value.compiler?.[compiler] === null || typeof value.compiler?.[compiler] !== "object") throw new Error(`native build input compiler.${compiler} is missing`);
    assertString(value.compiler[compiler].command, `compiler.${compiler}.command`);
    assertString(value.compiler[compiler].output, `compiler.${compiler}.output`);
    if (!Array.isArray(value.compiler[compiler].args)) throw new Error(`native build input compiler.${compiler}.args is invalid`);
  }
  assertString(value.generator?.package, "generator.package");
  if (value.generator.package !== "@napi-rs/cli") throw new Error("native build generator package is unsupported");
  assertString(value.generator?.version, "generator.version");
  assertString(value.generator?.package_sha256, "generator.package_sha256");
  assertString(value.generator?.entry_sha256, "generator.entry_sha256");
  assertString(value.generator?.lock_sha256, "generator.lock_sha256");
  for (const field of ["package_sha256", "entry_sha256", "lock_sha256"]) assertDigest(value.generator[field], `generator.${field}`);
  const generatorOptions = value.generator.options;
  if (generatorOptions === null || typeof generatorOptions !== "object") throw new Error("native build generator options are missing");
  for (const field of ["output_dir", "target_dir", "js_package_name", "js_binding", "dts"]) assertString(generatorOptions[field], `generator.options.${field}`);
  if (generatorOptions.release !== true || generatorOptions.platform !== true) throw new Error("native build generator options are invalid");
  if (generatorOptions.target !== value.target) throw new Error("native build generator target differs");
  if (generatorOptions.target_dir !== value.target_dir) throw new Error("native build generator target directory differs");
  assertString(value.profile?.name, "profile.name");
  if (value.profile.name !== "release") throw new Error("native build profile is not release");
  for (const field of ["manifest_sha256", "config_sha256"]) {
    assertString(value.profile?.[field], `profile.${field}`);
    assertDigest(value.profile[field], `profile.${field}`);
  }
  for (const field of ["cargo_incremental", "release_incremental", "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTC_WRAPPER", "CARGO_TARGET_DIR"]) {
    const section = ["cargo_incremental", "release_incremental"].includes(field) ? value.profile : value.environment;
    if (section[field] !== null && typeof section[field] !== "string") throw new Error(`native build input ${field} is invalid`);
  }
  if (value.cache === null || typeof value.cache !== "object") throw new Error("native build cache inputs are missing");
  for (const field of ["wrapper", "wrapper_command", "directory", "size"]) {
    if (value.cache[field] !== null && typeof value.cache[field] !== "string") throw new Error(`native build cache input ${field} is invalid`);
  }
  if (value.cache.wrapper_version !== null && (typeof value.cache.wrapper_version !== "object" || typeof value.cache.wrapper_version.output !== "string")) throw new Error("native build cache wrapper identity is invalid");
  if (value.linker === null || typeof value.linker !== "object") throw new Error("native build linker inputs are missing");
  for (const section of ["configured", "environment"]) {
    if (value.linker[section] === null || typeof value.linker[section] !== "object") throw new Error(`native build linker ${section} inputs are missing`);
    for (const item of Object.values(value.linker[section])) if (item !== null && typeof item !== "string") throw new Error("native build linker environment input is invalid");
  }
  if (value.linker.default !== null && typeof value.linker.default !== "string") throw new Error("native build linker default is invalid");
  const actualLinker = value.linker.actual;
  if (actualLinker === null || typeof actualLinker !== "object") throw new Error("native build linker invocation is missing");
  assertString(actualLinker.source, "linker.actual.source");
  if (actualLinker.source !== "rustc-invocation") throw new Error("native build linker invocation source is unsupported");
  assertString(actualLinker.rustc, "linker.actual.rustc");
  assertString(actualLinker.target, "linker.actual.target");
  if (actualLinker.linker !== null && typeof actualLinker.linker !== "string") throw new Error("native build linker invocation linker is invalid");
  if (!Array.isArray(actualLinker.args) || actualLinker.args.some(item => typeof item !== "string")) throw new Error("native build linker invocation arguments are invalid");
  return value;
}

export function assertMatchingBuildInputs(left, right) {
  assertBuildInputs(left);
  assertBuildInputs(right);
  if (JSON.stringify(left) !== JSON.stringify(right)) throw new Error("native bundle build input attestation differs");
}

function assertCleanSource() {
  const status = execFileSync("git", ["status", "--porcelain=v1", "--untracked-files=all", "--", ...sourceRoots], { cwd: root, encoding: "utf8" });
  if (status.trim() !== "") throw new Error("Stream native build requires a clean source closure; commit or stage source changes before building");
}

function rustMetadata() {
  const metadata = JSON.parse(execFileSync("cargo", ["metadata", "--locked", "--no-deps", "--format-version", "1"], { cwd: root, encoding: "utf8" }));
  const rustPackage = metadata.packages.find(item => item.name === "acyclic-stream-napi");
  const targets = rustPackage?.metadata?.napi?.targets;
  if (!rustPackage || !Array.isArray(targets) || targets.length === 0 || targets.some(target => typeof target !== "string" || target.length === 0) || new Set(targets).size !== targets.length) {
    throw new Error(`${manifestRelative} must declare unique package.metadata.napi.targets in Rust`);
  }
  return { rustPackage, targets };
}

async function packageJson() {
  return JSON.parse(await readFile(resolve(root, packageRelative), "utf8"));
}

async function rootPackageJson() {
  return JSON.parse(await readFile(resolve(root, "package.json"), "utf8"));
}

function assertVersion(rustPackage, packageManifest) {
  if (rustPackage.version !== packageManifest.version) throw new Error(`acyclic-stream-napi ${rustPackage.version} does not match ${packageRelative} ${packageManifest.version}`);
}

function relativeArtifactPath(name) {
  return `generated/native/${name}`;
}

export async function assertExactInventory(output, allowed) {
  const entries = await readdir(output, { withFileTypes: true });
  const extras = entries.map(entry => entry.name).filter(name => !allowed.has(name));
  if (extras.length > 0) throw new Error(`native bundle ${output} contains unstated files: ${extras.join(", ")}`);
}

async function bundleArtifacts(output) {
  const names = (await readdir(output)).filter(name => name === "binding.cjs" || name === "binding.d.ts" || name.endsWith(".node")).sort();
  await assertExactInventory(output, new Set([...names, generationManifestName, "native-targets.json"]));
  if (!names.includes("binding.cjs") || !names.includes("binding.d.ts")) throw new Error(`native bundle ${output} is missing generated binding loader or declarations`);
  const nodes = names.filter(name => name.endsWith(".node"));
  if (nodes.length !== 1) throw new Error(`native bundle ${output} must contain exactly one .node artifact; found ${nodes.length}`);
  const artifacts = [];
  for (const name of names) {
    const bytes = await readFile(resolve(output, name));
    artifacts.push({ path: relativeArtifactPath(name), sha256: digest(bytes), bytes: bytes.length });
  }
  return { artifacts, node: artifacts.find(item => item.path.endsWith(".node")) };
}

function pathFromArtifact(output, artifactPath) {
  const prefix = "generated/native/";
  if (typeof artifactPath !== "string" || !artifactPath.startsWith(prefix) || artifactPath.includes("..")) throw new Error(`invalid native bundle artifact path ${JSON.stringify(artifactPath)}`);
  const candidate = resolve(output, artifactPath.slice(prefix.length));
  if (!candidate.startsWith(`${resolve(output)}${sep}`)) throw new Error(`native bundle artifact escapes ${output}`);
  return candidate;
}

async function assertBundle(output, { expectedTarget } = {}) {
  const metadataPath = resolve(output, "native-targets.json");
  const generationPath = resolve(output, generationManifestName);
  const metadataBytes = await readFile(metadataPath);
  const generationBytes = await readFile(generationPath);
  const metadata = JSON.parse(metadataBytes.toString("utf8"));
  const generation = JSON.parse(generationBytes.toString("utf8"));
  const packageManifest = await packageJson();
  const { rustPackage, targets } = rustMetadata();
  assertVersion(rustPackage, packageManifest);
  if (metadata.schema !== nativeTargetsSchema || generation.schema !== generationSchema) throw new Error(`native bundle ${output} has unsupported provenance schema`);
  assertMatchingBuildInputs(metadata.build_inputs, generation.build_inputs);
  if (metadata.package !== rustPackage.name || generation.package !== rustPackage.name) throw new Error(`native bundle ${output} names the wrong Rust package`);
  if (metadata.version !== packageManifest.version || generation.version !== packageManifest.version) throw new Error(`native bundle ${output} version does not match ${packageRelative}`);
  if (metadata.source_path !== manifestRelative || generation.source_path !== manifestRelative) throw new Error(`native bundle ${output} has the wrong Rust source path`);
  if (canonicalSha256(metadata.generation_sha256) !== digest(generationBytes)) throw new Error(`native bundle ${output} generation manifest digest differs`);
  if (metadata.generation_manifest !== `generated/native/${generationManifestName}`) throw new Error(`native bundle ${output} has the wrong generation manifest path`);
  if (JSON.stringify(metadata.targets) !== JSON.stringify(targets) || JSON.stringify(generation.targets) !== JSON.stringify(targets)) throw new Error(`native bundle ${output} target metadata differs from Rust`);
  if (expectedTarget !== undefined && metadata.selected_target !== expectedTarget) throw new Error(`native bundle selected target ${metadata.selected_target} differs from ${expectedTarget}`);
  if (!targets.includes(metadata.selected_target) || generation.selected_target !== metadata.selected_target) throw new Error("native bundle selected target is not Rust-qualified");
  const revision = sourceRevision();
  const current = await sourceSnapshot();
  if (metadata.source_revision !== revision || generation.revision !== revision) throw new Error("native bundle source revision differs from current checkout");
  if (metadata.source_sha256 !== current.sha256 || generation.source_sha256 !== current.sha256) throw new Error("native bundle source closure digest differs from current checkout");
  if (JSON.stringify(metadata.source_files) !== JSON.stringify(current.files) || JSON.stringify(generation.source_files) !== JSON.stringify(current.files)) throw new Error("native bundle source file attestation differs from current checkout");
  const artifacts = generation.artifacts;
  if (!Array.isArray(artifacts) || !Array.isArray(metadata.artifacts) || JSON.stringify(metadata.artifacts) !== JSON.stringify(artifacts) || metadata.artifact === undefined) throw new Error("native bundle artifact attestation is invalid");
  if (metadata.artifact.sha256 !== artifacts.find(item => item.path === metadata.artifact.path)?.sha256) throw new Error("native bundle selected artifact digest differs");
  for (const artifact of artifacts) {
    const bytes = await readFile(pathFromArtifact(output, artifact.path));
    if (artifact.sha256 !== digest(bytes) || artifact.bytes !== bytes.length) throw new Error(`native bundle artifact differs: ${artifact.path}`);
  }
  const bundle = await bundleArtifacts(output);
  if (JSON.stringify(bundle.artifacts) !== JSON.stringify(artifacts)) throw new Error("native bundle contains unstated or missing generated files");
  return { metadata, generation, artifacts };
}

async function build(options) {
  if (options.target === undefined) throw new Error(`build requires --target <rust-triple>\n\n${usage()}`);
  assertCleanSource();
  const output = resolve(options.output ?? defaultOutput);
  const packageManifest = await packageJson();
  const { rustPackage, targets } = rustMetadata();
  assertVersion(rustPackage, packageManifest);
  if (!targets.includes(options.target)) throw new Error(`unsupported Stream N-API target ${JSON.stringify(options.target)}; expected one of ${targets.join(", ")}`);
  const revision = sourceRevision();
  const source = await sourceSnapshot();
  const targetDir = resolve(options.targetDir ?? resolve(root, "target"));
  const attestedInputs = await buildInputs(options.target, targetDir, output, packageManifest.name);
  const rootManifest = await rootPackageJson();
  const expectedGeneratorVersion = rootManifest.devDependencies?.["@napi-rs/cli"];
  if (typeof expectedGeneratorVersion === "string" && expectedGeneratorVersion !== attestedInputs.generator.version) {
    throw new Error(`loaded @napi-rs/cli ${attestedInputs.generator.version} does not match package.json ${expectedGeneratorVersion}`);
  }
  await mkdir(output, { recursive: true });
  const temporary = await mkdtemp(resolve(tmpdir(), "acyclic-stream-napi-package-"));
  const packagePath = resolve(temporary, `${randomUUID()}.json`);
  await writeFile(packagePath, JSON.stringify({ ...packageManifest, napi: { ...packageManifest.napi, targets } }));
  const runNapiBuild = async () => {
    const { NapiCli } = await import("@napi-rs/cli");
    const buildResult = await new NapiCli().build({
      cwd: root,
      packageJsonPath: packagePath,
      manifestPath: resolve(root, manifestRelative),
      outputDir: output,
      target: options.target,
      targetDir,
      platform: true,
      jsPackageName: packageManifest.name,
      jsBinding: "binding.cjs",
      dts: "binding.d.ts",
      release: true,
    });
    await buildResult.task;
  };
  let rustcCapture = await createRustcInvocationCapture();
  try {
    // Remove only the two provenance files this command owns. Any other
    // pre-existing entry is rejected by bundleArtifacts rather than hidden.
    await rm(resolve(output, generationManifestName), { force: true });
    await rm(resolve(output, "native-targets.json"), { force: true });
    // Staging and checking a previously qualified bundle must work from the
    // clean publication assembly directory, which has no workspace dev
    // dependencies. Load NAPI-RS only for the build command.
    await runNapiBuild();
    try {
      attestedInputs.linker.actual = await rustcCapture.read(options.target);
    } catch (error) {
      if (!/captured 0 Stream rustc link invocations/u.test(String(error?.message))) throw error;
      await rustcCapture.close();
      execFileSync("cargo", ["clean", "--package", "acyclic-stream-napi", "--target-dir", targetDir], { cwd: root, stdio: "inherit" });
      rustcCapture = await createRustcInvocationCapture();
      await runNapiBuild();
      attestedInputs.linker.actual = await rustcCapture.read(options.target);
    }
  } finally {
    await rustcCapture.close();
    await rm(temporary, { recursive: true, force: true });
  }
  await assertSourceSnapshot(source);
  if (sourceRevision() !== revision) throw new Error("Stream native source changed during native build");
  const bundle = await bundleArtifacts(output);
  const generation = {
    schema: generationSchema,
    package: rustPackage.name,
    version: packageManifest.version,
    source_path: manifestRelative,
    revision,
    source_sha256: source.sha256,
    source_files: source.files,
    targets,
    selected_target: options.target,
    build_inputs: attestedInputs,
    artifacts: bundle.artifacts,
  };
  const generationBytes = Buffer.from(`${JSON.stringify(generation, null, 2)}\n`);
  await writeFile(resolve(output, generationManifestName), generationBytes);
  const metadata = {
    schema: nativeTargetsSchema,
    package: rustPackage.name,
    version: packageManifest.version,
    source_path: manifestRelative,
    source_revision: revision,
    source_sha256: source.sha256,
    source_files: source.files,
    targets,
    selected_target: options.target,
    build_inputs: attestedInputs,
    generation_manifest: `generated/native/${generationManifestName}`,
    generation_sha256: digest(generationBytes),
    artifacts: bundle.artifacts,
    artifact: bundle.node,
  };
  await writeFile(resolve(output, "native-targets.json"), `${JSON.stringify(metadata, null, 2)}\n`);
  await assertBundle(output, { expectedTarget: options.target });
}

async function stage(options) {
  if (options.bundle === undefined) throw new Error(`stage requires --bundle <native-bundle>\n\n${usage()}`);
  const bundle = resolve(options.bundle);
  if (!(await stat(bundle)).isDirectory()) throw new Error(`native bundle is not a directory: ${bundle}`);
  let input = bundle;
  try { await stat(resolve(input, "native-targets.json")); }
  catch { input = resolve(bundle, "generated/native"); }
  await assertBundle(input);
  const output = resolve(options.output ?? defaultOutput);
  await mkdir(output, { recursive: true });
  const metadata = JSON.parse((await readFile(resolve(input, "native-targets.json"))).toString("utf8"));
  const expectedNames = new Set([
    ...metadata.artifacts.map(artifact => artifact.path.slice("generated/native/".length)),
    generationManifestName,
    "native-targets.json",
  ]);
  await assertExactInventory(output, expectedNames);
  for (const artifact of metadata.artifacts) await cp(pathFromArtifact(input, artifact.path), resolve(output, artifact.path.slice("generated/native/".length)));
  await cp(resolve(input, generationManifestName), resolve(output, generationManifestName));
  await cp(resolve(input, "native-targets.json"), resolve(output, "native-targets.json"));
  await assertBundle(output, { expectedTarget: metadata.selected_target });
}

async function check(options) {
  const output = resolve(options.output ?? defaultOutput);
  await assertBundle(output);
  console.log(JSON.stringify({ schema: "acyclic.stream.native-check.v1", status: "passed", output }));
}

if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) console.log(usage());
  else if (options.command === "build") await build(options);
  else if (options.command === "stage") await stage(options);
  else if (options.command === "check") await check(options);
  else throw new Error(`unknown command ${options.command}\n\n${usage()}`);
}
