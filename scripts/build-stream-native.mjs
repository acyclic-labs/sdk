import { createHash, randomUUID } from "node:crypto";
import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";
import { chmod, cp, lstat, mkdir, mkdtemp, rename, readdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { createRequire } from "node:module";
import { basename, delimiter, dirname, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const manifestRelative = "rust/crates/stream-napi/Cargo.toml";
const packageRelative = "typescript/packages/stream/package.json";
const defaultOutput = resolve(root, "typescript/packages/stream/generated/native");
const nativeTargetsSchema = "acyclic.stream.native-targets.v1";
const generationSchema = "acyclic.stream.native-generation.v1";
const buildInputsSchema = "acyclic.stream.native-build-inputs.v3";
const buildInputsReceiptSchema = "acyclic.stream.native-build-inputs-receipt.v1";
const generationManifestName = "generation-manifest.json";
const buildInputsReceiptName = "stream-native-build-inputs.receipt.json";
const cargoCacheTagSignature = "Signature: 8a477f597d28d172789f06886806bc55";
const cargoCacheTag = `${cargoCacheTagSignature}\n# This file is a cache directory tag created by cargo.\n# For information about cache directory tags see https://bford.info/cachedir/\n`;
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

export async function ensureCargoTargetDirectory(targetDir) {
  await mkdir(targetDir, { recursive: true });
  const tagPath = resolve(targetDir, "CACHEDIR.TAG");
  try {
    const existing = (await readFile(tagPath)).toString("utf8");
    if (existing.split(/\r?\n/u, 1)[0] !== cargoCacheTagSignature) throw new Error(`native target directory has an invalid ${tagPath}`);
    return;
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }
  if ((await readdir(targetDir)).length !== 0) {
    throw new Error(`native target directory ${targetDir} is pre-existing without a valid CACHEDIR.TAG`);
  }
  await writeFile(tagPath, cargoCacheTag);
}

function splitRustflags(value) {
  if (typeof value !== "string" || value.trim().length === 0) return [];
  const flags = [];
  let current = "";
  /** @type {string|null} */
  let quote = null;
  const appendEscaped = (index) => {
    const next = value[index + 1];
    if (next === undefined) return false;
    if (next === "\\" || next === '"' || next === "'" || /\s/u.test(next)) {
      current += next;
      return true;
    }
    return false;
  };
  for (let index = 0; index < value.length; index += 1) {
    const character = value[index];
    if (quote !== null) {
      if (character === quote) quote = null;
      else if (character === "\\") {
        if (appendEscaped(index)) index += 1;
        else current += character;
      }
      else current += character;
    } else if (character === "'" || character === '"') quote = character;
    else if (/\s/u.test(character)) {
      if (current.length > 0) { flags.push(current); current = ""; }
    } else if (character === "\\") {
      if (appendEscaped(index)) index += 1;
      else current += character;
    }
    else current += character;
  }
  if (quote !== null) throw new Error("RUSTFLAGS contains an unterminated quote");
  if (current.length > 0) flags.push(current);
  return flags;
}

export function deterministicRustflags(sourceRoot, targetDir, target, /** @type {{plain?: string|null, encoded?: string|null}} */ { plain = process.env.RUSTFLAGS, encoded = process.env.CARGO_ENCODED_RUSTFLAGS } = {}) {
  const prior = encoded !== null && typeof encoded === "string"
    ? (encoded.length === 0 ? [] : encoded.split("\x1f"))
    : plain !== null && typeof plain === "string" ? splitRustflags(plain) : [];
  const flags = [
    ...prior,
    `--remap-path-prefix=${resolve(sourceRoot).replaceAll("\\", "/")}=/__acyclic_stream_source`,
    `--remap-path-prefix=${resolve(targetDir).replaceAll("\\", "/")}=/__acyclic_stream_target`,
  ];
  if (typeof target === "string" && target.endsWith("-pc-windows-msvc")) flags.push("-C", "target-feature=+crt-static", "-C", "link-arg=/Brepro");
  if (typeof target === "string" && target.endsWith("-apple-darwin")) {
    // NAPI-RS emits a dylib-backed addon on Darwin. Keep its Mach-O install
    // name relocatable so the package never embeds the producer's Cargo path.
    // Keep the final linker identity stable while Cargo builds in its own path.
    // rust-lld is invoked directly by rustc, so pass Darwin options directly;
    // the -Wl, prefix is only valid when the driver is Apple's ld wrapper.
    flags.push("-C", "link-arg=-install_name", "-C", "link-arg=@rpath/libacyclic_stream_napi.dylib", "-C", "link-arg=-final_output", "-C", "link-arg=libacyclic_stream_napi.dylib");
  }
  return flags.join("\x1f");
}

/**
 * Return the toolchain-owned Darwin linker and loader paths. rust-lld is used
 * deliberately: Apple's ld hashes its producer-specific output path into the
 * Mach-O UUID even when Cargo path remapping is enabled.
 */
export function darwinRustLldPaths(target, sysroot) {
  if (typeof target !== "string" || !target.endsWith("-apple-darwin")) return null;
  if (typeof sysroot !== "string" || sysroot.length === 0) throw new Error("Darwin rust-lld requires a Rust sysroot");
  return {
    linkerEnvironment: targetEnvName(target, "LINKER"),
    linker: resolve(sysroot, "lib", "rustlib", target, "bin", "rust-lld"),
    loaderPath: resolve(sysroot, "lib"),
  };
}

export function configureDarwinRustLld(target, { sysroot: suppliedSysroot, sdkRoot: suppliedSdkRoot, linkerExists = existsSync } = {}) {
  if (typeof target !== "string" || !target.endsWith("-apple-darwin")) return () => {};
  const sysroot = suppliedSysroot ?? commandOutput("rustc", ["--print", "sysroot"]);
  const paths = darwinRustLldPaths(target, sysroot);
  if (!linkerExists(paths.linker)) throw new Error(`Rust toolchain rust-lld is unavailable at ${paths.linker}`);
  const sdkRoot = suppliedSdkRoot ?? commandOutput("xcrun", ["--sdk", "macosx", "--show-sdk-path"]);
  if (sdkRoot.length === 0) throw new Error("Darwin rust-lld requires an Apple macOS SDK");
  const priorTargetLinker = envValue(paths.linkerEnvironment);
  const priorDyldLibraryPath = envValue("DYLD_LIBRARY_PATH");
  const priorSdkRoot = envValue("SDKROOT");
  process.env[paths.linkerEnvironment] = paths.linker;
  process.env.DYLD_LIBRARY_PATH = [paths.loaderPath, priorDyldLibraryPath].filter(value => typeof value === "string" && value.length > 0).join(delimiter);
  process.env.SDKROOT = sdkRoot;
  return () => {
    if (priorTargetLinker === null) delete process.env[paths.linkerEnvironment];
    else process.env[paths.linkerEnvironment] = priorTargetLinker;
    if (priorDyldLibraryPath === null) delete process.env.DYLD_LIBRARY_PATH;
    else process.env.DYLD_LIBRARY_PATH = priorDyldLibraryPath;
    if (priorSdkRoot === null) delete process.env.SDKROOT;
    else process.env.SDKROOT = priorSdkRoot;
  };
}

export async function withDeterministicRustflags(sourceRoot, targetDir, target, operation) {
  const priorRustflags = envValue("RUSTFLAGS");
  const priorEncodedRustflags = envValue("CARGO_ENCODED_RUSTFLAGS");
  const priorCargoIncremental = envValue("CARGO_INCREMENTAL");
  const priorReleaseIncremental = envValue("CARGO_PROFILE_RELEASE_INCREMENTAL");
  const restoreDarwinRustLld = configureDarwinRustLld(target);
  try {
    process.env.CARGO_INCREMENTAL = "0";
    process.env.CARGO_PROFILE_RELEASE_INCREMENTAL = "false";
    process.env.CARGO_ENCODED_RUSTFLAGS = deterministicRustflags(sourceRoot, targetDir, target, { plain: priorRustflags, encoded: priorEncodedRustflags });
    delete process.env.RUSTFLAGS;
    return await operation();
  } finally {
    if (priorRustflags === null) delete process.env.RUSTFLAGS;
    else process.env.RUSTFLAGS = priorRustflags;
    if (priorEncodedRustflags === null) delete process.env.CARGO_ENCODED_RUSTFLAGS;
    else process.env.CARGO_ENCODED_RUSTFLAGS = priorEncodedRustflags;
    if (priorCargoIncremental === null) delete process.env.CARGO_INCREMENTAL;
    else process.env.CARGO_INCREMENTAL = priorCargoIncremental;
    if (priorReleaseIncremental === null) delete process.env.CARGO_PROFILE_RELEASE_INCREMENTAL;
    else process.env.CARGO_PROFILE_RELEASE_INCREMENTAL = priorReleaseIncremental;
    restoreDarwinRustLld();
  }
}

function targetEnvName(target, suffix) {
  return `CARGO_TARGET_${target.replaceAll("-", "_").toUpperCase()}_${suffix}`;
}

export function linkerInputs(target, environment = process.env) {
  const targetLinkerName = targetEnvName(target, "LINKER");
  const configured = {
    target: envValue(targetLinkerName, environment),
  };
  return {
    configured,
    environment: {
      LINK: envValue("LINK", environment),
      CC: envValue("CC", environment),
      AR: envValue("AR", environment),
      RUSTC_LINKER: envValue("RUSTC_LINKER", environment),
      DYLD_LIBRARY_PATH: envValue("DYLD_LIBRARY_PATH", environment),
      SDKROOT: envValue("SDKROOT", environment),
      VCINSTALLDIR: envValue("VCINSTALLDIR", environment),
      VCToolsInstallDir: envValue("VCToolsInstallDir", environment),
      WindowsSdkDir: envValue("WindowsSdkDir", environment),
      VisualStudioVersion: envValue("VisualStudioVersion", environment),
    },
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
${delegate === null ? "delete environment.RUSTC_WORKSPACE_WRAPPER;" : `environment.RUSTC_WORKSPACE_WRAPPER = ${JSON.stringify(delegate)};`}
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

export async function createRustcInvocationCapture(target = "test") {
  // Keep the wrapper identity stable for Cargo's crate disambiguation. The
  // capture records remain run-specific and live outside the wrapper
  // directory, so their temporary path cannot affect the native artifact.
  const directory = await mkdtemp(resolve(tmpdir(), "acyclic-stream-rustc-wrapper-"));
  const invocations = await mkdtemp(resolve(tmpdir(), "acyclic-stream-rustc-invocations-"));
  const delegate = envValue("RUSTC_WORKSPACE_WRAPPER");
  const priorPath = envValue("PATH");
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
  const wrapperIdentity = process.platform === "win32" ? "capture.cmd" : "capture";
  process.env.PATH = priorPath === null ? directory : `${directory}${delimiter}${priorPath}`;
  process.env.RUSTC_WORKSPACE_WRAPPER = wrapperIdentity;
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
      if (delegate === null) delete process.env.RUSTC_WORKSPACE_WRAPPER;
      else process.env.RUSTC_WORKSPACE_WRAPPER = delegate;
      if (priorPath === null) delete process.env.PATH;
      else process.env.PATH = priorPath;
      await rm(directory, { recursive: true, force: true });
      await rm(invocations, { recursive: true, force: true });
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

function maintainedBunCandidates(version) {
  const executable = process.platform === "win32" ? "bun.exe" : "bun";
  const target = process.platform === "win32" ? "bun-windows-x64" : process.platform === "darwin" ? `bun-darwin-${process.arch === "arm64" ? "aarch64" : "x64"}` : `bun-linux-${process.arch === "arm64" ? "aarch64" : "x64"}`;
  const candidates = [];
  const configured = envValue("BUN_BINARY");
  if (configured !== null) candidates.push(configured);
  const tools = envValue("TOOLS_DIR");
  if (typeof tools === "string") candidates.push(resolve(tools, "bun", version, target, executable));
  candidates.push("bun");
  return [...new Set(candidates)];
}

function maintainedBunIdentity(version) {
  const observed = [];
  for (const candidate of maintainedBunCandidates(version)) {
    const identity = optionalCommandIdentity(candidate, ["--version"]);
    if (identity === null) continue;
    observed.push(identity);
    if (identity.output === version) return identity;
  }
  const versions = observed.map(identity => `${identity.command}: ${identity.output}`).join(", ");
  throw new Error(`maintained Bun ${version} is unavailable${versions.length === 0 ? "" : `; observed ${versions}`}`);
}

function normalizedPath(value, /** @type {{targetDir?: string, outputDir?: string}} */ { targetDir, outputDir } = {}) {
  if (typeof value !== "string") return value;
  let text = value.replaceAll("\\", "/");
  const prefixes = [
    [targetDir, "<target-dir>"],
    [outputDir, "<output-dir>"],
    [root, "<source-root>"],
  ];
  let replacedPrefix = false;
  for (const [prefix, replacement] of prefixes) {
    if (typeof prefix !== "string" || typeof replacement !== "string") continue;
    const normalizedPrefix = prefix.replaceAll("\\", "/").replace(/\/$/u, "");
    if (text === normalizedPrefix) return replacement;
    const prefixPattern = new RegExp(`${normalizedPrefix.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&")}(?=[/=]|$)`, "u");
    if (prefixPattern.test(text)) {
      text = text.replace(prefixPattern, replacement);
      replacedPrefix = true;
    }
  }
  if (replacedPrefix) return text;
  if (/^(?:[A-Za-z]:\/|\/|\\\\)/u.test(text)) return "<host-path>";
  return value;
}

function normalizeBuildInputPaths(value, context) {
  if (typeof value === "string") return normalizedPath(value, context);
  if (Array.isArray(value)) return value.map(item => normalizeBuildInputPaths(item, context));
  if (value === null || typeof value !== "object") return value;
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, normalizeBuildInputPaths(item, context)]));
}

function normalizeToolPath(value, context) {
  if (typeof value !== "string") return value;
  const normalized = normalizedPath(value, context);
  if (normalized !== "<host-path>") return normalized;
  const parts = value.replaceAll("\\", "/").split("/").filter(Boolean);
  return `<toolchain-path>/${parts.slice(-3).join("/")}`;
}

function normalizeToolPathList(value, context) {
  if (typeof value !== "string") return value;
  const separator = process.platform === "win32" ? ";" : ":";
  return value.split(separator).map(item => normalizeToolPath(item, context)).join(separator);
}

function normalizeFlagValue(value, context) {
  if (typeof value !== "string") return value;
  const normalizeSegment = (segment, encoded) => {
    const withToolIdentity = encoded
      ? segment.replace(/(-C(?:\s+)?linker=)(.+)$/u, (_match, prefix, linker) => `${prefix}${normalizeToolPath(linker, context)}`).replace(/^linker=(.+)$/u, (_match, linker) => `linker=${normalizeToolPath(linker, context)}`)
      : segment.replace(/(-C(?:\s+)?linker=)([^\s\u001f]+)/gu, (_match, prefix, linker) => `${prefix}${normalizeToolPath(linker, context)}`);
    return normalizedPath(withToolIdentity, context);
  };
  const encoded = value.includes("\x1f");
  return encoded ? value.split("\x1f").map(segment => normalizeSegment(segment, true)).join("\x1f") : normalizeSegment(value, false);
}

// Rust may add diagnostic-only arguments to the delegated invocation (for
// example, --diagnostic-width=79). Keep those exact arguments in the raw
// receipt, but omit them from the published recipe so equivalent builds do
// not drift with the host's diagnostic settings.
function isDiagnosticOnlyRustcArgument(value) {
  return typeof value === "string" && /^--diagnostic-width=\d+$/u.test(value);
}

export function normalizeBuildInputs(value, { targetDir, outputDir }) {
  const context = { targetDir: resolve(targetDir), outputDir: resolve(outputDir) };
  const normalized = normalizeBuildInputPaths(value, context);
  normalized.target_dir = "<target-dir>";
  normalized.runtime.node_path = "<runtime>";
  normalized.runtime.bun.actual.command = "bun";
  normalized.invocation.runtime = "bun";
  normalized.generator.options.output_dir = "<output-dir>";
  normalized.generator.options.target_dir = "<target-dir>";
  normalized.linker.actual.rustc = "rustc";
  normalized.linker.actual.linker = normalizeToolPath(value.linker.actual.linker, context);
  const rawLinkerArgs = value.linker.actual.args.filter(arg => !isDiagnosticOnlyRustcArgument(arg));
  normalized.linker.actual.args = normalized.linker.actual.args.filter(arg => !isDiagnosticOnlyRustcArgument(arg)).map((arg, index) => {
    const raw = rawLinkerArgs[index];
    if (typeof raw === "string" && raw.startsWith("-Clinker=")) return `-Clinker=${normalizeToolPath(raw.slice("-Clinker=".length), context)}`;
    if (index > 0 && rawLinkerArgs[index - 1] === "-C" && typeof raw === "string" && raw.startsWith("linker=")) return `linker=${normalizeToolPath(raw.slice("linker=".length), context)}`;
    return arg;
  });
  normalized.linker.configured.target = normalizeToolPath(value.linker.configured.target, context);
  for (const field of ["LINK", "CC", "AR", "RUSTC_LINKER"]) normalized.linker.environment[field] = normalizeToolPath(value.linker.environment[field], context);
  normalized.linker.environment.DYLD_LIBRARY_PATH = normalizeToolPathList(value.linker.environment.DYLD_LIBRARY_PATH, context);
  normalized.linker.environment.SDKROOT = normalizeToolPath(value.linker.environment.SDKROOT, context);
  for (const field of ["RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS"]) normalized.environment[field] = normalizeFlagValue(value.environment[field], context);
  return normalized;
}

export function buildInputsReceipt(raw, published) {
  assertBuildInputs(raw);
  assertBuildInputs(published);
  return {
    schema: buildInputsReceiptSchema,
    published_build_inputs_sha256: digest(Buffer.from(JSON.stringify(published))),
    raw_build_inputs: raw,
  };
}

export async function buildInputs(target, targetDir, outputDir, packageName) {
  if (typeof target !== "string" || target.length === 0) throw new Error("native build inputs require a target");
  const wrapper = envValue("RUSTC_WRAPPER");
  const maintainedBun = await maintainedBunVersion();
  const bunIdentity = maintainedBunIdentity(maintainedBun);
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
        cargo_options: ["--locked"],
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
      RUSTC_WORKSPACE_WRAPPER: envValue("RUSTC_WORKSPACE_WRAPPER"),
      CARGO_TARGET_DIR: envValue("CARGO_TARGET_DIR"),
    },
    cache: {
      wrapper,
      wrapper_version: wrapper === null ? null : optionalCommandIdentity(wrapper, ["--version"]),
      directory: envValue("SCCACHE_DIR"),
      size: envValue("SCCACHE_CACHE_SIZE"),
    },
  };
}

function assertString(value, label) {
  if (typeof value !== "string" || value.length === 0) throw new Error(`native build input ${label} is missing`);
}

function assertObject(value, label) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new Error(`native build input ${label} is invalid`);
  return value;
}

function assertStringArray(value, label) {
  if (!Array.isArray(value) || value.some(item => typeof item !== "string")) throw new Error(`native build input ${label} is invalid`);
}

function assertStringFields(value, fields, label) {
  const object = assertObject(value, label);
  for (const field of fields) assertString(object[field], `${label}.${field}`);
  return object;
}

function assertNullableStringFields(value, fields, label) {
  const object = assertObject(value, label);
  for (const field of fields) {
    if (object[field] !== null && typeof object[field] !== "string") throw new Error(`native build input ${label}.${field} is invalid`);
  }
  return object;
}

function assertDigest(value, label) {
  if (canonicalSha256(value) === undefined) throw new Error(`native build input ${label} is invalid`);
}

export function assertBuildInputs(value) {
  assertObject(value, "build inputs");
  if (value.schema !== buildInputsSchema) throw new Error("native build inputs have unsupported schema");
  assertStringFields(value, ["target", "target_dir"], "build inputs");
  const runtime = assertStringFields(value.runtime, ["node", "node_path", "platform", "arch"], "runtime");
  const bun = assertStringFields(runtime.bun, ["maintained"], "runtime.bun");
  if (bun.actual !== null) {
    const actualBun = assertStringFields(bun.actual, ["command", "output"], "runtime.bun.actual");
    assertStringArray(actualBun.args, "runtime.bun.actual.args");
  }
  const invocation = assertStringFields(value.invocation, ["script", "runtime"], "invocation");
  assertStringArray(invocation.args, "invocation.args");
  for (const compiler of ["rustc", "cargo"]) {
    const identity = assertStringFields(value.compiler?.[compiler], ["command", "output"], `compiler.${compiler}`);
    assertStringArray(identity.args, `compiler.${compiler}.args`);
  }
  const generator = assertStringFields(value.generator, ["package", "version", "package_sha256", "entry_sha256", "lock_sha256"], "generator");
  assertString(generator.package, "generator.package");
  if (value.generator.package !== "@napi-rs/cli") throw new Error("native build generator package is unsupported");
  for (const field of ["package_sha256", "entry_sha256", "lock_sha256"]) assertDigest(generator[field], `generator.${field}`);
  const generatorOptions = assertStringFields(generator.options, ["output_dir", "target_dir", "js_package_name", "js_binding", "dts"], "generator.options");
  assertStringArray(generatorOptions.cargo_options, "generator.options.cargo_options");
  if (JSON.stringify(generatorOptions.cargo_options) !== JSON.stringify(["--locked"])) throw new Error("native build generator cargo options are invalid");
  if (generatorOptions.release !== true || generatorOptions.platform !== true) throw new Error("native build generator options are invalid");
  if (generatorOptions.target !== value.target) throw new Error("native build generator target differs");
  if (generatorOptions.target_dir !== value.target_dir) throw new Error("native build generator target directory differs");
  const profile = assertStringFields(value.profile, ["name", "manifest_sha256", "config_sha256"], "profile");
  if (value.profile.name !== "release") throw new Error("native build profile is not release");
  for (const field of ["manifest_sha256", "config_sha256"]) assertDigest(profile[field], `profile.${field}`);
  assertNullableStringFields(profile, ["cargo_incremental", "release_incremental"], "profile");
  if (profile.cargo_incremental !== "0" || profile.release_incremental !== "false") throw new Error("native build incremental policy is not pinned to zero");
  assertNullableStringFields(value.environment, ["RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "CARGO_TARGET_DIR"], "environment");
  const cache = assertNullableStringFields(value.cache, ["wrapper", "directory", "size"], "cache");
  if (cache.wrapper_version !== null) assertStringFields(cache.wrapper_version, ["output"], "cache.wrapper_version");
  const linker = assertObject(value.linker, "linker");
  assertNullableStringFields(linker.configured, ["target"], "linker.configured");
  assertNullableStringFields(linker.environment, ["LINK", "CC", "AR", "RUSTC_LINKER", "DYLD_LIBRARY_PATH", "SDKROOT", "VCINSTALLDIR", "VCToolsInstallDir", "WindowsSdkDir", "VisualStudioVersion"], "linker.environment");
  const actualLinker = assertStringFields(linker.actual, ["source", "rustc", "target"], "linker.actual");
  if (actualLinker.source !== "rustc-invocation") throw new Error("native build linker invocation source is unsupported");
  if (actualLinker.linker !== null && typeof actualLinker.linker !== "string") throw new Error("native build linker invocation linker is invalid");
  assertStringArray(actualLinker.args, "linker.actual.args");
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
  const nonRegular = entries.filter(entry => allowed.has(entry.name) && (entry.isSymbolicLink() || !entry.isFile())).map(entry => entry.name);
  if (nonRegular.length > 0) throw new Error(`native bundle ${output} contains a symlink or reparse point/non-regular artifact: ${nonRegular.join(", ")}`);
}

export async function assertOwnedDirectory(output, { allowMissing = false } = {}) {
  try {
    const metadata = await lstat(output);
    if (!metadata.isDirectory() || metadata.isSymbolicLink()) throw new Error(`native bundle output is not an owned directory: ${output}`);
    return true;
  } catch (error) {
    if (error?.code === "ENOENT" && allowMissing) return false;
    throw error;
  }
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

/**
 * @param {string} output
 * @param {{ expectedTarget?: string, verifySource?: boolean }} options
 */
async function assertBundle(output, { expectedTarget, verifySource = true } = {}) {
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
  if (verifySource) {
    const revision = sourceRevision();
    const current = await sourceSnapshot();
    if (metadata.source_revision !== revision || generation.revision !== revision) throw new Error("native bundle source revision differs from current checkout");
    if (metadata.source_sha256 !== current.sha256 || generation.source_sha256 !== current.sha256) throw new Error("native bundle source closure digest differs from current checkout");
    if (JSON.stringify(metadata.source_files) !== JSON.stringify(current.files) || JSON.stringify(generation.source_files) !== JSON.stringify(current.files)) throw new Error("native bundle source file attestation differs from current checkout");
  }
  const artifacts = generation.artifacts;
  if (!Array.isArray(artifacts) || !Array.isArray(metadata.artifacts) || JSON.stringify(metadata.artifacts) !== JSON.stringify(artifacts) || metadata.artifact === undefined) throw new Error("native bundle artifact attestation is invalid");
  if (metadata.artifact.sha256 !== artifacts.find(item => item.path === metadata.artifact.path)?.sha256) throw new Error("native bundle selected artifact digest differs");
  for (const artifact of artifacts) {
    const bytes = await readFile(pathFromArtifact(output, artifact.path));
    if (artifact.sha256 !== digest(bytes) || artifact.bytes !== bytes.length) throw new Error(`native bundle artifact differs: ${artifact.path}`);
  }
  await assertExactInventory(output, new Set([
    ...artifacts.map(artifact => artifact.path.slice("generated/native/".length)),
    generationManifestName,
    "native-targets.json",
  ]));
  return { metadata, generation, artifacts };
}

async function assertExistingBundle(output) {
  if (!(await assertOwnedDirectory(output, { allowMissing: true }))) return false;
  await assertBundle(output, { verifySource: false });
  return true;
}

export async function publishBundle(candidate, output, { validateExisting = assertExistingBundle, cleanup = rm } = {}) {
  const parent = dirname(output);
  await mkdir(parent, { recursive: true });
  let existing = false;
  try {
    existing = await validateExisting(output);
  } catch (error) {
    throw new Error(`refusing to replace unowned native bundle ${output}: ${error.message}`, { cause: error });
  }
  if (!existing) {
    await rename(candidate, output);
    return;
  }
  const backup = resolve(parent, `.${basename(output)}.backup-${randomUUID()}`);
  await rename(output, backup);
  try {
    await rename(candidate, output);
  } catch (error) {
    await rename(backup, output).catch(() => {});
    throw error;
  }
  try {
    await cleanup(backup, { recursive: true, force: true });
  } catch (error) {
    // Publication has committed once the candidate is at output. Retain the
    // producer-owned backup and report cleanup separately rather than making a
    // successful build appear to have failed.
    console.warn(`native bundle backup retained at ${backup}: ${error.message}`);
  }
}

async function build(options) {
  if (options.target === undefined) throw new Error(`build requires --target <rust-triple>\n\n${usage()}`);
  assertCleanSource();
  const output = resolve(options.output ?? defaultOutput);
  await assertExistingBundle(output);
  const buildOutput = await mkdtemp(resolve(dirname(output), ".acyclic-stream-native-build-"));
  let published = false;
  try {
  const packageManifest = await packageJson();
  const { rustPackage, targets } = rustMetadata();
  assertVersion(rustPackage, packageManifest);
  if (!targets.includes(options.target)) throw new Error(`unsupported Stream N-API target ${JSON.stringify(options.target)}; expected one of ${targets.join(", ")}`);
  const revision = sourceRevision();
  const source = await sourceSnapshot();
  const targetDir = resolve(options.targetDir ?? resolve(root, "target"));
  await ensureCargoTargetDirectory(targetDir);
  const attestedInputs = await withDeterministicRustflags(root, targetDir, options.target, async () => {
    const attestedInputs = await buildInputs(options.target, targetDir, buildOutput, packageManifest.name);
    const rootManifest = await rootPackageJson();
    const expectedGeneratorVersion = rootManifest.devDependencies?.["@napi-rs/cli"];
    if (typeof expectedGeneratorVersion === "string" && expectedGeneratorVersion !== attestedInputs.generator.version) {
      throw new Error(`loaded @napi-rs/cli ${attestedInputs.generator.version} does not match package.json ${expectedGeneratorVersion}`);
    }
    const temporary = await mkdtemp(resolve(tmpdir(), "acyclic-stream-napi-package-"));
    const packagePath = resolve(temporary, `${randomUUID()}.json`);
    await writeFile(packagePath, JSON.stringify({ ...packageManifest, napi: { ...packageManifest.napi, targets } }));
    const runNapiBuild = async () => {
      const { NapiCli } = await import("@napi-rs/cli");
      const buildResult = await new NapiCli().build({
        cwd: root,
        packageJsonPath: packagePath,
        manifestPath: resolve(root, manifestRelative),
        outputDir: buildOutput,
        target: options.target,
        targetDir,
        platform: true,
        jsPackageName: packageManifest.name,
        jsBinding: "binding.cjs",
        dts: "binding.d.ts",
        cargoOptions: ["--locked"],
        release: true,
      });
      await buildResult.task;
      // NAPI's transaction helper can leave dot-prefixed recovery entries on
      // mounted filesystems after commit. They are not published artifacts.
      for (const entry of await readdir(buildOutput, { withFileTypes: true })) {
        if (entry.name.startsWith(".")) await rm(resolve(buildOutput, entry.name), { recursive: true, force: true });
      }
    };
    let rustcCapture = await createRustcInvocationCapture();
    try {
      // Remove only the two provenance files this command owns. Any other
      // pre-existing entry is rejected by bundleArtifacts rather than hidden.
      await rm(resolve(buildOutput, generationManifestName), { force: true });
      await rm(resolve(buildOutput, "native-targets.json"), { force: true });
      // Staging and checking a previously qualified bundle must work from the
      // clean publication assembly directory, which has no workspace dev
      // dependencies. Load NAPI-RS only for the build command.
      await runNapiBuild();
      try {
        attestedInputs.linker.actual = await rustcCapture.read(options.target);
      } catch (error) {
        if (!/captured 0 Stream rustc link invocations/u.test(String(error?.message))) throw error;
        await rustcCapture.close();
        execFileSync("cargo", ["clean", "--package", "acyclic-stream-napi", "--release", "--target-dir", targetDir], { cwd: root, stdio: "inherit" });
        rustcCapture = await createRustcInvocationCapture();
        await runNapiBuild();
        attestedInputs.linker.actual = await rustcCapture.read(options.target);
      }
    } finally {
      await rustcCapture.close();
      await rm(temporary, { recursive: true, force: true });
    }
    return attestedInputs;
  });
  await assertSourceSnapshot(source);
  if (sourceRevision() !== revision) throw new Error("Stream native source changed during native build");
  const publishedInputs = normalizeBuildInputs(attestedInputs, { targetDir, outputDir: buildOutput });
  const receipt = buildInputsReceipt(attestedInputs, publishedInputs);
  // Keep host-specific compiler paths and argv in the Cargo target directory;
  // publication manifests must remain stable when the checkout is relocated.
  await mkdir(targetDir, { recursive: true });
  await writeFile(resolve(targetDir, buildInputsReceiptName), `${JSON.stringify(receipt, null, 2)}\n`);
  const bundle = await bundleArtifacts(buildOutput);
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
    build_inputs: publishedInputs,
    artifacts: bundle.artifacts,
  };
  const generationBytes = Buffer.from(`${JSON.stringify(generation, null, 2)}\n`);
  await writeFile(resolve(buildOutput, generationManifestName), generationBytes);
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
    build_inputs: publishedInputs,
    generation_manifest: `generated/native/${generationManifestName}`,
    generation_sha256: digest(generationBytes),
    artifacts: bundle.artifacts,
    artifact: bundle.node,
  };
  await writeFile(resolve(buildOutput, "native-targets.json"), `${JSON.stringify(metadata, null, 2)}\n`);
  await assertBundle(buildOutput, { expectedTarget: options.target });
  await publishBundle(buildOutput, output);
  published = true;
  } finally {
    if (!published) await rm(buildOutput, { recursive: true, force: true });
  }
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
  await assertOwnedDirectory(output, { allowMissing: true });
  await mkdir(output, { recursive: true });
  await assertOwnedDirectory(output);
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
