#!/usr/bin/env node

import { createHash } from "node:crypto";
import { cpSync, existsSync, mkdirSync, readFileSync, readdirSync, realpathSync, rmSync, statSync, writeFileSync } from "node:fs";
import { delimiter, dirname, extname, join, relative, resolve, sep } from "node:path";
import { spawn, spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { readBoundedGzip, tarEntries } from "./archive-utils.mjs";
import { verifyQualificationSummary } from "./verify-guide-projection-receipts.mjs";

// Resolve from the script directory so this remains correct when invoked from a docs checkout, a release archive, or a clean worktree.
const repo = resolve(fileURLToPath(new URL(".", import.meta.url)), "..");
const cargo = process.env.CARGO_BIN ?? (process.platform === "win32" ? join(process.env.USERPROFILE ?? "C:\\Users\\varun", ".cargo", "bin", "cargo.exe") : "cargo");
const defaultTsc = process.platform === "win32"
  ? (() => {
      const candidates = [
        join(repo, "node_modules", ".bin", "tsc.cmd"),
        join(process.env.APPDATA ?? join(process.env.USERPROFILE ?? "C:\\Users\\varun", "AppData", "Roaming"), "npm", "tsc.cmd"),
      ];
      return candidates.find(candidate => existsSync(candidate)) ?? "tsc";
    })()
  : "tsc";
const defaultBun = process.platform === "win32"
  ? (() => {
      const candidate = join(process.env.APPDATA ?? join(process.env.USERPROFILE ?? "C:\\Users\\varun", "AppData", "Roaming"), "npm", "node_modules", "bun", "bin", "bun.exe");
      return existsSync(candidate) ? candidate : "bun";
    })()
  : "bun";
// Release qualification supplies pinned toolchain paths. Keep every local
// fallback overridable so a clean checkout can use the same resolver without
// depending on a machine's PATH layout.
const binaries = {
  rustfmt: process.env.SDK_RUSTFMT_BIN ?? process.env.RUSTFMT_BIN ?? "rustfmt",
  python: process.env.SDK_PYTHON_BIN ?? process.env.PYTHON_BIN ?? "python",
  bun: process.env.SDK_BUN_BIN ?? process.env.BUN_BIN ?? defaultBun,
  node: process.env.SDK_NODE_BIN ?? process.env.NODE_BIN ?? "node",
  tsc: process.env.SDK_TSC_BIN ?? process.env.TSC_BIN ?? defaultTsc,
  go: process.env.SDK_GO_BIN ?? process.env.GO_BIN ?? "go",
  maven: process.env.SDK_MAVEN_BIN ?? process.env.MAVEN_BIN ?? "mvn",
  dotnet: process.env.SDK_DOTNET_BIN ?? process.env.DOTNET_BIN ?? "dotnet",
  ruby: process.env.SDK_RUBY_BIN ?? process.env.RUBY_BIN ?? "ruby",
  gem: process.env.SDK_GEM_BIN ?? process.env.GEM_BIN ?? "gem",
  dart: process.env.SDK_DART_BIN ?? process.env.DART_BIN ?? "dart",
  php: process.env.SDK_PHP_BIN ?? process.env.PHP_BIN ?? "php",
  composer: process.env.SDK_COMPOSER_BIN ?? process.env.COMPOSER_BIN ?? "composer",
  java: process.env.SDK_JAVA_BIN ?? process.env.JAVA_BIN ?? "java",
};
const args = new Map();
for (let i = 2; i < process.argv.length; i += 1) {
  const value = process.argv[i];
  if (value === "--execute" || value === "--strict") args.set(value, true);
  else if (value.startsWith("--")) args.set(value, process.argv[++i]);
}

const output = resolve(args.get("--output") ?? join(repo, "work", "guide-projection-qualification"));
const snippets = join(output, "snippets");
mkdirSync(snippets, { recursive: true });
// Rust guide archives have private contract crates which are intentionally
// publish = false.  Keep their immutable package archives in the qualification
// input and overlay those archive bytes onto an isolated Cargo vendor tree.
// The consumer then resolves registry sources from that tree, never checkout
// paths or a source-patched dependency graph.
const rustGuidePackageClosure = [
  "acyclic-sdk-contract-validation",
  "acyclic-sdk-contract-options",
  "acyclic-sdk-contract-wire",
  "acyclic-sdk-remote-web",
  "acyclic-native-runtime",
  "acyclic-stream",
  "acyclic-objects",
  "acyclic-fs",
  "acyclic-machines",
  "acyclic-inference",
  "acyclic-workers",
  "acyclic-harness",
];
let rustVendorState = null;
// Keep consumer compilation artifacts outside each receipt directory so a
// stable output path can reuse them. An explicit target may point at a shared
// cache; the default remains owned by this checkout.
const cargoTargetDir = resolve(repo, process.env.CARGO_TARGET_DIR ?? process.env.SDK_CARGO_TARGET_DIR ?? join(repo, "target", "sdk-guide-qualification"));
const fixtureBinary = resolve(repo, process.env.SDK_FIXTURE_SERVER_BIN ?? join(repo, "target", "debug", process.platform === "win32" ? "fixture-server.exe" : "fixture-server"));
const expectedProjectionCount = args.has("--expected-count")
  ? Number(args.get("--expected-count"))
  : 54;
if (!Number.isInteger(expectedProjectionCount) || expectedProjectionCount < 1) {
  console.error("--expected-count must be a positive integer");
  process.exit(2);
}

function command(name, commandArgs, cwd = repo, extraEnv = {}) {
  const shell = /\.(?:cmd|bat)$/i.test(name);
  const result = spawnSync(name, commandArgs, {
    cwd,
    encoding: "utf8",
    windowsHide: true,
    shell,
    env: { ...process.env, ...extraEnv },
    timeout: Number(process.env.QUALIFY_COMMAND_TIMEOUT_MS ?? 120000),
    killSignal: "SIGTERM",
  });
  return {
    command: [name, ...commandArgs].join(" "),
    exitCode: result.status ?? 127,
    stdout: result.stdout ?? "",
    stderr: result.stderr || (result.error?.message ?? ""),
  };
}

async function startFixture() {
  let binary;
  try {
    binary = statSync(fixtureBinary).isFile() ? fixtureBinary : null;
  } catch {
    binary = null;
  }
  if (!binary) return null;
  const child = spawn(binary, ["--port", "0", "--grpc-port", "0", "--max-requests", "512"], {
    cwd: repo,
    stdio: ["ignore", "pipe", "pipe"],
    windowsHide: true,
  });
  const address = await new Promise((resolveAddress, reject) => {
    let buffer = "";
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      buffer += chunk;
      for (const line of buffer.split(/\r?\n/).slice(0, -1)) {
        try {
          const record = JSON.parse(line);
          if (record.grpc_address) {
            resolveAddress(record.grpc_address);
            return;
          }
        } catch {}
      }
      buffer = buffer.split(/\r?\n/).at(-1) ?? "";
    });
    child.once("error", reject);
    child.once("exit", (code) => reject(new Error(`fixture server exited before readiness (${code})`)));
  });
  child.stdout.unref();
  child.stderr.unref();
  child.unref();
  return { child, address };
}

function fixtureEnvironment(language, address) {
  const normalized = address.replace(/^https?:\/\//, "");
  const urlLanguages = new Set(["typescript", "csharp"]);
  return { FIXTURE_GRPC_ADDRESS: urlLanguages.has(language) ? address : normalized };
}

function allFiles(root, seen = new Set()) {
  const files = [];
  if (!existsSync(root)) return files;
  const pending = [root];
  while (pending.length) {
    const directory = pending.pop();
    const canonical = realpathSync(directory);
    if (seen.has(canonical)) continue;
    seen.add(canonical);
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name);
      if (entry.isDirectory() && !entry.isSymbolicLink()) pending.push(path);
      else files.push(path);
    }
  }
  return files;
}

function artifact(root, pattern) {
  const normalized = pattern.replaceAll("\\", "/");
  if (!normalized.includes("*")) {
    const path = resolve(root, normalized);
    if (!existsSync(path) || !statSync(path).isFile() || !within(root, path)) return null;
    // Do not follow a symlink from the source snapshot to an unbounded file.
    const canonicalRoot = realpathSync(root);
    const canonicalPath = realpathSync(path);
    return canonicalPath === canonicalRoot || canonicalPath.startsWith(`${canonicalRoot}${sep}`)
      ? path
      : null;
  }
  const expression = new RegExp(`^${normalized.split("*").map((part) => part.replace(/[.+?^${}()|[\\]\\]/g, "\\$&")).join(".*")}$`, "i");
  const staticPrefix = normalized.slice(0, normalized.indexOf("*"));
  const prefixSlash = staticPrefix.lastIndexOf("/");
  const searchRoot = resolve(root, prefixSlash >= 0 ? staticPrefix.slice(0, prefixSlash) : ".");
  const canonicalRoot = realpathSync(root);
  const matches = allFiles(searchRoot).filter((path) => {
    if (!within(root, path) || !existsSync(path)) return false;
    let canonicalPath;
    try {
      canonicalPath = realpathSync(path);
    } catch {
      return false;
    }
    return (canonicalPath === canonicalRoot || canonicalPath.startsWith(`${canonicalRoot}${sep}`))
      && expression.test(relative(root, path).replaceAll("\\", "/"));
  });
  // A wildcard is a producer declaration, not permission to choose an
  // arbitrary archive. Exactly one staged archive must satisfy it.
  return matches.length === 1 ? matches[0] : null;
}

const archivePattern = /\.(?:tgz|tar\.gz|crate)$/i;
const maxArchiveBytes = Number(process.env.SDK_MAX_PACKAGE_ARCHIVE_BYTES ?? 256 * 1024 * 1024);
const maxExpandedArchiveBytes = Number(process.env.SDK_MAX_PACKAGE_EXPANDED_BYTES ?? 1024 * 1024 * 1024);

function within(root, candidate) {
  const base = resolve(root);
  const path = resolve(candidate);
  return path === base || path.startsWith(`${base}${sep}`);
}

function sourcePackagePath(path) {
  const normalized = path.replaceAll("\\", "/").toLowerCase();
  return normalized.includes("/rust/crates/") || normalized.includes("/typescript/packages/");
}

function findPackageArtifact(root, projection) {
  // Qualification may consume only a produced immutable package archive.
  // Source manifests remain useful metadata, but they are never install inputs.
  const declared = projection.package_artifact_path ?? projection.package_artifact ?? projection.artifact_path;
  if (typeof declared !== "string" || !archivePattern.test(declared)) return null;
  const resolvedArtifact = artifact(root, declared);
  if (!resolvedArtifact || sourcePackagePath(resolvedArtifact)) return null;
  return resolvedArtifact;
}

function normalizedArchivePath(path) {
  const normalized = path.replaceAll("\\", "/").replace(/^\.\//, "");
  if (!normalized || normalized.startsWith("/") || /^[A-Za-z]:\//.test(normalized)) {
    throw new Error(`archive member has an absolute path: ${path}`);
  }
  const parts = normalized.split("/").filter(Boolean);
  if (parts.some((part) => part === "..")) throw new Error(`archive member escapes extraction root: ${path}`);
  return parts.join("/");
}

function archiveRootPrefix(paths) {
  const first = paths[0]?.split("/")[0];
  if (!first || paths.some((path) => path === first || !path.startsWith(`${first}/`))) return "";
  return `${first}/`;
}

function archiveTreeDigest(files) {
  const digest = createHash("sha256");
  for (const file of [...files].sort((left, right) => left.path.localeCompare(right.path))) {
    digest.update(file.path);
    digest.update(Buffer.from([0]));
    digest.update(file.body);
    digest.update(Buffer.from([0]));
  }
  return `sha256:${digest.digest("hex")}`;
}

function extractPackageArchive(packageArtifact, extractionRoot) {
  rmSync(extractionRoot, { recursive: true, force: true });
  mkdirSync(extractionRoot, { recursive: true });
  const { compressed, expanded } = readBoundedGzip(packageArtifact, maxArchiveBytes, maxExpandedArchiveBytes);
  const entries = tarEntries(expanded);
  const regularEntries = entries
    .filter((entry) => entry.type === "0" || entry.type === "\0")
    .map((entry) => ({ ...entry, path: normalizedArchivePath(entry.path) }));
  if (!regularEntries.length) throw new Error("package archive contains no regular files");
  const prefix = archiveRootPrefix(regularEntries.map((entry) => entry.path));
  const files = [];
  const seen = new Set();
  for (const entry of regularEntries) {
    const path = prefix ? entry.path.slice(prefix.length) : entry.path;
    if (!path || seen.has(path)) throw new Error(`package archive contains a duplicate member: ${path}`);
    seen.add(path);
    const target = resolve(extractionRoot, path);
    if (!within(extractionRoot, target)) throw new Error(`package archive member escapes extraction root: ${entry.path}`);
    mkdirSync(dirname(target), { recursive: true });
    writeFileSync(target, entry.body, { flag: "wx" });
    files.push({ path, body: entry.body });
  }
  for (const entry of entries) {
    if (entry.type !== "0" && entry.type !== "\0" && entry.type !== "5") {
      throw new Error(`package archive contains unsupported member type ${JSON.stringify(entry.type)}`);
    }
  }
  return {
    archiveSha256: `sha256:${createHash("sha256").update(compressed).digest("hex")}`,
    treeSha256: archiveTreeDigest(files),
    files,
    extractionRoot,
  };
}

function packageJsonPath(root) {
  return allFiles(root).find((path) => path.toLowerCase().endsWith(`${sep}package.json`) || path.toLowerCase().endsWith("/package.json")) ?? null;
}

function rustPackagePath(root, packageName) {
  const manifests = allFiles(root).filter((path) => path.endsWith(`${sep}Cargo.toml`) || path.endsWith("/Cargo.toml"));
  const matches = manifests.filter((path) => readFileSync(path, "utf8").match(/^name\s*=\s*"([^"]+)"/m)?.[1] === packageName);
  if (matches.length !== 1) throw new Error(`package archive must contain exactly one Cargo package named ${packageName}`);
  return dirname(matches[0]);
}

function validateRustArchivePaths(root) {
  for (const manifest of allFiles(root).filter((path) => path.endsWith(`${sep}Cargo.toml`) || path.endsWith("/Cargo.toml"))) {
    const source = readFileSync(manifest, "utf8");
    for (const match of source.matchAll(/path\s*=\s*"([^"]+)"/g)) {
      const dependency = resolve(dirname(manifest), match[1]);
      if (!within(root, dependency) || !existsSync(join(dependency, "Cargo.toml"))) {
        throw new Error(`Rust package archive has an external path dependency: ${match[1]}`);
      }
    }
  }
}

function rustArchiveManifest(root) {
  const manifests = allFiles(root).filter((path) => path.endsWith(`${sep}Cargo.toml`) || path.endsWith("/Cargo.toml"));
  const rootManifest = manifests.find((manifest) => dirname(manifest) === resolve(root));
  if (!rootManifest) throw new Error("Rust package archive has no top-level Cargo.toml");
  const source = readFileSync(rootManifest, "utf8");
  const name = source.match(/^name\s*=\s*"([^"]+)"/m)?.[1];
  const version = source.match(/^version\s*=\s*"([^"]+)"/m)?.[1];
  if (!name) throw new Error("Rust package archive top-level Cargo.toml has no package name");
  if (!version) throw new Error(`Rust package archive ${name} has no resolved version`);
  return { manifest: rootManifest, name, version, root: dirname(rootManifest) };
}

function writeCargoDirectoryChecksum(root, extracted) {
  const files = {};
  for (const path of allFiles(root)) {
    if (path.endsWith(`${sep}.cargo-checksum.json`) || path.endsWith("/.cargo-checksum.json")) continue;
    const relativePath = relative(root, path).replaceAll("\\", "/");
    files[relativePath] = createHash("sha256").update(readFileSync(path)).digest("hex");
  }
  writeFileSync(join(root, ".cargo-checksum.json"), JSON.stringify({ files, package: extracted.archiveSha256.slice("sha256:".length) }) + "\n");
}

function ensureRustVendor() {
  if (rustVendorState) return rustVendorState;
  const archiveDirectory = join(repo, "qualification", "packages");
  if (!existsSync(archiveDirectory)) throw new Error(`Rust package closure archive directory is absent: ${archiveDirectory}`);
  const vendorRoot = join(output, "installed-rust-vendor");
  rmSync(vendorRoot, { recursive: true, force: true });
  mkdirSync(vendorRoot, { recursive: true });
  const seeded = command(cargo, ["vendor", "--locked", "--offline", "--manifest-path", join(repo, "Cargo.toml"), vendorRoot], repo, { CARGO_TARGET_DIR: cargoTargetDir });
  if (seeded.exitCode !== 0) throw new Error(`cargo vendor failed: ${seeded.stderr || seeded.stdout}`);
  const archiveStage = join(output, "installed-rust-archives");
  rmSync(archiveStage, { recursive: true, force: true });
  mkdirSync(archiveStage, { recursive: true });
  const archives = readdirSync(archiveDirectory, { withFileTypes: true })
    .filter((entry) => entry.isFile() && entry.name.endsWith(".crate"))
    .map((entry) => join(archiveDirectory, entry.name));
  const packages = new Map();
  for (const archive of archives) {
    const extractionRoot = join(archiveStage, archive.replaceAll("\\", "/").split("/").at(-1).slice(0, -6));
    const extracted = extractPackageArchive(archive, extractionRoot);
    validateRustArchivePaths(extractionRoot);
    const manifest = rustArchiveManifest(extractionRoot);
    if (packages.has(manifest.name)) throw new Error(`Rust package closure contains multiple archives for ${manifest.name}`);
    packages.set(manifest.name, { archive, extracted, manifest });
  }
  const missing = rustGuidePackageClosure.filter((name) => !packages.has(name));
  if (missing.length) throw new Error(`Rust package closure archives are missing: ${missing.join(", ")}`);
  for (const name of rustGuidePackageClosure) {
    const packageRecord = packages.get(name);
    const destination = join(vendorRoot, `${name}-${packageRecord.manifest.version}`);
    rmSync(destination, { recursive: true, force: true });
    cpSync(packageRecord.manifest.root, destination, { recursive: true });
    writeCargoDirectoryChecksum(destination, packageRecord.extracted);
  }
  rustVendorState = { vendorRoot, overlaidNames: new Set(rustGuidePackageClosure) };
  return rustVendorState;
}

function writeRustVendorConfig(directory, vendorRoot) {
  const configDirectory = join(directory, ".cargo");
  mkdirSync(configDirectory, { recursive: true });
  const quotedRoot = vendorRoot.replaceAll("\\", "/").replaceAll('"', '\\"');
  writeFileSync(join(configDirectory, "config.toml"), `[source.crates-io]\nreplace-with = "guide-vendor"\n\n[source.guide-vendor]\ndirectory = "${quotedRoot}"\n\n[net]\noffline = true\n`);
}

function extension(language) {
  return ({ rust: ".rs", python: ".py", typescript: ".ts", go: ".go", java: ".java", csharp: ".cs", ruby: ".rb", dart: ".dart", php: ".php" })[language];
}

function compile(language, file, cwd, packageArtifact, environment = {}) {
  switch (language) {
    case "rust": {
      // Compile the snippet as a consumer crate against the installed package
      // artifact so API drift is caught.
      const source = readFileSync(file, "utf8");
      mkdirSync(join(cwd, "src"), { recursive: true });
      writeFileSync(join(cwd, "src", "main.rs"), `#[tokio::main]\nasync fn main() -> Result<(), Box<dyn std::error::Error>> {\n${source}\nOk(())\n}\n`);
      return command(cargo, ["check", "--offline", "--manifest-path", join(cwd, "Cargo.toml")], cwd, environment);
    }
    case "python": return command(environment.PYTHON_BIN ?? binaries.python, ["-m", "py_compile", file], cwd, environment);
    case "typescript": return command(binaries.tsc, ["--ignoreConfig", "--types", "node", "--noEmit", "--strict", "--module", "NodeNext", "--moduleResolution", "NodeNext", "--target", "ES2022", file], cwd, environment);
    case "go": return command(binaries.go, ["test", "."], cwd, environment);
    case "java": {
      const args = ["--offline", "--batch-mode", "-q"];
      if (environment.MAVEN_REPO_LOCAL) args.push(`-Dmaven.repo.local=${environment.MAVEN_REPO_LOCAL}`);
      args.push("-DskipTests", "package");
      return command(binaries.maven, args, cwd, environment);
    }
    case "csharp": return command(binaries.dotnet, ["build", join(cwd, "GuideSnippet.csproj"), "--nologo", "--verbosity", "quiet"], cwd, environment);
    case "ruby": return command(binaries.ruby, ["-c", file], cwd, environment);
    case "dart": return command(binaries.dart, ["analyze", file], cwd, environment);
    case "php": return command(binaries.php, ["-l", file], cwd, environment);
    default: return { command: "", exitCode: 127, stdout: "", stderr: `unsupported language ${language}` };
  }
}

function execute(language, file, cwd, environment = {}) {
  switch (language) {
    case "python": return command(environment.PYTHON_BIN ?? binaries.python, [file], cwd, environment);
    case "typescript": return command(binaries.bun, [file], cwd, environment);
    case "go": return command(binaries.go, ["run", "."], cwd, environment);
    case "ruby": return command(binaries.ruby, [file], cwd, environment);
    case "dart": return command(binaries.dart, ["run", file], cwd, environment);
    case "php": return command(binaries.php, [file], cwd, environment);
    case "java": {
      const classpathFile = join(cwd, "runtime-classpath.txt");
      const classpath = existsSync(classpathFile) ? readFileSync(classpathFile, "utf8").trim() : "";
      const targetClasses = join(cwd, "target", "classes");
      return command(binaries.java, ["-cp", [targetClasses, classpath].filter(Boolean).join(delimiter), "GuideSnippet"], cwd, environment);
    }
    case "csharp": return command(binaries.dotnet, ["run", "--project", join(cwd, "GuideSnippet.csproj"), "--no-build"], cwd, environment);
    case "rust": return command(cargo, ["run", "--offline", "--manifest-path", join(cwd, "Cargo.toml")], cwd, environment);
    default: return { command: "", exitCode: 125, stdout: "", stderr: `execution is release-only for ${language}` };
  }
}

function prepare(language, packageArtifact, directory, projection) {
  if (!packageArtifact) return { status: "artifact-missing", install: null, environment: {} };

  if (language === "rust" && archivePattern.test(packageArtifact)) {
    const packageName = typeof projection.package_name === "string" ? projection.package_name.trim() : "";
    if (!packageName) return { status: "install-failed", install: null, environment: {}, error: "Rust package archive has no Cargo package name" };
    const packageInstall = join(output, "installed-rust-packages", packageName);
    let extracted;
    try {
      extracted = extractPackageArchive(packageArtifact, packageInstall);
      validateRustArchivePaths(packageInstall);
    } catch (error) {
      return { status: "install-failed", install: null, environment: {}, error: String(error.message ?? error) };
    }
    let vendor;
    try {
      vendor = ensureRustVendor();
      writeRustVendorConfig(directory, vendor.vendorRoot);
    } catch (error) {
      return { status: "install-failed", install: null, environment: {}, packageSha256: extracted.archiveSha256, packageTreeSha256: extracted.treeSha256, archiveFormat: "gzip+ustar", error: String(error.message ?? error) };
    }
    const installedRoot = rustPackagePath(packageInstall, packageName);
    writeFileSync(join(directory, "Cargo.toml"), `[package]\nname = "guide_snippet"\nversion = "0.0.0"\nedition = "2024"\n\n[workspace]\n\n[dependencies]\n${packageName} = { package = "${packageName}", path = "${installedRoot.replaceAll("\\", "/")}" }\nbytes = "1.10.1"\nprost = "0.14.4"\nsha2 = "0.10.9"\ntokio = { version = "1.48.0", features = ["macros", "rt", "rt-multi-thread", "time", "sync"] }\n`);
    const environment = { CARGO_TARGET_DIR: cargoTargetDir };
    const metadata = command(cargo, ["metadata", "--offline", "--manifest-path", join(directory, "Cargo.toml"), "--format-version", "1"], directory, environment);
    if (metadata.exitCode !== 0) {
      return { status: "install-failed", install: metadata, environment, packageSha256: extracted.archiveSha256, packageTreeSha256: extracted.treeSha256, archiveFormat: "gzip+ustar", resolvedPackageRoot: relative(repo, installedRoot).replaceAll("\\", "/") };
    }
    try {
      const metadataJson = JSON.parse(metadata.stdout);
      const localPackages = metadataJson.packages ?? [];
      const expectedManifest = resolve(installedRoot, "Cargo.toml");
      const resolved = localPackages.find((entry) => entry.name === packageName);
      if (!resolved || resolve(resolved.manifest_path) !== expectedManifest) throw new Error("Cargo metadata resolved a different package manifest");
      for (const entry of localPackages) {
        const manifest = resolve(entry.manifest_path);
        if (entry.source == null && !within(packageInstall, manifest) && !within(directory, manifest) && !within(vendor.vendorRoot, manifest)) throw new Error(`Cargo metadata resolved checkout path ${manifest}`);
        if (vendor.overlaidNames.has(entry.name) && entry.name !== packageName && !within(vendor.vendorRoot, manifest)) throw new Error(`Cargo metadata resolved closure package ${entry.name} outside the immutable vendor tree`);
      }
    } catch (error) {
      return { status: "install-failed", install: { ...metadata, stderr: `${metadata.stderr}${error.message ?? error}` }, environment, packageSha256: extracted.archiveSha256, packageTreeSha256: extracted.treeSha256, archiveFormat: "gzip+ustar", resolvedPackageRoot: relative(repo, installedRoot).replaceAll("\\", "/") };
    }
    return {
      status: "installed",
      packageArtifact,
      packageSha256: extracted.archiveSha256,
      packageTreeSha256: extracted.treeSha256,
      archiveFormat: "gzip+ustar",
      resolvedPackageRoot: relative(repo, installedRoot).replaceAll("\\", "/"),
      install: { ...metadata, command: `archive extract ${packageArtifact} && cargo vendor overlay && ${metadata.command}`, stdout: `extracted ${extracted.files.length} immutable package files; vendored ${vendor.overlaidNames.size} Rust closure archives\n${metadata.stdout}`, stderr: metadata.stderr },
      environment,
    };
  }

  if (language === "python" && packageArtifact.endsWith(".whl")) {
    const site = join(directory, "site");
    mkdirSync(site, { recursive: true });
    const install = command(binaries.python, ["-m", "pip", "install", "--no-deps", "--no-cache-dir", "--target", site, packageArtifact], directory);
    return {
      status: install.exitCode === 0 ? "installed" : "install-failed",
      install,
      environment: { PYTHON_BIN: binaries.python, PYTHONPATH: [site, process.env.PYTHONPATH].filter(Boolean).join(delimiter) },
    };
  }

  if (language === "go") {
    const sourceModuleRoot = resolve(packageArtifact, "..");
    // A consumer must resolve the produced package tree, never the moving
    // checkout. Stage the module artifact into this qualification's isolated
    // install directory before the module download and test.
    const moduleRoot = join(directory, "installed-module");
    cpSync(sourceModuleRoot, moduleRoot, { recursive: true, filter: (path) => !path.includes(`${String.fromCharCode(92)}.git${String.fromCharCode(92)}`) && !path.includes("/target/") });
    const module = readFileSync(packageArtifact, "utf8").match(/^module\s+([^\r\n]+)/m)?.[1]?.trim();
    if (!module) return { status: "install-failed", install: null, environment: {}, error: "go.mod has no module declaration" };
    const packageGoMod = readFileSync(packageArtifact, "utf8").replace(/^module\s+[^\r\n]+\r?\n?/m, "");
    writeFileSync(join(directory, "go.mod"), `module guide-snippet\n\n${packageGoMod}\nrequire ${module} v0.0.0\n\nreplace ${module} => ${moduleRoot.replaceAll("\\", "/")}\n`);
    const packageGoSum = join(moduleRoot, "go.sum");
    if (existsSync(packageGoSum)) writeFileSync(join(directory, "go.sum"), readFileSync(packageGoSum));
    const install = command(binaries.go, ["mod", "download", "all"], directory, { GOPROXY: "off", GOTOOLCHAIN: "local" });
    return { status: install.exitCode === 0 ? "installed" : "install-failed", install, environment: { GOPROXY: "off", GOTOOLCHAIN: "local" } };
  }

  if (language === "java") {
    mkdirSync(join(directory, "src", "main", "java"), { recursive: true });
    cpSync(join(directory, "GuideSnippet.java"), join(directory, "src", "main", "java", "GuideSnippet.java"));
    const jarName = packageArtifact.replaceAll("\\", "/").split("/").at(-1) ?? "";
    const version = jarName.match(/acyclic-sdk-jvm-transport-(.+)\.jar$/)?.[1] ?? "0.2.0-SNAPSHOT";
    const localRepo = process.env.SDK_MAVEN_REPO ?? join(repo, ".tmp-jvm-producer-smoke", ".m2");
    const installArgs = ["--offline", "--batch-mode", "-q", "org.apache.maven.plugins:maven-install-plugin:3.1.2:install-file", `-Dfile=${packageArtifact}`, "-DgroupId=dev.acyclic", "-DartifactId=acyclic-sdk-jvm-transport", `-Dversion=${version}`, "-Dpackaging=jar", `-Dmaven.repo.local=${localRepo}`];
    const installed = command(binaries.maven, installArgs, directory);
    if (installed.exitCode !== 0) return { status: "install-failed", install: installed, environment: {} };
    writeFileSync(join(directory, "pom.xml"), `<?xml version="1.0" encoding="UTF-8"?><project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion><groupId>guide</groupId><artifactId>guide-snippet</artifactId><version>0.0.0</version><properties><maven.compiler.release>17</maven.compiler.release><project.build.sourceEncoding>UTF-8</project.build.sourceEncoding></properties><dependencies><dependency><groupId>dev.acyclic</groupId><artifactId>acyclic-sdk-jvm-transport</artifactId><version>${version}</version></dependency><dependency><groupId>io.grpc</groupId><artifactId>grpc-stub</artifactId><version>1.75.0</version></dependency><dependency><groupId>io.grpc</groupId><artifactId>grpc-protobuf</artifactId><version>1.75.0</version></dependency><dependency><groupId>io.grpc</groupId><artifactId>grpc-netty-shaded</artifactId><version>1.75.0</version></dependency><dependency><groupId>com.google.protobuf</groupId><artifactId>protobuf-java</artifactId><version>4.31.1</version></dependency></dependencies><build><plugins><plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-dependency-plugin</artifactId><version>3.7.0</version></plugin></plugins></build></project>`);
    const mavenArgs = ["--offline", "--batch-mode", "-q", "-Dmaven.repo.local=" + localRepo, "org.apache.maven.plugins:maven-dependency-plugin:3.7.0:build-classpath", "-Dmdep.outputFile=runtime-classpath.txt"];
    const classpath = command(binaries.maven, mavenArgs, directory);
    if (classpath.exitCode !== 0) return { status: "install-failed", install: classpath, environment: {} };
    return { status: "installed", install: { ...classpath, command: `${installed.command} && ${classpath.command}` }, environment: { MAVEN_REPO_LOCAL: localRepo } };
  }

  if (language === "typescript" && archivePattern.test(packageArtifact)) {
    const packageInstall = join(output, "installed-ts-packages");
    let extracted;
    try {
      extracted = extractPackageArchive(packageArtifact, packageInstall);
    } catch (error) {
      return { status: "install-failed", install: null, environment: {}, error: String(error.message ?? error) };
    }
    const archiveFields = {
      packageArtifact,
      packageSha256: extracted.archiveSha256,
      packageTreeSha256: extracted.treeSha256,
      archiveFormat: "gzip+ustar",
      resolvedPackageRoot: relative(repo, packageInstall).replaceAll("\\", "/"),
    };
    const packageJsonFile = packageJsonPath(packageInstall);
    if (!packageJsonFile) return { status: "install-failed", ...archiveFields, install: null, environment: {}, error: "TypeScript package archive has no package.json" };
    let packageJson;
    try {
      packageJson = JSON.parse(readFileSync(packageJsonFile, "utf8"));
    } catch (error) {
      return { status: "install-failed", ...archiveFields, install: null, environment: {}, error: `TypeScript package package.json is invalid: ${error.message ?? error}` };
    }
    if (typeof packageJson.name !== "string" || !packageJson.name) return { status: "install-failed", ...archiveFields, install: null, environment: {}, error: "TypeScript package package.json has no name" };
    if (projection.package_name && projection.package_name !== packageJson.name) return { status: "install-failed", ...archiveFields, install: null, environment: {}, error: `TypeScript package name ${packageJson.name} does not match projection ${projection.package_name}` };
    rmSync(join(directory, "node_modules"), { recursive: true, force: true });
    rmSync(join(directory, "bun.lock"), { force: true });
    writeFileSync(join(directory, "package.json"), JSON.stringify({
      name: "guide-snippet",
      private: true,
      type: "module",
      dependencies: {
        [packageJson.name]: `file:${relative(directory, packageArtifact).replaceAll("\\", "/")}`,
        "@connectrpc/connect": packageJson.dependencies?.["@connectrpc/connect"] ?? "2.1.1",
        "@connectrpc/connect-node": "2.1.1",
      },
      devDependencies: {
        "@types/node": "26.4.1",
      },
    }, null, 2));
    const install = command(binaries.bun, ["install", "--offline", "--no-progress"], directory);
    if (install.exitCode !== 0) return {
      status: "install-failed",
      ...archiveFields,
      install,
      environment: {},
    };
    const packageNodeRoot = join(directory, "node_modules", ...packageJson.name.split("/"));
    if (!existsSync(join(packageNodeRoot, "package.json")) || statSync(packageNodeRoot).isSymbolicLink()) return { status: "install-failed", ...archiveFields, install, environment: {}, error: "Bun did not install the TypeScript archive into node_modules" };
    const lock = join(directory, "bun.lock");
    if (existsSync(lock) && /typescript[\\/]packages[\\/]/i.test(readFileSync(lock, "utf8"))) return { status: "install-failed", ...archiveFields, install, environment: {}, error: "Bun lockfile resolves a TypeScript checkout path" };
    return { status: "installed", packageArtifact, packageSha256: extracted.archiveSha256, packageTreeSha256: extracted.treeSha256, archiveFormat: "gzip+ustar", resolvedPackageRoot: relative(repo, packageNodeRoot).replaceAll("\\", "/"), install, environment: { PATH: process.env.PATH } };
  }

  if (language === "csharp") {
    const packageVersion = packageArtifact.match(/Acyclic\.Sdk\.Transport\.([0-9A-Za-z.-]+)\.nupkg$/i)?.[1] ?? "0.2.0-alpha.1";
    const feed = join(directory, "nuget-feed");
    mkdirSync(feed, { recursive: true });
    cpSync(packageArtifact, join(feed, packageArtifact.split(/[\\/]/).at(-1)));
    const dependencyFeed = join(repo, ".tmp-dotnet-producer-smoke", ".nuget");
    const nugetConfig = join(directory, "NuGet.Config");
    writeFileSync(nugetConfig, `<configuration><packageSources><clear /><add key="sdk" value="${feed.replaceAll("\\", "/")}" /><add key="dependencies" value="${dependencyFeed.replaceAll("\\", "/")}" /></packageSources></configuration>\n`);
    const project = join(directory, "GuideSnippet.csproj");
    writeFileSync(project, `<Project Sdk="Microsoft.NET.Sdk"><PropertyGroup><OutputType>Exe</OutputType><TargetFramework>net8.0</TargetFramework><ImplicitUsings>enable</ImplicitUsings><Nullable>enable</Nullable></PropertyGroup><ItemGroup><PackageReference Include="Acyclic.Sdk.Transport" Version="${packageVersion}" /></ItemGroup></Project>\n`);
    const install = command(binaries.dotnet, ["restore", project, "--configfile", nugetConfig, "--nologo", "--force-evaluate"], directory, { NUGET_PACKAGES: dependencyFeed });
    return { status: install.exitCode === 0 ? "installed" : "install-failed", install, environment: { NUGET_PACKAGES: dependencyFeed } };
  }

  if (language === "dart") {
    const packageRoot = resolve(packageArtifact, "..");
    writeFileSync(join(directory, "pubspec.yaml"), `name: guide_snippet\nenvironment:\n  sdk: ">=3.8.0 <4.0.0"\ndependencies:\n  acyclic_sdk:\n    path: ${packageRoot.replaceAll("\\", "/")}\n`);
    const install = command(binaries.dart, ["pub", "get", "--offline"], directory);
    return { status: install.exitCode === 0 ? "installed" : "install-failed", install, environment: {} };
  }

  if (language === "php") {
    const packageRoot = resolve(packageArtifact, "..");
    writeFileSync(join(directory, "composer.json"), JSON.stringify({
      require: { "acyclic/sdk": "*" },
      "minimum-stability": "dev",
      "prefer-stable": true,
      repositories: [{ type: "path", url: packageRoot.replaceAll("\\", "/"), options: { symlink: false } }],
    }, null, 2));
    const composerArgs = process.env.SDK_COMPOSER_PHAR
      ? [process.env.SDK_COMPOSER_PHAR, "install", "--no-interaction", "--no-progress"]
      : ["install", "--no-interaction", "--no-progress"];
    const install = command(binaries.composer, composerArgs, directory);
    const ini = join(directory, "runtime.ini");
    const grpcExtension = process.env.SDK_PHP_GRPC_EXTENSION ?? "";
    const protobufExtension = process.env.SDK_PHP_PROTOBUF_EXTENSION ?? "";
    const environment = existsSync(grpcExtension) && existsSync(protobufExtension)
      ? { PHPRC: ini }
      : {};
    if (environment.PHPRC) writeFileSync(ini, `extension=${grpcExtension}\nextension=${protobufExtension}\n`);
    return { status: install.exitCode === 0 ? "installed" : "install-failed", install, environment };
  }

  if (language === "ruby" && packageArtifact.endsWith(".gemspec")) {
    const gemPath = join(directory, "acyclic-sdk.gem");
    const gemHome = join(directory, "vendor", "bundle");
    const built = command(binaries.gem, ["build", packageArtifact, "--output", gemPath], directory);
    if (built.exitCode !== 0) return { status: "install-failed", install: built, environment: {} };
    const rubyGemPath = [process.env.SDK_RUBY_GEM_PATH, gemHome, process.env.GEM_PATH].filter(Boolean).join(delimiter);
    const rubyEnv = { GEM_HOME: gemHome, ...(rubyGemPath ? { GEM_PATH: rubyGemPath } : {}) };
    const dependencyCache = process.env.SDK_RUBY_GEM_PATH ? join(process.env.SDK_RUBY_GEM_PATH, "cache") : null;
    const dependencyNames = ["google-protobuf-4.33.0", "grpc-1.82.0", "googleapis-common-protos-types-1.23.0"];
    const dependencyInstalls = [];
    if (dependencyCache && existsSync(dependencyCache)) {
      for (const dependencyName of dependencyNames) {
        const dependencyGem = readdirSync(dependencyCache).find((entry) => entry.startsWith(dependencyName) && entry.endsWith(".gem"));
        if (!dependencyGem) continue;
        const dependencyInstall = command(binaries.gem, ["install", "--local", join(dependencyCache, dependencyGem), "--install-dir", gemHome, "--no-document", "--ignore-dependencies"], directory, rubyEnv);
        dependencyInstalls.push(dependencyInstall);
        if (dependencyInstall.exitCode !== 0) return { status: "install-failed", install: dependencyInstall, environment: {} };
      }
    }
    const install = command(binaries.gem, ["install", "--local", gemPath, "--install-dir", gemHome, "--no-document", "--ignore-dependencies"], directory, rubyEnv);
    return {
      status: install.exitCode === 0 ? "installed" : "install-failed",
      install: { ...install, command: [built.command, ...dependencyInstalls.map((dependency) => dependency.command), install.command].join(" && ") },
      environment: { ...rubyEnv },
    };
  }

  return {
    status: "install-failed",
    install: { command: "unsupported package installation", exitCode: 127, stdout: "", stderr: `no package installer for ${language}` },
    environment: {},
    error: `no package installer for ${language}`,
  };
}

const projectionInput = args.get("--projections");
const manifestCommand = projectionInput
  ? { exitCode: 0, stdout: readFileSync(resolve(repo, projectionInput), "utf8"), stderr: "", command: `file:${projectionInput}` }
  : command(cargo, ["run", "--locked", "--offline", "--manifest-path", join(repo, "rust", "crates", "sdk-examples", "Cargo.toml"), "--example", "guide-projections"]);
if (manifestCommand.exitCode !== 0) {
  console.error(manifestCommand.stderr);
  process.exit(manifestCommand.exitCode);
}
const parsedProjections = JSON.parse(manifestCommand.stdout);
const allProjections = Array.isArray(parsedProjections) ? parsedProjections : [parsedProjections];
const languageFilter = args.get("--language");
const scenarioFilter = args.get("--scenario");
const projections = allProjections.filter((projection) =>
  (!languageFilter || projection.language === languageFilter) &&
  (!scenarioFilter || projection.scenario_id === scenarioFilter),
);
const sourceDigests = [...new Set(projections.map((projection) => projection.source_sha256).filter(Boolean))];
const sourceSha256 = sourceDigests.length === 1 ? sourceDigests[0] : null;
const sourceGitRevisions = [...new Set(projections.map((projection) => projection.source_git_revision).filter(Boolean))];
const sourceGitRevision = sourceGitRevisions.length === 1 ? sourceGitRevisions[0] : null;
// Rust emits the source closure digest in every projection. It is the
// authoritative identity for an archive or dirty checkout; Git HEAD is not
// sufficient because it can describe a different tree than the producer.
const sourceRevision = sourceSha256
  ? `source-sha256:${sourceSha256.replace(/^sha256:/, "")}`
  : null;
const receipts = [];
const fixture = args.has("--execute") && !process.env.FIXTURE_GRPC_ADDRESS ? await startFixture() : null;
if (args.has("--execute") && !process.env.FIXTURE_GRPC_ADDRESS && !fixture) {
  console.error("--execute requires FIXTURE_GRPC_ADDRESS or a built sdk-examples fixture-server binary");
}
if (fixture) process.on("exit", () => fixture.child.kill());
for (const projection of projections) {
  const directory = join(snippets, projection.scenario_id, projection.language);
  mkdirSync(directory, { recursive: true });
  const fileName = ["java", "csharp"].includes(projection.language)
    ? `GuideSnippet${extension(projection.language)}`
    : `${projection.scenario_id}${extension(projection.language)}`;
  const file = join(directory, fileName);
  writeFileSync(file, projection.code);
  const packageArtifact = findPackageArtifact(repo, projection);
  const receipt = {
    scenario_id: projection.scenario_id,
    family: projection.family,
    operation: projection.operation,
    language: projection.language,
    mode: projection.mode,
    source: projection.source,
    source_revision: sourceRevision,
    source_git_revision: sourceGitRevision,
    source_sha256: projection.source_sha256 ?? null,
    package_manager: projection.package_manager,
    package_name: projection.package_name,
    artifact_path: projection.artifact_path,
    package_artifact: packageArtifact ? relative(repo, packageArtifact).replaceAll("\\", "/") : null,
    package_sha256: packageArtifact ? `sha256:${createHash("sha256").update(readFileSync(packageArtifact)).digest("hex")}` : null,
    snippet_path: relative(repo, file).replaceAll("\\", "/"),
    snippet_sha256: createHash("sha256").update(projection.code, "utf8").digest("hex"),
    qualification: projection.qualification ?? null,
    package_archive_sha256: null,
    package_tree_sha256: null,
    package_archive_format: null,
    resolved_package_root: null,
  };
  const recipe = projection.qualification;
  if (!recipe || !recipe.install || !recipe.compile || !recipe.execute) {
    receipt.status = "install-failed";
    receipt.install = { command: "Rust qualification recipe missing", exitCode: 127, stdout: "", stderr: "projection did not carry a Rust-owned install/compile/execute recipe" };
  } else if (!packageArtifact) {
    receipt.status = "artifact-missing";
  } else {
    const prepared = prepare(projection.language, packageArtifact, directory, projection);
    if (prepared.packageArtifact && existsSync(prepared.packageArtifact)) {
      receipt.package_artifact = relative(repo, prepared.packageArtifact).replaceAll("\\", "/");
      receipt.package_sha256 = prepared.packageSha256 ?? `sha256:${createHash("sha256").update(readFileSync(prepared.packageArtifact)).digest("hex")}`;
    }
    receipt.package_archive_sha256 = prepared.packageSha256 ?? receipt.package_sha256;
    receipt.package_tree_sha256 = prepared.packageTreeSha256 ?? null;
    receipt.package_archive_format = prepared.archiveFormat ?? null;
    receipt.resolved_package_root = prepared.resolvedPackageRoot ?? null;
    receipt.install = prepared.install ?? (prepared.error ? {
      command: `archive validation ${packageArtifact}`,
      exitCode: 1,
      stdout: "",
      stderr: prepared.error,
    } : null);
    receipt.install_status = prepared.status;
    if (prepared.status !== "installed") {
      receipt.status = prepared.status;
    } else {
      const checked = compile(projection.language, file, directory, packageArtifact, prepared.environment);
      receipt.compile = checked;
      receipt.status = checked.exitCode === 0 ? "compiled" : "compile-failed";
    }
    if (receipt.status === "compiled" && args.has("--execute")) {
      const executionEnvironment = {
        ...(fixture ? fixtureEnvironment(projection.language, fixture.address) : {}),
        ...prepared.environment,
      };
      const ran = execute(projection.language, file, directory, executionEnvironment);
      receipt.execution = ran;
      receipt.status = ran.exitCode === 0 ? "executed" : "execution-failed";
    }
  }
  writeFileSync(join(directory, "receipt.json"), `${JSON.stringify(receipt, null, 2)}\n`);
  receipts.push(receipt);
}

const summary = {
  schema: "acyclic.sdk.guide-projection-qualification.v1",
  source_revision: sourceRevision,
  source_git_revision: sourceGitRevision,
  source_sha256: sourceSha256,
  source: "rust/crates/sdk-examples/src/guide_projections.rs",
  projection_count: receipts.length,
  compiled: receipts.filter((receipt) => receipt.status === "compiled" || receipt.status === "executed").length,
  executed: receipts.filter((receipt) => receipt.status === "executed").length,
  installed: receipts.filter((receipt) => receipt.install_status === "installed").length,
  artifact_missing: receipts.filter((receipt) => receipt.status === "artifact-missing").length,
  install_failed: receipts.filter((receipt) => receipt.status === "install-failed").length,
  failed: receipts.filter((receipt) => receipt.status.endsWith("failed")).length,
  receipts,
};
const hasSuccessfulCommand = (value) => value && value.exitCode === 0 && typeof value.command === "string" && value.command.length > 0 && !/artifact present/i.test(value.command);
const everyReceiptHasEvidence = receipts.every((receipt) =>
  receipt.install_status === "installed" &&
  hasSuccessfulCommand(receipt.install) &&
  hasSuccessfulCommand(receipt.compile) &&
  (!args.has("--execute") || (receipt.status === "executed" && hasSuccessfulCommand(receipt.execution))),
);
summary.evidence_complete = everyReceiptHasEvidence;
const verification = verifyQualificationSummary(summary, {
  expectedProjections: projections,
  expectedProjectionCount,
  requireExecution: args.has("--execute"),
  readArtifact: (path) => readFileSync(resolve(repo, path)),
});
summary.verification = verification;
writeFileSync(join(output, "qualification.json"), `${JSON.stringify(summary, null, 2)}\n`);
console.log(JSON.stringify({ ...summary, receipts: undefined }, null, 2));
if (args.has("--strict") && (summary.artifact_missing > 0 || summary.failed > 0 || summary.projection_count !== expectedProjectionCount || !sourceRevision || !sourceSha256 || !everyReceiptHasEvidence || !verification.valid || projections.some((projection) => projection.source_sha256 !== sourceSha256))) process.exit(1);
