#!/usr/bin/env node

// Build, pack, and consume GraphCoder in one explicit argv-only qualification
// lane. The platform runner owns the output paths and records their bytes;
// this helper only performs the package operations required to create them.

import { createHash, randomUUID } from "node:crypto";
import { execFileSync, spawnSync } from "node:child_process";
import { copyFileSync, existsSync, mkdirSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { basename, dirname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { assertCanonicalParents, ensureOwnedDirectory, isWithin } from "./graphcoder-path-ownership.mjs";
import { workingTreeDigest } from "./graphcoder-source-fence.mjs";
import { describeArtifact } from "./graphcoder-artifact.mjs";
import {
  GRAPH_CODER_EXPORT_COUNT,
  GRAPH_CODER_EXPORTS,
  LAZY_LISTING_COUNTERS,
  NATIVE_PROCESS_OWNER_CAPABILITY,
  NATIVE_PROCESS_OWNER_VERSION,
  inspectInstalledPackage,
} from "./fixtures/graphcoder-qualification/package-contract.mjs";

const ROOT = resolve(fileURLToPath(new URL("..", import.meta.url)));
const PACKAGE_ROOT = join(ROOT, "typescript", "packages", "graphcoder");
const DIST_ROOT = join(PACKAGE_ROOT, "dist");
const FS_PACKAGE_ROOT = join(ROOT, "typescript", "packages", "filesystem");
const QUALIFICATION_ROOT = join(ROOT, "target", "graphcoder-package-qualification");
const NPM_EXECUTABLE = process.platform === "win32" ? process.execPath : "npm";
const NPM_PREFIX = process.platform === "win32"
  ? [join(dirname(process.execPath), "node_modules", "npm", "bin", "npm-cli.js")]
  : [];
const GENERATED_DIST = [
  "api.js", "api.d.ts", "bridge.js", "bridge.d.ts", "cli.js", "cli.d.ts",
  "dispatcher.js", "dispatcher.d.ts", "index.js", "index.d.ts", "mock.js", "mock.d.ts",
  "native-cli.js", "native-cli.d.ts", "node.js", "node.d.ts", "node-dispatcher.js",
  "node-dispatcher.d.ts", "process.js", "process.d.ts", "terminal.js", "terminal.d.ts",
];
const SECRET_ENVIRONMENT = /(?:TOKEN|PASSWORD|SECRET|CREDENTIAL|AUTH|PRIVATE_KEY|ACCESS_KEY)/iu;
const TOOLCHAIN_KEYS = new Set([
  "PATH", "PATHEXT", "SYSTEMROOT", "TEMP", "TMP",
  "CARGO_HOME", "CARGO_TARGET_DIR", "RUSTUP_HOME", "RUSTFLAGS", "RUSTDOCFLAGS",
  "CI", "NUMBER_OF_PROCESSORS", "PROCESSOR_ARCHITECTURE", "NPM_CONFIG_CACHE",
]);
const RUNTIME_KEYS = new Set([
  "PATH", "PATHEXT", "SYSTEMROOT", "TEMP", "TMP",
  "CI", "NUMBER_OF_PROCESSORS", "PROCESSOR_ARCHITECTURE",
]);

function fail(message) {
  throw new Error(`graphcoder-package-gate: ${message}`);
}

function sha256(path) {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}

function filteredEnvironment(environment = process.env, policy = "runtime", extra = {}) {
  const allow = policy === "toolchain" ? TOOLCHAIN_KEYS : RUNTIME_KEYS;
  const result = {};
  for (const [key, value] of Object.entries(environment)) {
    if (allow.has(key.toUpperCase()) && !SECRET_ENVIRONMENT.test(key) && typeof value === "string") result[key] = value;
  }
  const pathEntry = Object.entries(environment).find(([key]) => key.toLowerCase() === "path");
  if (pathEntry !== undefined) result[pathEntry[0]] = pathEntry[1];
  Object.assign(result, extra);
  return result;
}

function run(executable, args, cwd, environment = filteredEnvironment(process.env, "toolchain")) {
  const result = spawnSync(executable, args, {
    cwd,
    env: environment,
    encoding: "utf8",
    shell: false,
    windowsHide: true,
    stdio: ["ignore", "pipe", "pipe"],
    maxBuffer: 64 * 1024 * 1024,
  });
  if (result.error) fail(`${executable} failed to start: ${result.error.message}`);
  if (result.status !== 0 || result.signal !== null) {
    const diagnostics = `${result.stderr ?? ""}${result.stdout ?? ""}`.trim();
    fail(`${executable} ${args.join(" ")} failed with ${result.status ?? result.signal}: ${diagnostics}`);
  }
  return result.stdout ?? "";
}

function outputRoot(value) {
  if (value === undefined || value.trim() === "") fail("--output requires a path");
  const root = resolve(ROOT, value);
  if (!isWithin(QUALIFICATION_ROOT, root)) {
    fail(`output must remain under ${QUALIFICATION_ROOT}`);
  }
  try {
    return ensureOwnedDirectory(root, ROOT, "package qualification output");
  } catch (error) {
    fail(error instanceof Error ? error.message : String(error));
  }
}

function sourceIdentity() {
  const sourceCommit = execFileSync("git", ["-C", ROOT, "rev-parse", "HEAD"], { encoding: "utf8" }).trim();
  const sourceTree = execFileSync("git", ["-C", ROOT, "rev-parse", "HEAD^{tree}"], { encoding: "utf8" }).trim();
  return { source_commit: sourceCommit, source_tree: sourceTree };
}

function claimOutput(root) {
  const markerPath = join(root, ".graphcoder-package-source.json");
  const marker = { protocol: "acyclic.graphcoder.package-output-owner.v1", ...sourceIdentity() };
  if (existsSync(markerPath)) {
    let existing;
    try { existing = JSON.parse(readFileSync(markerPath, "utf8")); }
    catch (error) { fail(`package output ownership marker is corrupt: ${error instanceof Error ? error.message : String(error)}`); }
    if (existing.source_commit !== marker.source_commit || existing.source_tree !== marker.source_tree) fail("package output source identity changed; use a fresh output directory");
  } else {
    writeFileSync(markerPath, `${JSON.stringify(marker, null, 2)}\n`, { flag: "wx" });
  }
}

function packageVersion() {
  return JSON.parse(readFileSync(join(PACKAGE_ROOT, "package.json"), "utf8")).version;
}

function nativeCompanionPackage(output, npmEnvironment, attemptNonce, builtAt) {
  if (process.platform !== "win32") return undefined;
  const bindingPath = process.env.ACYCLIC_FS_NATIVE_BINDING;
  const producerReceipt = process.env.ACYCLIC_FS_NATIVE_BINDING_RECEIPT;
  if (typeof bindingPath !== "string" || bindingPath.trim() === "") {
    fail("ACYCLIC_FS_NATIVE_BINDING is required for the Windows installed consumer");
  }
  if (typeof producerReceipt !== "string" || producerReceipt.trim() === "") {
    fail("ACYCLIC_FS_NATIVE_BINDING_RECEIPT is required for the Windows installed consumer");
  }
  const binding = resolve(bindingPath);
  if (!binding.toLowerCase().endsWith(".node")) fail("native companion binding must be a .node artifact");
  const target = `win32-${process.arch}`;
  const expectedPackage = `@acyclic-labs/fs-${target}`;
  const expectedFile = `acyclic-fs-${packageVersion()}-${target}.node`;
  if (basename(binding) !== expectedFile) fail(`native companion binding filename must be ${expectedFile}`);
  const bindingArtifact = describeArtifact({
    path: binding,
    sourceCwd: ROOT,
    buildId: `graphcoder-native-companion-${attemptNonce}`,
    builtAt,
    producerReceipt,
    requiredProducerReceipt: true,
    requireCausalReceipt: true,
  });
  const producer = bindingArtifact.producer_receipt;
  if (producer.platform_target !== target || producer.architecture !== process.arch) {
    fail(`native companion producer receipt must bind ${target} architecture`);
  }
  const stageRoot = join(output, "native-companion-stage");
  assertCanonicalParents(ROOT, stageRoot, "native companion staging directory", false);
  if (existsSync(stageRoot)) assertCanonicalParents(ROOT, stageRoot, "native companion staging directory", true);
  rmSync(stageRoot, { recursive: true, force: true });
  mkdirSync(stageRoot, { recursive: true });
  const stagedBinding = join(stageRoot, expectedFile);
  copyFileSync(binding, stagedBinding, { errorOnExist: true });
  writeFileSync(join(stageRoot, "package.json"), `${JSON.stringify({
    name: expectedPackage,
    version: packageVersion(),
    description: "Platform native companion for @acyclic-labs/fs",
    type: "commonjs",
    main: `./${expectedFile}`,
    exports: { ".": `./${expectedFile}` },
    files: [expectedFile],
    os: ["win32"],
    cpu: [process.arch],
  }, null, 2)}\n`, { flag: "wx" });
  const packArgs = [...NPM_PREFIX, "pack", "--pack-destination", output];
  const invocationSha256 = createHash("sha256").update(JSON.stringify({
    executable: NPM_EXECUTABLE,
    args: packArgs,
    cwd: stageRoot,
    environment_policy: "toolchain",
    environment: Object.fromEntries(Object.entries(npmEnvironment).sort(([left], [right]) => left.localeCompare(right))),
    binding: bindingArtifact,
  })).digest("hex");
  const packedOutput = run(NPM_EXECUTABLE, packArgs, stageRoot, npmEnvironment).trim().split(/\r?\n/u).at(-1);
  if (!packedOutput) fail("npm pack did not report the native companion archive");
  const packedPath = resolve(output, packedOutput);
  if (packedPath !== output && !packedPath.startsWith(`${output}${sep}`)) fail(`native companion archive escapes output: ${packedOutput}`);
  if (!existsSync(packedPath)) fail(`native companion archive is missing: ${packedPath}`);
  assertCanonicalParents(ROOT, packedPath, "native companion archive", false);
  const archivePath = join(output, "acyclic-fs-native-package.tgz");
  assertCanonicalParents(ROOT, archivePath, "native companion package archive", false);
  renameSync(packedPath, archivePath);
  rmSync(stageRoot, { recursive: true, force: true });
  return { archivePath, bindingArtifact, invocationSha256, packageName: expectedPackage };
}

function verifyConsumer(consumerRoot, verificationPath, archivePath) {
  const packageRoot = join(consumerRoot, "node_modules", "@acyclic-labs", "graphcoder");
  const installedIdentity = inspectInstalledPackage(packageRoot, { artifactPath: archivePath });
  const graphCoderNames = GRAPH_CODER_EXPORTS
    .map(specifier => specifier === "@acyclic-labs/graphcoder" ? "." : `.${specifier.slice("@acyclic-labs/graphcoder".length)}`)
    .filter(name => name !== "./native-cli");
  const graphCoderExportNames = [...graphCoderNames, "./native-cli"];
  const filesystemNames = ["@acyclic-labs/fs", "@acyclic-labs/fs/native", "@acyclic-labs/fs/native-process"];
  const script = [
    "import fs from 'node:fs';",
    "import path from 'node:path';",
    "import { createRequire } from 'node:module';",
    `const names = ${JSON.stringify(graphCoderNames)};`,
    "const loaded = [];",
    "for (const name of names) await import('@acyclic-labs/graphcoder' + (name === '.' ? '' : name.slice(1))).then(() => loaded.push(name));",
    "const require = createRequire(import.meta.url); require.resolve('@acyclic-labs/graphcoder/native-cli'); loaded.push('./native-cli');",
    "const nodeEntry = await import('@acyclic-labs/graphcoder/node');",
    ...(process.platform === "win32" ? ["const nativeConnection = await nodeEntry.createNativeNodeGraphCoderConnection({ executable: process.execPath, args: ['-e', 'setTimeout(() => {}, 25)'], cwd: process.cwd(), env: {} }); if (typeof nativeConnection?.bridge?.close !== 'function') throw new Error('native GraphCoder connection did not expose a bridge'); nativeConnection.bridge.close('installed package native owner qualification');"] : []),
    "const packageJson = JSON.parse(fs.readFileSync(path.join(process.cwd(), 'node_modules', '@acyclic-labs', 'graphcoder', 'package.json')));",
    "if (packageJson.name !== '@acyclic-labs/graphcoder') throw new Error('installed package name mismatch');",
    "const fsPackageJson = JSON.parse(fs.readFileSync(path.join(process.cwd(), 'node_modules', '@acyclic-labs', 'fs', 'package.json')));",
    "if (fsPackageJson.name !== '@acyclic-labs/fs') throw new Error('installed filesystem companion name mismatch');",
    "await import('@acyclic-labs/fs');",
    `const fsNativeProcess = await import('@acyclic-labs/fs/native-process'); if (fsNativeProcess.NATIVE_PROCESS_OWNER_CAPABILITY !== ${JSON.stringify(NATIVE_PROCESS_OWNER_CAPABILITY)} || fsNativeProcess.NATIVE_PROCESS_OWNER_VERSION !== ${JSON.stringify(NATIVE_PROCESS_OWNER_VERSION)} || typeof fsNativeProcess.createNativeProcessOwner !== 'function') throw new Error('installed filesystem native-process owner contract is incomplete'); loaded.push(...${JSON.stringify(filesystemNames)});`,
    "const fsNative = await import('@acyclic-labs/fs/native');",
    ...(process.platform === "win32" ? ["const nativeProcessOwner = await fsNative.openNativeProcessOwner(); if (typeof nativeProcessOwner?.spawn !== 'function' || typeof nativeProcessOwner?.terminate !== 'function') throw new Error('native companion did not provide the process owner capability');"] : []),
    ...(process.platform === "win32" ? ["await import('@acyclic-labs/fs-win32-x64').then(() => loaded.push('@acyclic-labs/fs-win32-x64')); "] : []),
    `fs.writeFileSync(process.argv.at(-1), JSON.stringify({ package: packageJson.name, version: packageJson.version, loaded, expected_graphcoder_export_count: ${GRAPH_CODER_EXPORT_COUNT}, native_connection: process.platform === 'win32', native_process_owner: process.platform === 'win32' ? { capability: fsNativeProcess.NATIVE_PROCESS_OWNER_CAPABILITY, version: fsNativeProcess.NATIVE_PROCESS_OWNER_VERSION } : null, lazy_listing_counters: ${JSON.stringify(LAZY_LISTING_COUNTERS)}, companion: { package: fsPackageJson.name, version: fsPackageJson.version } }, null, 2) + '\\n');`,
  ].join(" ");
  run(process.execPath, ["--input-type=module", "-e", script, "--", "qualification-consumer", verificationPath], consumerRoot, filteredEnvironment(process.env, "runtime"));
  if (!existsSync(verificationPath)) fail("consumer verification output was not created");
  const result = JSON.parse(readFileSync(verificationPath, "utf8"));
  const expectedLoadedNames = [...graphCoderExportNames, ...filesystemNames, ...(process.platform === "win32" ? ["@acyclic-labs/fs-win32-x64"] : [])];
  if (result.expected_graphcoder_export_count !== GRAPH_CODER_EXPORT_COUNT) fail("consumer recorded the wrong GraphCoder export count");
  if (JSON.stringify(result.loaded) !== JSON.stringify(expectedLoadedNames)) fail("consumer did not load the exact GraphCoder and filesystem export set");
  if (process.platform === "win32" && result.native_process_owner?.capability !== NATIVE_PROCESS_OWNER_CAPABILITY) fail("consumer did not validate the native companion process owner capability");
  const verification = { ...result, installed_identity: installedIdentity };
  writeFileSync(verificationPath, `${JSON.stringify(verification, null, 2)}\n`);
  return verification;
}

export function runPackageGate(outputArgument) {
  const output = outputRoot(outputArgument);
  claimOutput(output);
  assertCanonicalParents(ROOT, DIST_ROOT, "package dist", true);
  mkdirSync(output, { recursive: true });
  // The runner has already rotated exact outputs. Remove generated directories
  // owned by this lane so stale declarations cannot enter the packed archive.
  rmSync(DIST_ROOT, { recursive: true, force: true });
  const consumerRoot = join(output, "consumer");
  assertCanonicalParents(ROOT, consumerRoot, "package consumer", false);
  if (existsSync(consumerRoot)) assertCanonicalParents(ROOT, consumerRoot, "package consumer", true);
  rmSync(consumerRoot, { recursive: true, force: true });
  mkdirSync(output, { recursive: true });
  const npmCache = join(ROOT, "target", "graphcoder-package-npm-cache");
  ensureOwnedDirectory(npmCache, ROOT, "package npm cache");
  const npmEnvironment = filteredEnvironment(process.env, "toolchain", { npm_config_cache: npmCache });

  run("bun", ["run", "--filter", "@acyclic-labs/graphcoder", "build"], ROOT);
  run("bun", ["run", "--filter", "@acyclic-labs/fs", "build"], ROOT);
  for (const file of GENERATED_DIST) if (!existsSync(join(DIST_ROOT, file))) fail(`build did not produce dist/${file}`);

  const attemptNonce = randomUUID();
  const builtAt = new Date().toISOString();
  const packArgs = [...NPM_PREFIX, "pack", "--pack-destination", output];
  const invocationSha256 = createHash("sha256").update(JSON.stringify({
    executable: NPM_EXECUTABLE,
    args: packArgs,
    cwd: PACKAGE_ROOT,
    environment_policy: "toolchain",
    environment: Object.fromEntries(Object.entries(npmEnvironment).sort(([left], [right]) => left.localeCompare(right))),
  })).digest("hex");
  const sourceWorkingTreeSha256 = workingTreeDigest(ROOT);
  const packedOutput = run(NPM_EXECUTABLE, packArgs, PACKAGE_ROOT, npmEnvironment).trim().split(/\r?\n/u).at(-1);
  if (!packedOutput) fail("npm pack did not report an archive");
  const packedPath = resolve(output, packedOutput);
  if (packedPath !== output && !packedPath.startsWith(`${output}${sep}`)) fail(`npm pack archive escapes output: ${packedOutput}`);
  if (!existsSync(packedPath)) fail(`npm pack archive is missing: ${packedPath}`);
  assertCanonicalParents(ROOT, packedPath, "packed archive", false);
  const archivePath = join(output, "graphcoder-package.tgz");
  assertCanonicalParents(ROOT, archivePath, "package archive", false);
  renameSync(packedPath, archivePath);

  const fsPackedOutput = run(NPM_EXECUTABLE, [...NPM_PREFIX, "pack", "--pack-destination", output], FS_PACKAGE_ROOT, npmEnvironment).trim().split(/\r?\n/u).at(-1);
  if (!fsPackedOutput) fail("npm pack did not report the filesystem companion archive");
  const fsPackedPath = resolve(output, fsPackedOutput);
  if (fsPackedPath !== output && !fsPackedPath.startsWith(`${output}${sep}`)) fail(`filesystem companion archive escapes output: ${fsPackedOutput}`);
  if (!existsSync(fsPackedPath)) fail(`filesystem companion archive is missing: ${fsPackedPath}`);
  assertCanonicalParents(ROOT, fsPackedPath, "filesystem companion archive", false);
  const fsArchivePath = join(output, "acyclic-fs-package.tgz");
  assertCanonicalParents(ROOT, fsArchivePath, "filesystem companion package archive", false);
  renameSync(fsPackedPath, fsArchivePath);

  const nativeCompanion = nativeCompanionPackage(output, npmEnvironment, attemptNonce, builtAt);

  mkdirSync(consumerRoot, { recursive: true });
  writeFileSync(join(consumerRoot, "package.json"), `${JSON.stringify({
    private: true,
    type: "module",
    dependencies: {
      "@acyclic-labs/graphcoder": "file:../graphcoder-package.tgz",
      "@acyclic-labs/fs": "file:../acyclic-fs-package.tgz",
      ...(nativeCompanion === undefined ? {} : {
        [nativeCompanion.packageName]: "file:../acyclic-fs-native-package.tgz",
      }),
    },
  }, null, 2)}\n`);
  run("bun", ["install", "--offline"], consumerRoot, filteredEnvironment(process.env, "runtime"));
  const verificationPath = join(consumerRoot, "verification.json");
  const verification = verifyConsumer(consumerRoot, verificationPath, archivePath);
  const identity = sourceIdentity();
  const receiptPath = join(output, "graphcoder-package.json");
  const receipt = {
    protocol: "acyclic.graphcoder.producer-receipt.v1",
    producer_id: "graphcoder-package-gate",
    source_commit: identity.source_commit,
    source_tree: identity.source_tree,
    observed_after_dispatch: true,
    attempt_nonce: attemptNonce,
    invocation_sha256: invocationSha256,
    source_working_tree_sha256: sourceWorkingTreeSha256,
    exit_code: 0,
    observed_at: new Date().toISOString(),
    outputs: [archivePath, fsArchivePath, ...(nativeCompanion === undefined ? [] : [nativeCompanion.archivePath]), join(consumerRoot, "package.json"), join(consumerRoot, "bun.lock"), verificationPath].map(path => ({ path, sha256: sha256(path) })),
    native_companion: nativeCompanion === undefined ? null : {
      archive: nativeCompanion.archivePath,
      archive_sha256: sha256(nativeCompanion.archivePath),
      binding: nativeCompanion.bindingArtifact,
      invocation_sha256: nativeCompanion.invocationSha256,
    },
    installed_identity: verification.installed_identity,
  };
  assertCanonicalParents(ROOT, receiptPath, "package receipt", false);
  rmSync(receiptPath, { force: true });
  writeFileSync(receiptPath, `${JSON.stringify(receipt, null, 2)}\n`, { flag: "wx" });
  process.stdout.write(`${JSON.stringify({ archive: archivePath, archive_sha256: sha256(archivePath), companion_archive: fsArchivePath, companion_archive_sha256: sha256(fsArchivePath), native_companion_archive: nativeCompanion?.archivePath ?? null, consumer: verification, receipt: receiptPath }, null, 2)}\n`);
  return { archivePath, fsArchivePath, verificationPath, receiptPath };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const [command, outputFlag, output] = process.argv.slice(2);
    if (command !== "run" || outputFlag !== "--output" || output === undefined || process.argv.length !== 5) {
      fail("usage: graphcoder-package-gate.mjs run --output target/graphcoder-package-qualification");
    }
    runPackageGate(output);
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}
