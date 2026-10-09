#!/usr/bin/env node

import { createHash } from "node:crypto";
import { existsSync, lstatSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const MAX_RECEIPT_BYTES = 1_048_576;
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const PACKAGES = JSON.parse(readFileSync(join(root, "release", "npm-packages.json"), "utf8"))
  .filter(item => item.source === "typescript");

function fail(message) {
  throw new Error(message);
}

function digest(path) {
  const payload = readFileSync(path);
  return [createHash("sha256").update(payload).digest("hex"), payload.length];
}

function expectedAssets() {
  return PACKAGES.map(({ slug: assetSlug, directory, name }) => {
    const manifest = JSON.parse(readFileSync(join(root, "typescript", "packages", directory, "package.json"), "utf8"));
    if (manifest.name !== name) fail(`release identity differs for ${directory}`);
    return {
      asset: `acyclic-labs-${assetSlug}-${manifest.version}.tgz`,
      name,
      version: manifest.version,
      directory: `typescript/packages/${directory}`,
    };
  });
}

async function create(output, sourceSha, buildReceipt) {
  if (!/^(?:[0-9a-f]{40}|[0-9a-f]{64})$/.test(sourceSha)) fail("source commit must be a full lowercase Git object ID");
  // Keep verify-only publication staging independent of the archive validator:
  // publish-npm.yml copies this script without its repository-local imports.
  const { validateArchive } = await import("./validate-npm-package.mjs");
  const { build, receiptSha256: buildSha256 } = readCompilerBuild(buildReceipt, sourceSha);
  const archiveUtils = await import("./archive-utils.mjs");
  const packages = expectedAssets().map(({ directory, ...packageEntry }) => {
    const archive = join(output, packageEntry.asset);
    const validatedSha256 = validateArchive(archive, packageEntry.name, packageEntry.version, directory);
    {
      const { expanded } = archiveUtils.readBoundedGzip(archive, 104_857_600, 536_870_912);
      const prefix = `${directory}/dist/`;
      const expected = build.outputs.filter(file => file.path.startsWith(prefix));
      const observed = archiveUtils.tarEntries(expanded)
        .filter(entry => entry.path.startsWith("package/dist/") && entry.type !== "5")
        .map(entry => ({ path: `${directory}/${entry.path.slice(8)}`, sha256: `sha256:${archiveUtils.sha256(entry.body)}`, bytes: entry.body.length }))
        .sort((a, b) => Buffer.compare(Buffer.from(a.path), Buffer.from(b.path)));
      if (!expected.length || JSON.stringify(expected) !== JSON.stringify(observed)) fail(`archive dist differs from compiler output: ${packageEntry.asset}`);
    }
    const [sha256, size] = digest(archive);
    if (validatedSha256 !== sha256) fail(`archive changed while it was being validated: ${packageEntry.asset}`);
    return { ...packageEntry, sha256, size };
  });
  const expectedNames = new Set(packages.map(item => item.asset));
  const observedNames = new Set(readdirSync(output).filter(name => name.endsWith(".tgz")));
  if (expectedNames.size !== observedNames.size || [...expectedNames].some(name => !observedNames.has(name))) {
    fail("qualification output does not contain exactly the public npm archives");
  }
  const target = join(output, "QUALIFICATION.json");
  if (existsSync(target)) fail("qualification receipt already exists");
  if (sourceSha !== checkedGit(["rev-parse", "HEAD"]).trim() || buildSha256 !== createHash("sha256").update(readReceiptBytes(buildReceipt, 16_777_216)).digest("hex") || JSON.stringify(build.source_files) !== JSON.stringify(compilerSource()) || JSON.stringify(build.outputs) !== JSON.stringify(compilerOutputs())) fail("compiler receipt, inputs or outputs changed during archive admission");
  const provenance = { revision: 1, scope: "typescript-compiler", source_sha256: build.source_sha256, compiler_build_receipt_sha256: `sha256:${buildSha256}`, rust_producers_qualified: false };
  writeFileSync(target, `${JSON.stringify({ ...provenance, source_commit: sourceSha, packages })}\n`, { flag: "wx" });
}

function readReceiptBytes(path, limit) {
  const info = lstatSync(path);
  if (!info.isFile() || info.size > limit) fail("receipt must be a regular file within its size bound");
  const payload = readFileSync(path);
  if (payload.length !== info.size) fail("receipt changed while it was being read");
  return payload;
}

function readCompilerBuild(path, sourceSha) {
  const payload = readReceiptBytes(path, 16_777_216);
  const build = JSON.parse(payload.toString("utf8"));
  if (!build || Object.keys(build).sort().join() !== "executions,outputs,runtime,rust_producers_qualified,schema,scope,source_commit,source_files,source_sha256" || build.schema !== "acyclic.typescript-build-receipt.v1" || build.scope !== "typescript-compiler" || build.rust_producers_qualified !== false) fail("compiler build receipt scope is invalid");
  if (!/^v\d+\.\d+\.\d+/.test(build.runtime) || !Array.isArray(build.executions) || build.executions.length !== consumerCommands().length) fail("compiler build receipt executions are invalid");
  for (const [index, execution] of build.executions.entries()) {
    if (!execution || typeof execution.command !== "string" || !execution.command || !Array.isArray(execution.arguments) || typeof execution.stdout !== "string" || typeof execution.stderr !== "string") fail("compiler build receipt executions are invalid");
    const args = execution.command === "cmd" ? execution.arguments.slice(3) : execution.arguments;
    if (JSON.stringify(args) !== JSON.stringify(consumerCommands()[index])) fail("compiler build receipt command differs");
  }
  if (!/^Version \d+\.\d+\.\d+/.test(build.executions[1].stdout.trim())) fail("compiler build receipt compiler version is absent");
  if (build.source_commit !== sourceSha || sourceSha !== checkedGit(["rev-parse", "HEAD"]).trim()) fail("compiler build receipt belongs to another source commit");
  const source = compilerSource();
  if (JSON.stringify(build.source_files) !== JSON.stringify(source)) fail("compiler build receipt source differs from checkout");
  const encoded = source.map(file => `${file.path}\0${file.sha256}\0${file.bytes}\n`).join("");
  if (build.source_sha256 !== `sha256:${createHash("sha256").update(encoded).digest("hex")}`) fail("compiler build receipt source digest differs");
  if (JSON.stringify(build.outputs) !== JSON.stringify(compilerOutputs(build.executions[2].stdout))) fail("compiler build receipt output differs from checkout");
  return { build, receiptSha256: createHash("sha256").update(payload).digest("hex") };
}

function readReceipt(path) {
  const payload = readReceiptBytes(path, MAX_RECEIPT_BYTES);
  const receipt = JSON.parse(payload.toString("utf8"));
  const keys = "compiler_build_receipt_sha256,packages,revision,rust_producers_qualified,scope,source_commit,source_sha256";
  if (!receipt || typeof receipt !== "object" || Array.isArray(receipt) || Object.keys(receipt).sort().join() !== keys) {
    fail("qualification receipt schema is invalid");
  }
  if (receipt.revision !== 1 || !Array.isArray(receipt.packages) || receipt.packages.length !== PACKAGES.length) fail("qualification receipt schema is invalid");
  if (receipt.scope !== "typescript-compiler" || receipt.rust_producers_qualified !== false || !/^sha256:[0-9a-f]{64}$/.test(receipt.source_sha256) || !/^sha256:[0-9a-f]{64}$/.test(receipt.compiler_build_receipt_sha256)) fail("qualification compiler provenance is invalid");
  const names = new Set();
  for (const item of receipt.packages) {
    if (
      !item || typeof item !== "object" || Array.isArray(item)
      || Object.keys(item).sort().join() !== "asset,name,sha256,size,version"
      || typeof item.asset !== "string" || !/^acyclic-labs-[a-z][a-z-]*-[0-9A-Za-z.+-]+\.tgz$/.test(item.asset)
      || typeof item.name !== "string" || typeof item.version !== "string"
      || typeof item.sha256 !== "string" || !/^[0-9a-f]{64}$/.test(item.sha256)
      || !Number.isInteger(item.size) || item.size <= 0 || names.has(item.asset)
    ) fail("qualification receipt package is invalid");
    names.add(item.asset);
  }
  return receipt;
}

function verify(receiptPath, sourceSha, assetName, archive) {
  const receipt = readReceipt(receiptPath);
  if (receipt.source_commit !== sourceSha) fail("qualification receipt belongs to another source commit");
  const identity = item => `${item.asset}\0${item.name}\0${item.version}`;
  const observed = new Set(receipt.packages.map(identity));
  const expected = new Set(expectedAssets().map(identity));
  if (observed.size !== expected.size || [...expected].some(item => !observed.has(item))) fail("qualification receipt does not identify the public npm packages");
  const matches = receipt.packages.filter(item => item.asset === assetName);
  if (matches.length !== 1) fail("archive is absent from the qualification receipt");
  const [sha256, size] = digest(archive);
  if (matches[0].sha256 !== sha256 || matches[0].size !== size) fail("archive bytes differ from the qualified artifact");
}

function checkedGit(args) {
  const result = spawnSync("git", args, { cwd: root, encoding: "utf8", windowsHide: true });
  if (result.error || result.status !== 0) fail(`source inspection failed: ${result.error ?? result.stderr}`);
  return result.stdout;
}

// This receipt covers the TypeScript compiler inputs and its dist outputs.
// Runtime assets are captured as compiler inputs, not reclassified as proof
// that their Rust/native/WASM producers compiled this source.
function compilerSource() {
  const roots = ["Cargo.toml", "Cargo.lock", "rust-toolchain.toml", ".cargo", "rust", "proto", "scripts", "release", "typescript", "package.json", "bun.lock", "tsconfig.json", "tsconfig.base.json"];
  if (checkedGit(["status", "--porcelain=v1", "--untracked-files=all", "--", ...roots]).trim()) fail("compiler source is not a clean captured checkout");
  if (/^[a-zS] /m.test(checkedGit(["ls-files", "-v", "--", ...roots]))) fail("compiler source has concealed index changes");
  const paths = new Set(checkedGit(["ls-files", "-z", "--", ...roots]).split("\0").filter(Boolean));
  collectRegularFiles("typescript", paths, true);
  return fileRecords(paths);
}

function collectRegularFiles(name, paths, omitOutputs = false) {
  const path = join(root, name), info = lstatSync(path);
  if (info.isSymbolicLink()) fail(`compiler input or output redirects: ${name}`);
  if (info.isFile()) paths.add(name.replaceAll("\\", "/"));
  else if (info.isDirectory()) {
    for (const entry of readdirSync(path)) {
      if (omitOutputs && (["node_modules", "dist"].includes(entry) || /\.(?:tsbuildinfo|log|tgz)$/.test(entry))) continue;
      collectRegularFiles(join(name, entry), paths, omitOutputs);
    }
  } else fail(`compiler input or output is not a regular file: ${name}`);
}

function fileRecords(paths) {
  return [...paths].sort((a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b))).map(path => {
    // Check every ancestor as well as the leaf before hashing a tracked input.
    const parts = path.split("/");
    for (let count = 1; count <= parts.length; count++) {
      if (lstatSync(join(root, ...parts.slice(0, count))).isSymbolicLink()) fail(`compiler source redirects: ${path}`);
    }
    const [sha256, bytes] = digest(join(root, path));
    return { path, sha256: `sha256:${sha256}`, bytes };
  });
}

function compilerOutputs(emitted) {
  const paths = new Set();
  for (const { directory } of PACKAGES) collectRegularFiles(join("typescript/packages", directory, "dist"), paths);
  if (!paths.size) fail("TypeScript compiler emitted no package output");
  const outputs = fileRecords(paths);
  if (emitted !== undefined) {
    const actual = new Set(emitted.split(/\r?\n/).filter(line => line.startsWith("TSFILE: "))
      .map(line => relative(root, resolve(root, line.slice(8))).replaceAll("\\", "/"))
      .filter(path => PACKAGES.some(({ directory }) => path.startsWith(`typescript/packages/${directory}/dist/`))));
    if (actual.size !== paths.size || [...paths].some(path => !actual.has(path))) fail("dist inventory differs from files emitted by this compiler run");
  }
  return outputs;
}

function consumerCommands() {
  return [
    ["install", "--frozen-lockfile"],
    ["x", "tsc", "--version"],
    ["x", "tsc", "-b", "--force", "--listEmittedFiles", "--pretty", "false"],
    ["x", "tsc", "-p", "typescript/packages/sdk/consumer-tsconfig.json", "--pretty", "false"],
    ["test", "typescript/packages/sdk/test/public-consumer.test.ts"],
  ];
}

function consumer(bun, buildReceipt) {
  const started = performance.now();
  if (existsSync(buildReceipt)) fail("TypeScript build receipt already exists");
  let source, revision, outputs;
  const executions = [];
  const commands = consumerCommands();
  for (const args of commands) {
    if (args[0] === "x" && !source) {
      revision = checkedGit(["rev-parse", "HEAD"]).trim();
      source = compilerSource();
    }
    const command = process.platform === "win32" && /\.(?:cmd|bat)$/i.test(bun) ? "cmd" : bun;
    const commandArgs = command === "cmd" ? ["/d", "/c", bun, ...args] : args;
    const result = spawnSync(command, commandArgs, { cwd: root, encoding: "utf8", timeout: 90_000 });
    if (result.error || result.status !== 0) return { schema: 1, passed: false, elapsed_ms: Math.trunc(performance.now() - started), error: String(result.error ?? `${result.stderr}${result.stdout}`).slice(-2000) };
    if (args[0] === "x" && args[2] === "--version" && !/^Version \d+\.\d+\.\d+/.test(result.stdout.trim())) fail("TypeScript compiler version was not observed");
    if (args.includes("-b")) {
      if (JSON.stringify(source) !== JSON.stringify(compilerSource())) fail("compiler source changed during the consumer build");
      outputs = compilerOutputs(result.stdout);
    }
    executions.push({ command, arguments: commandArgs, stdout: result.stdout, stderr: result.stderr });
  }
  {
    if (!source || !revision) fail("consumer build did not capture compiler source inputs");
    if (revision !== checkedGit(["rev-parse", "HEAD"]).trim() || JSON.stringify(source) !== JSON.stringify(compilerSource())) fail("compiler source changed during the consumer build");
    if (!outputs || JSON.stringify(outputs) !== JSON.stringify(compilerOutputs())) fail("compiler output changed during consumer qualification");
    const encoded = source.map(file => `${file.path}\0${file.sha256}\0${file.bytes}\n`).join("");
    const receipt = {
      schema: "acyclic.typescript-build-receipt.v1", scope: "typescript-compiler",
      source_commit: revision, source_sha256: `sha256:${createHash("sha256").update(encoded).digest("hex")}`, source_files: source,
      runtime: process.version, executions, outputs,
      rust_producers_qualified: false,
    };
    writeFileSync(buildReceipt, `${JSON.stringify(receipt, null, 2)}\n`, { flag: "wx" });
  }
  return { schema: 1, passed: true, elapsed_ms: Math.trunc(performance.now() - started), consumer: "typescript/packages/sdk/test/public-consumer.test.ts", surfaces: ["stream", "objects", "filesystem", "wasm"], build_receipt: buildReceipt };
}

const [command, ...args] = process.argv.slice(2);
if (command === "create" && args.length === 3) await create(resolve(args[0]), args[1], resolve(args[2]));
else if (command === "verify" && args.length === 4) verify(resolve(args[0]), args[1], args[2], resolve(args[3]));
else if (command === "consumer" && args.length === 2) {
  const result = consumer(args[0], resolve(args[1]));
  console.log(JSON.stringify(result));
  process.exitCode = result.passed ? 0 : 1;
} else fail("usage: typescript-qualification.mjs create OUTPUT SOURCE_SHA BUILD_RECEIPT | verify RECEIPT SOURCE_SHA ASSET ARCHIVE | consumer BUN BUILD_RECEIPT");
