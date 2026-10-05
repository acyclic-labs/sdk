#!/usr/bin/env node

// Build the Windows Filesystem N-API companion and emit the causal producer
// receipt consumed by graphcoder-package-gate.mjs. The receipt is created only
// after Cargo exits successfully and the post-dispatch binding bytes exist.

import { createHash, randomUUID } from "node:crypto";
import { execFileSync } from "node:child_process";
import { constants, copyFileSync, existsSync, lstatSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describeArtifact } from "./graphcoder-artifact.mjs";
import { executeOwnedProcess } from "./graphcoder-process-ownership.mjs";
import { assertCanonicalParents, ensureOwnedDirectory, isWithin } from "./graphcoder-path-ownership.mjs";
import { gitEnvironment, workingTreeDigest } from "./graphcoder-source-fence.mjs";

const ROOT = resolve(fileURLToPath(new URL("..", import.meta.url)));
const QUALIFICATION_ROOT = join(ROOT, "target", "graphcoder-package-qualification");
const PROTOCOL = "acyclic.graphcoder.producer-receipt.v1";
const PRODUCER_ID = "graphcoder-native-binding-producer";
const CARGO = "cargo";
const CARGO_ARGS = ["build", "-p", "acyclic-fs-napi", "--locked", "-j1"];
const CARGO_TIMEOUT_MS = 1_800_000;
const SECRET_ENVIRONMENT = /(?:TOKEN|PASSWORD|SECRET|CREDENTIAL|AUTH|PRIVATE_KEY|ACCESS_KEY)/iu;
const TOOLCHAIN_KEYS = new Set([
  "PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC", "TEMP", "TMP", "CI",
  "CARGO_HOME", "RUSTUP_HOME", "RUSTFLAGS", "RUSTDOCFLAGS", "NUMBER_OF_PROCESSORS",
  "PROCESSOR_ARCHITECTURE",
]);

function fail(message) {
  throw new Error(`graphcoder-native-binding-producer: ${message}`);
}

function digest(value) {
  return createHash("sha256").update(value).digest("hex");
}

function sha256(path) {
  return digest(readFileSync(path));
}

function filteredEnvironment(environment = process.env) {
  const result = {};
  for (const [key, value] of Object.entries(environment)) {
    if (typeof value === "string" && TOOLCHAIN_KEYS.has(key.toUpperCase()) && !SECRET_ENVIRONMENT.test(key)) result[key] = value;
  }
  const pathEntry = Object.entries(environment).find(([key]) => key.toLowerCase() === "path");
  if (pathEntry !== undefined) result[pathEntry[0]] = pathEntry[1];
  return result;
}

function git(sourceRoot, ...args) {
  return execFileSync("git", ["-C", sourceRoot, ...args], {
    encoding: "utf8",
    windowsHide: true,
    env: gitEnvironment(),
  }).trim();
}

function sourceIdentity(sourceRoot) {
  return {
    source_commit: git(sourceRoot, "rev-parse", "HEAD"),
    source_tree: git(sourceRoot, "rev-parse", "HEAD^{tree}"),
    source_working_tree_sha256: workingTreeDigest(sourceRoot),
  };
}

function resolveCargoTool(environment) {
  let candidates;
  try {
    candidates = execFileSync("where.exe", [CARGO], { encoding: "utf8", windowsHide: true, env: environment })
      .split(/\r?\n/u).map(value => value.trim()).filter(Boolean);
  } catch (error) {
    fail(`Cargo tool identity is unavailable: ${error instanceof Error ? error.message : String(error)}`);
  }
  const executable = candidates.map(value => resolve(value)).find(value => {
    const metadata = lstatSync(value, { throwIfNoEntry: false });
    return metadata?.isFile() === true && metadata.isSymbolicLink() === false;
  });
  if (executable === undefined) fail("Cargo tool identity is unavailable: where.exe returned no regular executable");
  let version;
  try {
    version = execFileSync(executable, ["--version"], { encoding: "utf8", windowsHide: true, env: environment }).trim();
  } catch (error) {
    fail(`Cargo tool identity is unavailable: version probe failed: ${error instanceof Error ? error.message : String(error)}`);
  }
  if (version === "") fail("Cargo tool identity is unavailable: version probe was empty");
  return { executable, version, executable_sha256: sha256(executable), version_sha256: digest(version) };
}

function outputPath(value) {
  if (typeof value !== "string" || value.trim() === "") fail("--output requires a path");
  const output = resolve(ROOT, value);
  if (!isWithin(QUALIFICATION_ROOT, output) || output === QUALIFICATION_ROOT) fail(`output must be a fresh child of ${QUALIFICATION_ROOT}`);
  assertCanonicalParents(ROOT, output, "native binding output", false);
  ensureOwnedDirectory(output, ROOT, "native binding output");
  if (readdirSync(output).length !== 0) fail(`native binding output is not fresh: ${output}`);
  return output;
}

function regularFile(path, label) {
  const metadata = lstatSync(path, { throwIfNoEntry: false });
  if (!metadata || !metadata.isFile() || metadata.isSymbolicLink()) fail(`${label} is not a regular file: ${path}`);
}

function buildInvocation(sourceRoot, targetDirectory, environment, cargoTool) {
  const args = [...CARGO_ARGS, "--target-dir", targetDirectory];
  const tool = {
    ...cargoTool,
    args,
    cwd: sourceRoot,
    environment_policy: "toolchain",
    environment: Object.fromEntries(Object.entries(environment).sort(([left], [right]) => left.localeCompare(right))),
  };
  return { args, tool, invocation_sha256: digest(JSON.stringify(tool)) };
}

function bindingReceipt({ artifactPath, source, build, attemptNonce, observedAt, status, signal, stdoutPath, stderrPath }) {
  const artifactSha256 = sha256(artifactPath);
  return {
    protocol: PROTOCOL,
    producer_id: PRODUCER_ID,
    artifact_path: artifactPath,
    sha256: artifactSha256,
    source_commit: source.source_commit,
    source_tree: source.source_tree,
    observed_after_dispatch: true,
    attempt_nonce: attemptNonce,
    invocation_sha256: build.invocation_sha256,
    source_working_tree_sha256: source.source_working_tree_sha256,
    tool: build.tool,
    build: {
      package: "acyclic-fs-napi",
      argv: build.args,
      target_directory: build.tool.args.at(-1),
      exit_code: status,
      signal,
      stdout_path: stdoutPath,
      stderr_path: stderrPath,
      stdout_sha256: sha256(stdoutPath),
      stderr_sha256: sha256(stderrPath),
    },
    platform_target: `win32-${process.arch}`,
    architecture: process.arch,
    observed_at: observedAt,
  };
}

export function verifyNativeBindingReceipt({ artifactPath, receiptPath, sourceRoot = ROOT }) {
  const resolvedArtifact = resolve(artifactPath);
  const resolvedReceipt = resolve(receiptPath);
  if (!isWithin(sourceRoot, resolvedArtifact) || !isWithin(sourceRoot, resolvedReceipt)) fail("binding artifact and receipt must remain inside the source worktree");
  regularFile(resolvedArtifact, "native binding artifact");
  regularFile(resolvedReceipt, "native binding producer receipt");
  let receipt;
  try { receipt = JSON.parse(readFileSync(resolvedReceipt, "utf8")); }
  catch (error) { fail(`native binding producer receipt is invalid JSON: ${error instanceof Error ? error.message : String(error)}`); }
  if (receipt?.producer_id !== PRODUCER_ID || typeof receipt?.tool?.executable !== "string" || !Array.isArray(receipt?.build?.argv)) fail("native binding producer receipt does not identify the Cargo build");
  regularFile(receipt.tool.executable, "Cargo executable");
  if (sha256(receipt.tool.executable) !== receipt.tool.executable_sha256) fail("native binding producer receipt Cargo executable digest does not match");
  let observedVersion;
  try { observedVersion = execFileSync(receipt.tool.executable, ["--version"], { encoding: "utf8", windowsHide: true, env: filteredEnvironment() }).trim(); }
  catch (error) { fail(`native binding producer receipt Cargo version probe failed: ${error instanceof Error ? error.message : String(error)}`); }
  if (observedVersion !== receipt.tool.version || digest(observedVersion) !== receipt.tool.version_sha256) fail("native binding producer receipt Cargo version does not match");
  if (receipt.build.argv.join("\0") !== receipt.tool.args.join("\0")) fail("native binding producer receipt build argv differs from its tool argv");
  for (const [label, pathValue, expectedDigest] of [
    ["stdout", receipt.build.stdout_path, receipt.build.stdout_sha256],
    ["stderr", receipt.build.stderr_path, receipt.build.stderr_sha256],
  ]) {
    if (typeof pathValue !== "string" || !/^[0-9a-f]{64}$/u.test(expectedDigest ?? "")) fail(`native binding producer receipt lacks ${label} log provenance`);
    const logPath = resolve(pathValue);
    if (!isWithin(dirname(resolvedReceipt), logPath)) fail(`native binding producer receipt ${label} log escapes its receipt directory`);
    regularFile(logPath, `Cargo ${label} log`);
    if (sha256(logPath) !== expectedDigest) fail(`native binding producer receipt Cargo ${label} log digest does not match`);
  }
  const record = describeArtifact({
    path: resolvedArtifact,
    sourceCwd: sourceRoot,
    producerReceipt: resolvedReceipt,
    buildId: PRODUCER_ID,
    builtAt: receipt.observed_at,
    requiredProducerReceipt: true,
    requireCausalReceipt: true,
  });
  if (record.sha256 !== receipt.sha256) fail("native binding producer receipt artifact digest does not match bytes");
  return record;
}

export async function runNativeBindingProducer({ outputArgument, sourceRoot = ROOT, execute = executeOwnedProcess, resolveTool = resolveCargoTool, sourceIdentityFn = sourceIdentity, now = () => new Date(), attemptNonce = randomUUID() }) {
  if (process.platform !== "win32") fail(`Windows N-API production is unsupported on ${process.platform}`);
  const output = outputPath(outputArgument);
  const targetDirectory = join(output, "cargo-target");
  const builtBinding = join(targetDirectory, "debug", "acyclic_fs_napi.dll");
  const packageVersion = JSON.parse(readFileSync(join(sourceRoot, "typescript", "packages", "filesystem", "package.json"), "utf8")).version;
  if (typeof packageVersion !== "string" || packageVersion.trim() === "") fail("filesystem package version is invalid");
  const bindingPath = join(output, `acyclic-fs-${packageVersion}-win32-${process.arch}.node`);
  const receiptPath = join(output, "producer-receipt.json");
  const environment = filteredEnvironment();
  const source = sourceIdentityFn(sourceRoot);
  const cargoTool = resolveTool(environment);
  const build = buildInvocation(sourceRoot, targetDirectory, environment, cargoTool);
  const startedAt = now().toISOString();
  const result = await execute(build.tool.executable, build.args, {
    cwd: sourceRoot,
    env: environment,
    timeoutMs: CARGO_TIMEOUT_MS,
    windowsHide: true,
  });
  const stdoutPath = join(output, "cargo.stdout.log");
  const stderrPath = join(output, "cargo.stderr.log");
  writeFileSync(stdoutPath, result.stdout ?? "", { flag: "wx" });
  writeFileSync(stderrPath, result.stderr ?? "", { flag: "wx" });
  if (result.error || result.status !== 0 || result.signal !== null) {
    fail(`Cargo build failed with ${result.status ?? result.signal ?? "start error"}: ${result.error?.message ?? String(result.stderr ?? "").trim()}`);
  }
  const afterSource = sourceIdentityFn(sourceRoot);
  if (afterSource.source_commit !== source.source_commit || afterSource.source_tree !== source.source_tree || afterSource.source_working_tree_sha256 !== source.source_working_tree_sha256) {
    fail("source identity changed during Cargo build");
  }
  if (!existsSync(builtBinding)) fail(`Cargo build did not produce ${builtBinding}`);
  regularFile(builtBinding, "Cargo N-API output");
  assertCanonicalParents(ROOT, bindingPath, "native binding artifact", false);
  copyFileSync(builtBinding, bindingPath, constants.COPYFILE_EXCL);
  regularFile(bindingPath, "native binding artifact");
  const observedAt = now().toISOString();
  const receipt = bindingReceipt({
    artifactPath: bindingPath,
    source,
    build,
    attemptNonce,
    observedAt,
    status: result.status,
    signal: result.signal,
    stdoutPath,
    stderrPath,
  });
  writeFileSync(receiptPath, `${JSON.stringify(receipt, null, 2)}\n`, { flag: "wx" });
  const record = verifyNativeBindingReceipt({ artifactPath: bindingPath, receiptPath, sourceRoot });
  return { output, bindingPath, receiptPath, record, build, startedAt, observedAt };
}

async function main() {
  const [, , command, outputFlag, output] = process.argv;
  if (command !== "run" || outputFlag !== "--output" || output === undefined || process.argv.length !== 5) fail("usage: graphcoder-native-binding-producer.mjs run --output target/graphcoder-package-qualification/native-binding");
  const result = await runNativeBindingProducer({ outputArgument: output });
  process.stdout.write(`${JSON.stringify({ binding: result.bindingPath, receipt: result.receiptPath, sha256: result.record.sha256, source_commit: result.record.source_commit, source_tree: result.record.source_tree }, null, 2)}\n`);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(error => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
