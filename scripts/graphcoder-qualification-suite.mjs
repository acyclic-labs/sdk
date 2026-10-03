#!/usr/bin/env node

// Capture one real qualification suite at its execution boundary. The caller
// supplies the exact executable, arguments, working directory, environment
// allowlist, and artifacts that the suite is expected to consume. This utility
// does not invoke a shell and never turns a failed command into a passed suite.

import { createHash } from "node:crypto";
import { execFileSync, spawnSync } from "node:child_process";
import {
  existsSync,
  lstatSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const HEX64 = /^[0-9a-f]{64}$/;
const HEX40 = /^[0-9a-f]{40}$/;
const KINDS = new Set(["native", "compile", "mock", "pty", "package", "wasm"]);

function fail(message) {
  throw new Error(`qualification-suite: ${message}`);
}

function hash(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

function git(cwd, ...args) {
  const result = execFileSync("git", ["-C", cwd, ...args], { encoding: "utf8" });
  return result.trim();
}

function sourceWorktreeChanges(cwd, ignoredUntrackedPaths) {
  const output = git(cwd, "status", "--porcelain", "--untracked-files=all");
  return output.split(/\r?\n/u).filter(line => {
    if (line.trim() === "") return false;
    if (!line.startsWith("??")) return true;
    const path = line.slice(3).trim();
    const absolute = resolve(cwd, path);
    return !ignoredUntrackedPaths.some(ignored => absolute.toLowerCase() === ignored.toLowerCase() || absolute.toLowerCase().startsWith(`${ignored.toLowerCase()}\\`));
  }).join("\n");
}

function iso(value, label) {
  const parsed = value === undefined ? new Date() : new Date(value);
  if (Number.isNaN(parsed.valueOf())) fail(`${label} is not a timestamp`);
  return parsed.toISOString();
}

function regularFile(path, label) {
  const metadata = lstatSync(path, { throwIfNoEntry: false });
  if (!metadata || !metadata.isFile() || metadata.isSymbolicLink()) {
    fail(`${label} must be an existing regular file: ${path}`);
  }
}

function nonemptyText(value, label) {
  if (typeof value !== "string" || value.trim() === "") fail(`${label} must be nonempty`);
  return value;
}

function loadConfig(path) {
  const config = JSON.parse(readFileSync(resolve(path), "utf8"));
  if (!config || typeof config !== "object") fail("config must be an object");
  const id = nonemptyText(config.id, "id");
  const executionKind = nonemptyText(config.execution_kind, "execution_kind");
  if (!KINDS.has(executionKind)) fail(`execution_kind is invalid: ${executionKind}`);
  const platform = nonemptyText(config.platform, "platform");
  const command = config.command;
  if (!command || typeof command !== "object") fail("command is required");
  const executable = nonemptyText(command.executable, "command.executable");
  if (!Array.isArray(command.args) || command.args.some(arg => typeof arg !== "string")) fail("command.args must be strings");
  const cwd = resolve(nonemptyText(command.cwd, "command.cwd"));
  if (!existsSync(cwd)) fail(`command.cwd does not exist: ${cwd}`);
  const env = command.env === undefined ? {} : command.env;
  if (!env || typeof env !== "object" || Array.isArray(env) || Object.entries(env).some(([key, value]) => !/^[A-Za-z_][A-Za-z0-9_]*$/.test(key) || typeof value !== "string")) {
    fail("command.env must be a string environment allowlist");
  }
  const artifacts = config.artifacts;
  if (!Array.isArray(artifacts) || artifacts.length === 0) fail("artifacts must contain the exact consumed artifacts");
  const seen = new Set();
  const normalizedArtifacts = artifacts.map((item, index) => {
    if (!item || typeof item !== "object") fail(`artifact ${index} is not an object`);
    const pathValue = resolve(nonemptyText(item.path, `artifact ${index}.path`));
    if (seen.has(pathValue.toLowerCase())) fail(`duplicate artifact path: ${pathValue}`);
    seen.add(pathValue.toLowerCase());
    regularFile(pathValue, `artifact ${index}`);
    const declaredDigest = nonemptyText(item.sha256, `artifact ${index}.sha256`);
    if (!HEX64.test(declaredDigest)) fail(`artifact ${index}.sha256 must be a lowercase SHA-256 digest`);
    const observedDigest = hash(readFileSync(pathValue));
    if (declaredDigest !== observedDigest) fail(`artifact ${index} digest does not match its bytes: ${pathValue}`);
    const sourceCommit = nonemptyText(item.source_commit, `artifact ${index}.source_commit`);
    const sourceTree = nonemptyText(item.source_tree, `artifact ${index}.source_tree`);
    if (!/^[0-9a-f]{7,64}$/.test(sourceCommit) || !HEX40.test(sourceTree)) fail(`artifact ${index} source provenance is invalid`);
    const builtAt = iso(item.built_at, `artifact ${index}.built_at`);
    return {
      path: pathValue,
      sha256: declaredDigest,
      source_commit: sourceCommit,
      source_tree: sourceTree,
      built_at: builtAt,
      build_id: nonemptyText(item.build_id, `artifact ${index}.build_id`),
      fresh: item.fresh,
    };
  });
  if (normalizedArtifacts.some(item => item.fresh !== true)) fail("every artifact must declare fresh: true");
  const timeoutMs = config.timeout_ms === undefined ? 120_000 : config.timeout_ms;
  if (!Number.isInteger(timeoutMs) || timeoutMs <= 0) fail("timeout_ms must be a positive integer");
  return {
    id,
    descriptor: typeof config.descriptor === "string" && config.descriptor.trim() !== "" ? config.descriptor : id,
    execution_kind: executionKind,
    platform,
    command: { executable, args: [...command.args], cwd, env: { ...env } },
    artifacts: normalizedArtifacts,
    expected_exit_code: config.expected_exit_code === undefined ? 0 : config.expected_exit_code,
    timeout_ms: timeoutMs,
    started_at: config.started_at,
    output: config.output === undefined ? ".qualification/suites" : config.output,
  };
}

export function makeSuiteDescriptor({ config, qualifiedCommit, qualifiedTree, artifactDigests }) {
  return {
    protocol: "acyclic.graphcoder.suite-descriptor.v1",
    id: config.id,
    descriptor: config.descriptor,
    // Bind the suite descriptor itself to the checkout being qualified. The
    // consumed artifacts carry the same pair, but mock/package lanes may use
    // source drivers without a native binary artifact.
    source_commit: qualifiedCommit,
    source_tree: qualifiedTree,
    platform: config.platform,
    execution_kind: config.execution_kind,
    command: {
      ...config.command,
      env: Object.keys(config.command.env).sort(),
    },
    expected_exit_code: config.expected_exit_code,
    timeout_ms: config.timeout_ms,
    consumed_artifacts: config.artifacts.map(item => ({
      path: item.path,
      sha256: artifactDigests.get(item.path),
      source_commit: item.source_commit,
      source_tree: item.source_tree,
      build_id: item.build_id,
    })),
  };
}

function capture(configPath) {
  const config = loadConfig(configPath);
  if (!Number.isInteger(config.expected_exit_code) || config.expected_exit_code < 0) fail("expected_exit_code must be a nonnegative integer");
  const ignoredUntrackedPaths = [resolve(config.output), resolve(configPath)];
  if (sourceWorktreeChanges(config.command.cwd, ignoredUntrackedPaths) !== "") fail("qualified source worktree has uncommitted changes");
  const qualifiedCommit = git(config.command.cwd, "rev-parse", "HEAD");
  const qualifiedTree = git(config.command.cwd, "rev-parse", "HEAD^{tree}");
  const startedAt = iso(config.started_at, "started_at");
  const before = new Map(config.artifacts.map(item => [item.path, hash(readFileSync(item.path))]));
  for (const item of config.artifacts) {
    if (item.source_commit !== qualifiedCommit || item.source_tree !== qualifiedTree) {
      fail(`artifact ${item.path} is not built from the qualified source commit/tree`);
    }
    if (Date.parse(item.built_at) > Date.parse(startedAt)) fail(`artifact ${item.path} was built after the suite started`);
  }
  const inheritedEnv = { PATH: process.env.PATH ?? "", ...config.command.env };
  const result = spawnSync(config.command.executable, config.command.args, {
    cwd: config.command.cwd,
    env: inheritedEnv,
    encoding: "utf8",
    shell: false,
    windowsHide: true,
    maxBuffer: 64 * 1024 * 1024,
    timeout: config.timeout_ms,
  });
  const completedAt = new Date().toISOString();
  const stdout = typeof result.stdout === "string" ? result.stdout : "";
  const stderr = typeof result.stderr === "string" ? result.stderr : "";
  const transcript = `${stdout}${stderr === "" ? "" : `\n[stderr]\n${stderr}`}`;
  const output = resolve(config.output);
  mkdirSync(output, { recursive: true });
  const descriptorPath = resolve(output, `${config.id}.descriptor.json`);
  const transcriptPath = resolve(output, `${config.id}.transcript.log`);
  const recordPath = resolve(output, `${config.id}.record.json`);
  const descriptor = makeSuiteDescriptor({ config, qualifiedCommit, qualifiedTree, artifactDigests: before });
  writeFileSync(descriptorPath, `${JSON.stringify(descriptor, null, 2)}\n`, { flag: "wx" });
  writeFileSync(transcriptPath, transcript, { flag: "wx" });
  let artifactError;
  const artifacts = config.artifacts.map(item => {
    let after = before.get(item.path);
    try {
      after = hash(readFileSync(item.path));
      if (after !== before.get(item.path)) artifactError ??= `consumed artifact changed during suite: ${item.path}`;
    } catch (error) {
      artifactError ??= `consumed artifact could not be read after suite: ${item.path}: ${error instanceof Error ? error.message : String(error)}`;
    }
    return { ...item, sha256: after };
  });
  const exitCode = result.error ? null : result.status;
  const status = artifactError === undefined && exitCode === config.expected_exit_code && result.signal === null && !result.error ? "passed" : "failed";
  const resultError = artifactError ?? result.error?.message;
  const suite = {
    id: config.id,
    descriptor: config.descriptor,
    descriptor_path: descriptorPath,
    descriptor_sha256: hash(readFileSync(descriptorPath)),
    platform: config.platform,
    execution_kind: config.execution_kind,
    status,
    started_at: startedAt,
    completed_at: completedAt,
    artifact_paths: artifacts.map(item => item.path),
    transcript_path: transcriptPath,
    transcript_sha256: hash(readFileSync(transcriptPath)),
  };
  writeFileSync(recordPath, `${JSON.stringify({ suite, artifacts, result: { status, exit_code: exitCode, signal: result.signal, error: resultError } }, null, 2)}\n`, { flag: "wx" });
  process.stdout.write(`${JSON.stringify({ record: recordPath, suite, artifacts, result: { status, exit_code: exitCode, signal: result.signal, error: resultError } }, null, 2)}\n`);
  process.exitCode = status === "passed" ? 0 : 1;
}

export { loadConfig };

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const [command, configPath] = process.argv.slice(2);
    if (command !== "capture" || configPath === undefined || process.argv.length !== 4) fail("usage: graphcoder-qualification-suite.mjs capture CONFIG.json");
    capture(configPath);
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}
