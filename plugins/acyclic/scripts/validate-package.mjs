#!/usr/bin/env node

import { createHash } from "node:crypto";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { basename, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";

function fail(message) {
  throw new Error(message);
}

function parseArguments(argv) {
  const result = { require_target: [] };
  for (const argument of argv) {
    if (!result.package && !argument.startsWith("--")) result.package = argument;
    else if (argument === "--require-universal") result.require_universal = true;
    else if (result.pending_target) {
      result.require_target.push(argument);
      result.pending_target = false;
    } else if (argument === "--require-target") result.pending_target = true;
    else fail(`unknown argument: ${argument}`);
  }
  if (!result.package) fail("package path is required");
  if (result.pending_target) fail("--require-target requires a value");
  return result;
}

function walk(directory) {
  return readdirSync(directory, { withFileTypes: true }).flatMap(entry => {
    const path = join(directory, entry.name);
    return entry.isDirectory() ? walk(path) : [path];
  });
}

const args = parseArguments(process.argv.slice(2));
const root = resolve(args.package);
const plugin = join(root, "plugins", "acyclic");
const targetSchema = JSON.parse(readFileSync(join(plugin, "bin", "targets.json"), "utf8"));
if (targetSchema.version !== 1 || !targetSchema.targets || Array.isArray(targetSchema.targets)) {
  fail("unsupported Acyclic target schema");
}
const SUPPORTED_TARGETS = new Set(Object.keys(targetSchema.targets));
for (const path of [
  join(plugin, "plugin.json"),
  join(plugin, ".codex-plugin", "plugin.json"),
  join(plugin, ".mcp.json"),
  join(plugin, "hooks", "hooks.json"),
]) {
  if (!existsSync(path) || !statSync(path).isFile()) fail(`missing package asset: ${path}`);
  JSON.parse(readFileSync(path, "utf8"));
}

const manifest = JSON.parse(readFileSync(join(plugin, "bin", "platform-binaries.json"), "utf8"));
if (manifest.version !== 1 || !manifest.targets || typeof manifest.targets !== "object" || Array.isArray(manifest.targets)) {
  fail("unsupported platform binary manifest");
}
const targets = new Set(Object.keys(manifest.targets));
if (args.require_universal && (targets.size !== SUPPORTED_TARGETS.size || [...SUPPORTED_TARGETS].some(target => !targets.has(target)))) {
  const missing = [...SUPPORTED_TARGETS].filter(target => !targets.has(target)).sort();
  const extra = [...targets].filter(target => !SUPPORTED_TARGETS.has(target)).sort();
  fail(`universal target mismatch; missing=${JSON.stringify(missing)}, extra=${JSON.stringify(extra)}`);
}
if (args.require_universal) {
  const version = JSON.parse(readFileSync(join(plugin, "package.json"), "utf8")).version;
  const receipts = [
    ["linux", "x86_64", "linux-fuse"],
    ["macos", "aarch64", "macos-nfs"],
    ["windows", "x86_64", "windows-projfs"],
  ];
  for (const [os, arch, backend] of receipts) {
    const path = join(plugin, "certification", `native-mount-${os}-${arch}.json`);
    if (!existsSync(path) || !statSync(path).isFile()) fail(`missing certification receipt: ${path}`);
    const receipt = JSON.parse(readFileSync(path, "utf8"));
    if (
      receipt.schema !== "acyclic-native-mount-qualification-v2"
      || receipt.os !== os
      || receipt.arch !== arch
      || receipt.required_kind !== backend
      || receipt.release_version !== version
      || receipt.passed !== true
      || typeof receipt.executable_blake3 !== "string"
      || !/^[0-9a-f]{64}$/.test(receipt.executable_blake3)
    ) fail(`invalid certification receipt: ${path}`);
  }
}
for (const target of args.require_target) {
  if (!targets.has(target)) fail(`release package is missing required target: ${target}`);
}
for (const [target, entry] of Object.entries(manifest.targets)) {
  if (!SUPPORTED_TARGETS.has(target)) fail(`unsupported binary target: ${target}`);
  if (!entry || typeof entry !== "object" || typeof entry.path !== "string") fail(`invalid binary entry: ${target}`);
  const expectedName = targetSchema.targets[target];
  const expectedPath = `${target}/${expectedName}`;
  if (entry.path !== expectedPath) fail(`invalid binary path for ${target}`);
  const binary = join(plugin, "bin", target, expectedName);
  if (!existsSync(binary)) fail(`missing binary for ${target}`);
  const actual = createHash("sha256").update(readFileSync(binary)).digest("hex");
  if (actual !== entry.sha256) fail(`binary checksum mismatch for ${target}`);
  const verified = spawnSync(process.execPath, [join(plugin, "bin", "verify.js"), target], {
    cwd: plugin,
    stdio: "inherit",
  });
  if (verified.status !== 0) fail(`verification failed for ${target}`);
}
if (walk(plugin).some(path => basename(path) === "Cargo.toml" || basename(path) === "Cargo.lock" || path.endsWith(".rs"))) {
  fail("release package contains Rust sources or Cargo metadata");
}
