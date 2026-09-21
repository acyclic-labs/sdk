#!/usr/bin/env node

import { createHash } from "node:crypto";
import { existsSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const MAX_RECEIPT_BYTES = 1_048_576;
const PACKAGES = [
  ["objects", "objects"],
  ["stream", "stream"],
  ["inference", "inference"],
  ["machines", "machines"],
  ["fs", "filesystem"],
  ["sdk", "sdk"],
];
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function fail(message) {
  throw new Error(message);
}

function digest(path) {
  const payload = readFileSync(path);
  return [createHash("sha256").update(payload).digest("hex"), payload.length];
}

function expectedAssets() {
  return PACKAGES.map(([assetSlug, directory]) => {
    const manifest = JSON.parse(readFileSync(join(root, "typescript", "packages", directory, "package.json"), "utf8"));
    return { asset: `acyclic-labs-${assetSlug}-${manifest.version}.tgz`, name: manifest.name, version: manifest.version };
  });
}

function create(output, sourceSha) {
  if (!/^(?:[0-9a-f]{40}|[0-9a-f]{64})$/.test(sourceSha)) fail("source commit must be a full lowercase Git object ID");
  const packages = expectedAssets().map(packageEntry => {
    const [sha256, size] = digest(join(output, packageEntry.asset));
    return { ...packageEntry, sha256, size };
  });
  const expectedNames = new Set(packages.map(item => item.asset));
  const observedNames = new Set(readdirSync(output).filter(name => name.endsWith(".tgz")));
  if (expectedNames.size !== observedNames.size || [...expectedNames].some(name => !observedNames.has(name))) {
    fail("qualification output does not contain exactly the six core archives");
  }
  const target = join(output, "QUALIFICATION.json");
  if (existsSync(target)) fail("qualification receipt already exists");
  writeFileSync(target, `${JSON.stringify({ revision: 1, source_commit: sourceSha, packages })}\n`);
}

function readReceipt(path) {
  const payload = readFileSync(path);
  if (payload.length > MAX_RECEIPT_BYTES) fail("qualification receipt exceeds its size bound");
  const receipt = JSON.parse(payload.toString("utf8"));
  if (!receipt || typeof receipt !== "object" || Array.isArray(receipt) || Object.keys(receipt).sort().join() !== "packages,revision,source_commit") {
    fail("qualification receipt schema is invalid");
  }
  if (receipt.revision !== 1 || !Array.isArray(receipt.packages) || receipt.packages.length !== 6) fail("qualification receipt schema is invalid");
  const names = new Set();
  for (const item of receipt.packages) {
    if (
      !item || typeof item !== "object" || Array.isArray(item)
      || Object.keys(item).sort().join() !== "asset,name,sha256,size,version"
      || typeof item.asset !== "string" || !/^acyclic-labs-[a-z]+-[0-9A-Za-z.+-]+\.tgz$/.test(item.asset)
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
  if (observed.size !== expected.size || [...expected].some(item => !observed.has(item))) fail("qualification receipt does not identify the six core packages");
  const matches = receipt.packages.filter(item => item.asset === assetName);
  if (matches.length !== 1) fail("archive is absent from the qualification receipt");
  const [sha256, size] = digest(archive);
  if (matches[0].sha256 !== sha256 || matches[0].size !== size) fail("archive bytes differ from the qualified artifact");
}

function consumer(bun) {
  const started = performance.now();
  const commands = [
    ["install", "--frozen-lockfile"],
    ["x", "tsc", "-b", "--force", "typescript/packages/sdk/tsconfig.json", "--pretty", "false"],
    ["x", "tsc", "-p", "typescript/packages/sdk/consumer-tsconfig.json", "--pretty", "false"],
    ["test", "typescript/packages/sdk/test/public-consumer.test.ts"],
  ];
  for (const args of commands) {
    const command = process.platform === "win32" && /\.(?:cmd|bat)$/i.test(bun) ? "cmd" : bun;
    const commandArgs = command === "cmd" ? ["/d", "/c", bun, ...args] : args;
    const result = spawnSync(command, commandArgs, { cwd: root, encoding: "utf8", timeout: 90_000 });
    if (result.error || result.status !== 0) return { schema: 1, passed: false, elapsed_ms: Math.trunc(performance.now() - started), error: String(result.error ?? `${result.stderr}${result.stdout}`).slice(-2000) };
  }
  return { schema: 1, passed: true, elapsed_ms: Math.trunc(performance.now() - started), consumer: "typescript/packages/sdk/test/public-consumer.test.ts", surfaces: ["stream", "objects", "filesystem", "wasm"] };
}

const [command, ...args] = process.argv.slice(2);
if (command === "create" && args.length === 2) create(resolve(args[0]), args[1]);
else if (command === "verify" && args.length === 4) verify(resolve(args[0]), args[1], args[2], resolve(args[3]));
else if (command === "consumer" && args.length === 1) {
  const result = consumer(args[0]);
  console.log(JSON.stringify(result));
  process.exitCode = result.passed ? 0 : 1;
} else fail("usage: typescript-qualification.mjs create OUTPUT SOURCE_SHA | verify RECEIPT SOURCE_SHA ASSET ARCHIVE | consumer BUN");
