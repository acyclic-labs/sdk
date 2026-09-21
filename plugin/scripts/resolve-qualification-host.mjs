#!/usr/bin/env node

import { createHash } from "node:crypto";
import { appendFileSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const host = process.argv[2];
if (!new Set(["codex", "claude"]).has(host)) {
  throw new Error("usage: resolve-qualification-host.mjs <codex|claude>");
}
if (!process.env.GITHUB_OUTPUT) {
  throw new Error("GITHUB_OUTPUT is required");
}

const tests = resolve(dirname(fileURLToPath(import.meta.url)), "../tests");
const lockPath = join(tests, "hosts", "package-lock.json");
const lockBytes = readFileSync(lockPath);
const lock = JSON.parse(lockBytes);
if (lock.lockfileVersion !== 3 || !lock.packages || !lock.packages[""]) {
  throw new Error("qualification host lock must be npm lockfile v3");
}

const platform = process.platform;
const arch = process.arch;
if (!new Set(["linux", "darwin", "win32"]).has(platform)) {
  throw new Error(`unsupported qualification platform: ${platform}`);
}
if (!new Set(["x64", "arm64"]).has(arch)) {
  throw new Error(`unsupported qualification architecture: ${arch}`);
}

const rootPackage =
  host === "codex" ? "@openai/codex" : "@anthropic-ai/claude-code";
const platformPackage =
  host === "codex"
    ? `@openai/codex-${platform}-${arch}`
    : `@anthropic-ai/claude-code-${platform}-${arch}`;
const rootKey = `node_modules/${rootPackage}`;
const platformKey = `node_modules/${platformPackage}`;
const root = lock.packages[rootKey];
const selected = lock.packages[platformKey];
const requested = lock.packages[""].dependencies?.[rootPackage];
if (!root || !selected || requested !== root.version) {
  throw new Error(
    `qualification lock is incomplete for ${rootPackage} on ${platform}-${arch}`,
  );
}
for (const [name, entry] of [
  [rootPackage, root],
  [platformPackage, selected],
]) {
  if (
    !entry.integrity?.startsWith("sha512-") ||
    !entry.resolved?.startsWith("https://registry.npmjs.org/")
  ) {
    throw new Error(`${name} lacks a pinned npm registry SHA-512 artifact`);
  }
}

const installedRoot = JSON.parse(
  readFileSync(join(tests, "hosts", rootKey, "package.json")),
);
const installedPlatform = JSON.parse(
  readFileSync(join(tests, "hosts", platformKey, "package.json")),
);
if (
  installedRoot.version !== root.version ||
  installedPlatform.version !== selected.version
) {
  throw new Error(
    "installed qualification host does not match package-lock.json",
  );
}

const binary =
  host === "claude"
    ? join(
        tests,
        "hosts",
        platformKey,
        platform === "win32" ? "claude.exe" : "claude",
      )
    : join(
        tests,
        "hosts",
        platformKey,
        "vendor",
        `${arch === "x64" ? "x86_64" : "aarch64"}-${
          platform === "win32"
            ? "pc-windows-msvc"
            : platform === "darwin"
              ? "apple-darwin"
              : "unknown-linux-musl"
        }`,
        "bin",
        platform === "win32" ? "codex.exe" : "codex",
      );
readFileSync(binary);

const outputs = {
  binary,
  package: rootPackage,
  package_version: root.version,
  package_integrity: root.integrity,
  platform_package: platformPackage,
  platform_version: selected.version,
  platform_integrity: selected.integrity,
  lock_sha256: createHash("sha256").update(lockBytes).digest("hex"),
};
appendFileSync(
  process.env.GITHUB_OUTPUT,
  Object.entries(outputs)
    .map(([name, value]) => `${name}=${value}`)
    .join("\n") + "\n",
);
