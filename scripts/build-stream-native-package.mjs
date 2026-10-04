#!/usr/bin/env node

import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, rmSync, cpSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const targets = new Map([
  ["linux-x64-gnu", { rust: "x86_64-unknown-linux-gnu", binary: "libacyclic_stream_native.so" }],
  ["linux-x64-musl", { rust: "x86_64-unknown-linux-musl", binary: "libacyclic_stream_native.so" }],
  ["linux-arm64-gnu", { rust: "aarch64-unknown-linux-gnu", binary: "libacyclic_stream_native.so" }],
  ["linux-arm64-musl", { rust: "aarch64-unknown-linux-musl", binary: "libacyclic_stream_native.so" }],
  ["darwin-x64", { rust: "x86_64-apple-darwin", binary: "libacyclic_stream_native.dylib" }],
  ["darwin-arm64", { rust: "aarch64-apple-darwin", binary: "libacyclic_stream_native.dylib" }],
  ["win32-x64", { rust: "x86_64-pc-windows-msvc", binary: "acyclic_stream_native.dll" }],
  ["win32-arm64", { rust: "aarch64-pc-windows-msvc", binary: "acyclic_stream_native.dll" }],
]);

function usage() {
  console.error("usage: build-stream-native-package.mjs --target <package-target> --target-dir <dir> --output <dir> [--verify-install]");
  process.exit(2);
}

const args = process.argv.slice(2);
const value = (name) => {
  const index = args.indexOf(name);
  return index < 0 ? undefined : args[index + 1];
};
const packageTarget = value("--target");
const targetDir = value("--target-dir");
const output = value("--output");
const verifyInstall = args.includes("--verify-install");
if (!targets.has(packageTarget) || !targetDir || !output || args.includes("--help")) usage();

const root = fileURLToPath(new URL("../", import.meta.url));
const crate = join(root, "rust", "crates", "sdk-stream-native");
const packageSource = join(crate, "npm", packageTarget);
const target = targets.get(packageTarget);
const targetRoot = resolve(targetDir);
const artifactRoot = resolve(output);
const releaseRoot = join(targetRoot, target.rust, "release");
const binarySource = join(releaseRoot, target.binary);

if (!existsSync(packageSource)) throw new Error(`unknown native package fixture: ${packageTarget}`);
mkdirSync(targetRoot, { recursive: true });
mkdirSync(artifactRoot, { recursive: true });

const cargo = spawnSync("cargo", [
  "build", "--locked", "--release",
  "--manifest-path", join(crate, "Cargo.toml"),
  "--target", target.rust,
  "--target-dir", targetRoot,
], { cwd: root, stdio: "inherit", shell: process.platform === "win32" });
if (cargo.status !== 0) process.exit(cargo.status ?? 1);
if (!existsSync(binarySource)) throw new Error(`cargo did not produce ${binarySource}`);

const packageRoot = join(artifactRoot, packageTarget);
rmSync(packageRoot, { recursive: true, force: true });
mkdirSync(packageRoot, { recursive: true });
cpSync(join(packageSource, "package.json"), join(packageRoot, "package.json"));
cpSync(join(packageSource, "index.js"), join(packageRoot, "index.js"));
cpSync(binarySource, join(packageRoot, "acyclic_stream_native.node"));

const packageManifest = JSON.parse(readFileSync(join(packageRoot, "package.json"), "utf8"));
const binaryHash = createHash("sha256").update(readFileSync(join(packageRoot, "acyclic_stream_native.node"))).digest("hex");
writeFileSync(join(packageRoot, "BUILD.json"), `${JSON.stringify({
  package: packageManifest.name,
  version: packageManifest.version,
  package_target: packageTarget,
  rust_target: target.rust,
  binary_sha256: binaryHash,
  rustc: process.env.RUSTC_VERSION ?? "not supplied by caller",
}, null, 2)}\n`);

if (verifyInstall) {
  const packageName = packageManifest.name.split("/");
  const consumer = join(targetRoot, `${packageTarget}-consumer`);
  rmSync(consumer, { recursive: true, force: true });
  const installed = join(consumer, "node_modules", ...packageName);
  mkdirSync(join(consumer, "node_modules"), { recursive: true });
  cpSync(packageRoot, installed, { recursive: true });
  const requireFromConsumer = createRequire(join(consumer, "index.js"));
  const binding = requireFromConsumer(packageManifest.name);
  if (binding.nativeStreamCapabilities().maxEndpoints !== 16) {
    throw new Error(`installed ${packageManifest.name} reported an unexpected capability bound`);
  }
  // Windows keeps a loaded N-API DLL locked until this process exits. Leave the
  // installed tree available as a local consumer receipt on that platform.
  if (process.platform !== "win32") rmSync(consumer, { recursive: true, force: true });
}

console.log(JSON.stringify({ packageTarget, rustTarget: target.rust, packageRoot }));
