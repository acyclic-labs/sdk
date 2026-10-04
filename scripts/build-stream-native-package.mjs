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
  console.error("usage: build-stream-native-package.mjs --target <package-target> --target-dir <dir> --output <dir> [--provenance <file>] [--verify-install]");
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
const provenancePath = value("--provenance");
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

// The orchestrator supplies the immutable checkout identity. Keeping this
// explicit avoids asking a package build to discover Git through a host shell
// and makes the same builder usable from the Rust generation output tree.
const revision = process.env.SOURCE_REVISION?.trim();
if (!revision || !/^[0-9a-f]{40}$/i.test(revision)) {
  throw new Error("cannot bind native package to an immutable Git source revision");
}

const packageRoot = join(artifactRoot, packageTarget);
rmSync(packageRoot, { recursive: true, force: true });
mkdirSync(packageRoot, { recursive: true });
cpSync(join(packageSource, "package.json"), join(packageRoot, "package.json"));
cpSync(join(packageSource, "index.js"), join(packageRoot, "index.js"));
cpSync(binarySource, join(packageRoot, "acyclic_stream_native.node"));

const packageManifest = JSON.parse(readFileSync(join(packageRoot, "package.json"), "utf8"));
const binaryHash = createHash("sha256").update(readFileSync(join(packageRoot, "acyclic_stream_native.node"))).digest("hex");
const manifestHash = createHash("sha256").update(readFileSync(join(packageRoot, "package.json"))).digest("hex");
const loaderHash = createHash("sha256").update(readFileSync(join(packageRoot, "index.js"))).digest("hex");
const build = {
  schema: "acyclic.sdk.stream.native.build.v1",
  source_revision: revision,
  source_revision_kind: "git-oid",
  generator: {
    name: "scripts/build-stream-native-package.mjs",
    version: "1",
  },
  napi: {
    project: "napi-rs/napi-rs",
    napi: "3.6.1",
    napi_derive: "3.3.3",
    napi_build: "2.3.1",
    abi: "napi8",
  },
  package: {
    name: packageManifest.name,
    version: packageManifest.version,
    target: packageTarget,
    rust_target: target.rust,
  },
  artifacts: {
    binary: "acyclic_stream_native.node",
    binary_sha256: `sha256:${binaryHash}`,
    manifest: "package.json",
    manifest_sha256: `sha256:${manifestHash}`,
    loader: "index.js",
    loader_sha256: `sha256:${loaderHash}`,
  },
  toolchain: {
    rustc: process.env.RUSTC_VERSION ?? "not supplied by caller",
    node: process.version,
  },
  install: {
    verified: verifyInstall,
    resolver: "node_modules package name",
  },
};
writeFileSync(join(packageRoot, "BUILD.json"), `${JSON.stringify(build,
  null, 2)}\n`);

if (provenancePath) {
  const destination = resolve(provenancePath);
  mkdirSync(join(destination, ".."), { recursive: true });
  writeFileSync(destination, `${JSON.stringify({
    schema: "acyclic.sdk.stream.native.provenance.v1",
    source_revision: revision,
    source_revision_kind: "git-oid",
    package_target: packageTarget,
    rust_target: target.rust,
    package_root: packageRoot,
    package_manifest: packageManifest.name,
    package_version: packageManifest.version,
    build: `BUILD.json`,
    build_sha256: `sha256:${createHash("sha256").update(readFileSync(join(packageRoot, "BUILD.json"))).digest("hex")}`,
    binary_sha256: `sha256:${binaryHash}`,
    installed_consumer: {
      verified: verifyInstall,
      resolver: "node_modules package name",
    },
  }, null, 2)}\n`);
}
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

console.log(JSON.stringify({
  schema: "acyclic.sdk.stream.native.build-result.v1",
  packageTarget,
  rustTarget: target.rust,
  packageRoot,
  provenance: provenancePath ? resolve(provenancePath) : undefined,
  installedConsumerVerified: verifyInstall,
}));
