#!/usr/bin/env node

import { createHash } from "node:crypto";
import {
  cpSync,
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { dirname, isAbsolute, join, relative, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const NATIVE_BINARY = "sdk_machines_native.node";

function usage() {
  console.error(
    "usage: build-machines-native-package.mjs --target <package-target> --target-dir <dir> --output <dir> [--provenance <file>] [--verify-install]",
  );
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
if (args.includes("--help") || !packageTarget || !targetDir || !output) usage();

const root = fileURLToPath(new URL("../", import.meta.url));
const crate = join(root, "rust", "crates", "sdk-machines-native");
const npmRoot = join(crate, "npm");

const rustArch = {
  x64: "x86_64",
  arm64: "aarch64",
};

function rustTargetFor(name, manifest) {
  const parts = name.split("-");
  const platform = manifest.os?.[0];
  const cpu = manifest.cpu?.[0];
  if (!platform || !cpu || rustArch[cpu] === undefined) {
    throw new Error(`${name}: package metadata must declare one supported os and cpu`);
  }

  const expectedName = `@acyclic-labs/machines-${name}`;
  if (manifest.name !== expectedName) {
    throw new Error(`${name}: package name must be ${expectedName}`);
  }
  if (manifest.main !== "index.js" || !manifest.files?.includes("index.js") ||
      !manifest.files?.includes(NATIVE_BINARY)) {
    throw new Error(`${name}: package metadata must expose index.js and ${NATIVE_BINARY}`);
  }

  if (platform === "linux") {
    if (parts.length !== 3 || parts[0] !== "linux" ||
        !["gnu", "musl"].includes(parts[2])) {
      throw new Error(`${name}: Linux packages must use an explicit -gnu or -musl target suffix`);
    }
    return `${rustArch[cpu]}-unknown-linux-${parts[2]}`;
  }
  if (platform === "darwin") {
    if (parts.length !== 2 || parts[0] !== "darwin") {
      throw new Error(`${name}: Darwin package target must be darwin-<arch>`);
    }
    return `${rustArch[cpu]}-apple-darwin`;
  }
  if (platform === "win32") {
    if (parts.length !== 2 || parts[0] !== "win32") {
      throw new Error(`${name}: Windows package target must be win32-<arch>`);
    }
    return `${rustArch[cpu]}-pc-windows-msvc`;
  }
  throw new Error(`${name}: unsupported native package platform ${platform}`);
}

function binaryFor(platform, rustTarget) {
  if (platform === "win32") return "sdk_machines_native.dll";
  if (platform === "darwin") return "libsdk_machines_native.dylib";
  if (rustTarget.endsWith("-musl") || rustTarget.endsWith("-gnu")) {
    return "libsdk_machines_native.so";
  }
  throw new Error(`cannot derive native binary name for ${platform}/${rustTarget}`);
}

function loadTargets() {
  const entries = readdirSync(npmRoot, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)
    .sort();
  if (entries.length === 0) throw new Error(`no native package metadata under ${npmRoot}`);

  const targets = new Map();
  for (const name of entries) {
    const packageSource = join(npmRoot, name);
    const manifestPath = join(packageSource, "package.json");
    const loaderPath = join(packageSource, "index.js");
    if (!existsSync(manifestPath) || !existsSync(loaderPath)) {
      throw new Error(`${name}: package.json and index.js are required Rust-owned metadata`);
    }
    const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
    const loader = readFileSync(loaderPath, "utf8");
    if (!loader.includes(`require("./${NATIVE_BINARY}")`) &&
        !loader.includes(`require('./${NATIVE_BINARY}')`)) {
      throw new Error(`${name}: index.js must load ./${NATIVE_BINARY}`);
    }
    const rustTarget = rustTargetFor(name, manifest);
    targets.set(name, {
      name,
      manifest,
      packageSource,
      rust: rustTarget,
      binary: binaryFor(manifest.os[0], rustTarget),
    });
  }
  return targets;
}

const targets = loadTargets();
const target = targets.get(packageTarget);
if (target === undefined) {
  throw new Error(`unknown native package target ${packageTarget}; Rust-owned targets: ${[...targets.keys()].join(", ")}`);
}

// Bind the build to the exact source identity before creating or deleting any
// output. A dirty checkout is accepted only when the caller supplies both
// Rust model and source-content identities; otherwise an exact clean HEAD is
// required. This keeps a failed provenance check from starting Cargo or
// removing a previous package artifact.
const revision = process.env.SOURCE_REVISION?.trim();
if (!revision || !/^[0-9a-f]{40}$/i.test(revision)) {
  throw new Error("cannot bind native package to an immutable Git source revision");
}
const sourceModelRevision = process.env.SOURCE_MODEL_REVISION?.trim() || undefined;
if (sourceModelRevision !== undefined && !/^[0-9a-f]{64}$/i.test(sourceModelRevision)) {
  throw new Error("SOURCE_MODEL_REVISION must be a 64-character Rust model identity");
}
const sourceContentSha256 = process.env.SOURCE_CONTENT_SHA256?.trim() || undefined;
if (sourceContentSha256 !== undefined && !/^[0-9a-f]{64}$/i.test(sourceContentSha256)) {
  throw new Error("SOURCE_CONTENT_SHA256 must be a 64-character SHA-256 digest");
}

const gitHeadResult = spawnSync("git", ["rev-parse", "--verify", "HEAD"], {
  cwd: root,
  encoding: "utf8",
  stdio: ["ignore", "pipe", "ignore"],
});
const gitHead = gitHeadResult.status === 0 ? gitHeadResult.stdout.trim() : undefined;
if (gitHead !== undefined && !/^[0-9a-f]{40}$/i.test(gitHead)) {
  throw new Error("source checkout returned an invalid Git HEAD");
}
if (gitHead !== undefined && gitHead.toLowerCase() !== revision.toLowerCase()) {
  throw new Error(`SOURCE_REVISION ${revision} does not match checkout HEAD ${gitHead}`);
}
if (gitHead !== undefined) {
  const status = spawnSync("git", ["status", "--porcelain=v1", "--untracked-files=all"], {
    cwd: root,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"],
  });
  const clean = status.status === 0 && status.stdout.trim().length === 0;
  if (!clean && (sourceModelRevision === undefined || sourceContentSha256 === undefined)) {
    throw new Error("dirty source checkout requires SOURCE_MODEL_REVISION and SOURCE_CONTENT_SHA256 provenance");
  }
} else if (sourceModelRevision === undefined || sourceContentSha256 === undefined) {
  throw new Error("Git metadata is unavailable; SOURCE_MODEL_REVISION and SOURCE_CONTENT_SHA256 are required");
}

const targetRoot = resolve(targetDir);
const artifactRoot = resolve(output);
const pathSeparator = process.platform === "win32" ? "\\" : "/";
const pathInside = (parent, child) => {
  const childRelative = relative(parent, child);
  return childRelative === "" ||
    (!childRelative.startsWith(`..${pathSeparator}`) &&
      childRelative !== ".." && !isAbsolute(childRelative));
};
if (pathInside(root, targetRoot) || pathInside(root, artifactRoot)) {
  throw new Error("native build target and package output must be outside the Rust source checkout");
}
if (pathInside(targetRoot, artifactRoot) || pathInside(artifactRoot, targetRoot)) {
  throw new Error("native build target and package output must be disjoint directories");
}
if (provenancePath !== undefined && pathInside(root, resolve(provenancePath))) {
  throw new Error("native provenance output must be outside the Rust source checkout");
}
const releaseRoot = join(targetRoot, target.rust, "release");
const binarySource = join(releaseRoot, target.binary);
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
cpSync(join(target.packageSource, "package.json"), join(packageRoot, "package.json"));
cpSync(join(target.packageSource, "index.js"), join(packageRoot, "index.js"));
cpSync(binarySource, join(packageRoot, NATIVE_BINARY));

const packageManifest = JSON.parse(readFileSync(join(packageRoot, "package.json"), "utf8"));
let installedConsumerVerified = false;
if (verifyInstall) {
  const packageName = packageManifest.name.split("/");
  const consumer = join(targetRoot, `${packageTarget}-consumer`);
  rmSync(consumer, { recursive: true, force: true });
  const installed = join(consumer, "node_modules", ...packageName);
  mkdirSync(join(consumer, "node_modules"), { recursive: true });
  cpSync(packageRoot, installed, { recursive: true });
  const requireFromConsumer = createRequire(join(consumer, "index.js"));
  const binding = requireFromConsumer(packageManifest.name);
  if (typeof binding?.MachinesNativeClient !== "function") {
    throw new Error(`${packageManifest.name} did not expose MachinesNativeClient`);
  }
  installedConsumerVerified = true;
  // Windows keeps a loaded N-API DLL locked until this process exits. Leave the
  // installed tree available as a local consumer receipt on that platform.
  if (process.platform !== "win32") rmSync(consumer, { recursive: true, force: true });
}

const binaryHash = createHash("sha256")
  .update(readFileSync(join(packageRoot, NATIVE_BINARY))).digest("hex");
const manifestHash = createHash("sha256")
  .update(readFileSync(join(packageRoot, "package.json"))).digest("hex");
const loaderHash = createHash("sha256")
  .update(readFileSync(join(packageRoot, "index.js"))).digest("hex");
const build = {
  schema: "acyclic.sdk.machines.native.build.v1",
  source_revision: revision,
  source_revision_kind: "git-oid",
  source_model_revision: sourceModelRevision ?? null,
  source_content_sha256: sourceContentSha256 ? `sha256:${sourceContentSha256}` : null,
  generator: {
    name: "scripts/build-machines-native-package.mjs",
    version: "1",
    targets_source: "rust/crates/sdk-machines-native/npm",
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
    binary: NATIVE_BINARY,
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
    verified: installedConsumerVerified,
    resolver: "node_modules package name",
  },
};
writeFileSync(join(packageRoot, "BUILD.json"), `${JSON.stringify(build, null, 2)}\n`);

if (provenancePath) {
  const destination = resolve(provenancePath);
  mkdirSync(dirname(destination), { recursive: true });
  writeFileSync(destination, `${JSON.stringify({
    schema: "acyclic.sdk.machines.native.provenance.v1",
    source_revision: revision,
    source_revision_kind: "git-oid",
    source_model_revision: sourceModelRevision ?? null,
    source_content_sha256: sourceContentSha256 ? `sha256:${sourceContentSha256}` : null,
    package_target: packageTarget,
    rust_target: target.rust,
    package_root: packageRoot,
    package_manifest: packageManifest.name,
    package_version: packageManifest.version,
    build: "BUILD.json",
    build_sha256: `sha256:${createHash("sha256").update(readFileSync(join(packageRoot, "BUILD.json"))).digest("hex")}`,
    binary_sha256: `sha256:${binaryHash}`,
    installed_consumer: {
      verified: installedConsumerVerified,
      resolver: "node_modules package name",
    },
  }, null, 2)}\n`);
}

console.log(JSON.stringify({
  schema: "acyclic.sdk.machines.native.build-result.v1",
  packageTarget,
  rustTarget: target.rust,
  packageRoot,
  provenance: provenancePath ? resolve(provenancePath) : undefined,
  installedConsumerVerified,
}));
