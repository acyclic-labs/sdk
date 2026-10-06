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
import { runtimeSourceIdentity } from "./inference-native-source-identity.mjs";

const NATIVE_BINARY = "acyclic_inference_native.node";

function usage() {
  console.error(
    "usage: build-inference-native-package.mjs --target <package-target> --target-dir <dir> --output <dir> [--provenance <file>] [--verify-install]",
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
const crate = join(root, "rust", "crates", "sdk-inference-native");
const npmRoot = join(crate, "npm");

// Keep package provenance bound to the same Rust source closure used by the
// sdk-contract-wire generator. Parsing its Rust-owned MODEL_SOURCES list here
// avoids maintaining a second, silently drifting list in this packaging tool.
const wireGenerator = join(root, "rust", "crates", "sdk-contract-wire", "src", "bin", "sdk-contract-wire.rs");
const wireSource = readFileSync(wireGenerator, "utf8");
const modelSourcesMatch = wireSource.match(/const MODEL_SOURCES:[\s\S]*?= &\[([\s\S]*?)\n\];/);
if (!modelSourcesMatch) throw new Error("cannot locate Rust MODEL_SOURCES identity definition");
const modelSourcePaths = [...modelSourcesMatch[1].matchAll(/\(\s*"([^"]+)"\s*,\s*include_bytes!/g)].map((match) => match[1]);
if (modelSourcePaths.length === 0) throw new Error("Rust MODEL_SOURCES identity definition is empty");

const wirePath = (name) => {
  const path = modelSourcePaths.find((candidate) => candidate === `rust/crates/sdk-contract-wire/src/${name}`);
  if (!path) throw new Error(`Rust MODEL_SOURCES does not contain ${name}`);
  return path;
};
const inferenceContentPaths = [
  wirePath("lib.rs"),
  wirePath("family_registry.rs"),
  wirePath("credential.rs"),
  wirePath("transport.rs"),
  wirePath("inference.rs"),
];
const digestParts = (parts, includePaths) => {
  const hash = createHash("sha256");
  parts.forEach((part, index) => {
    if (includePaths) hash.update(includePaths[index]);
    hash.update(Buffer.from([0]));
    hash.update(part);
    hash.update(Buffer.from([0]));
  });
  return hash.digest("hex");
};
const sourceIdentity = () => {
  const modelParts = modelSourcePaths.map((path) => readFileSync(join(root, ...path.split("/"))));
  const contentParts = inferenceContentPaths.map((path) => readFileSync(join(root, ...path.split("/"))));
  return {
    modelRevision: digestParts(modelParts, modelSourcePaths),
    sourceContentSha256: digestParts(contentParts),
    modelSources: modelSourcePaths.map((path, index) => ({
      path,
      sha256: createHash("sha256").update(modelParts[index]).digest("hex"),
    })),
    contentSources: inferenceContentPaths,
  };
};
const sourceIdentityBeforeBuild = sourceIdentity();

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

  const expectedName = `@acyclic-labs/inference-${name}`;
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
    if (manifest.libc?.[0] !== (parts[2] === "gnu" ? "glibc" : "musl")) {
      throw new Error(`${name}: Linux package libc metadata does not match its target suffix`);
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
  if (platform === "win32") return NATIVE_BINARY.replace(".node", ".dll");
  if (platform === "darwin") return `lib${NATIVE_BINARY.replace(".node", ".dylib")}`;
  if (rustTarget.endsWith("-musl") || rustTarget.endsWith("-gnu")) {
    return `lib${NATIVE_BINARY.replace(".node", ".so")}`;
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

const runtimeManifest = join(crate, "Cargo.toml");
const runtimeSourceIdentityBeforeBuild = runtimeSourceIdentity(root, runtimeManifest, target.rust);

// Bind the build to the exact source identity before creating or deleting any
// output. Dirty checkouts require both Rust model and source-content identities.
const revision = process.env.SOURCE_REVISION?.trim();
if (!revision || !/^[0-9a-f]{40}$/i.test(revision)) {
  throw new Error("cannot bind native package to an immutable Git source revision");
}
const suppliedSourceModelRevision = process.env.SOURCE_MODEL_REVISION?.trim() || undefined;
if (suppliedSourceModelRevision !== undefined && !/^[0-9a-f]{64}$/i.test(suppliedSourceModelRevision)) {
  throw new Error("SOURCE_MODEL_REVISION must be a 64-character Rust model identity");
}
const suppliedSourceContentSha256 = process.env.SOURCE_CONTENT_SHA256?.trim() || undefined;
if (suppliedSourceContentSha256 !== undefined && !/^[0-9a-f]{64}$/i.test(suppliedSourceContentSha256)) {
  throw new Error("SOURCE_CONTENT_SHA256 must be a 64-character SHA-256 digest");
}
if (suppliedSourceModelRevision !== undefined &&
    suppliedSourceModelRevision.toLowerCase() !== sourceIdentityBeforeBuild.modelRevision) {
  throw new Error("SOURCE_MODEL_REVISION does not match the Rust MODEL_SOURCES checkout closure");
}
if (suppliedSourceContentSha256 !== undefined &&
    suppliedSourceContentSha256.toLowerCase() !== sourceIdentityBeforeBuild.sourceContentSha256) {
  throw new Error("SOURCE_CONTENT_SHA256 does not match the Rust Inference source closure");
}
const sourceModelRevision = sourceIdentityBeforeBuild.modelRevision;
const sourceContentSha256 = sourceIdentityBeforeBuild.sourceContentSha256;
const suppliedRuntimeClosureSha256 = process.env.SOURCE_RUNTIME_CLOSURE_SHA256?.trim() || undefined;
if (suppliedRuntimeClosureSha256 !== undefined && !/^[0-9a-f]{64}$/i.test(suppliedRuntimeClosureSha256)) {
  throw new Error("SOURCE_RUNTIME_CLOSURE_SHA256 must be a 64-character SHA-256 digest");
}
if (suppliedRuntimeClosureSha256 !== undefined &&
    suppliedRuntimeClosureSha256.toLowerCase() !== runtimeSourceIdentityBeforeBuild.closureSha256) {
  throw new Error("SOURCE_RUNTIME_CLOSURE_SHA256 does not match the native Rust source closure");
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
if (!clean && (suppliedSourceModelRevision === undefined || suppliedSourceContentSha256 === undefined)) {
      throw new Error("dirty source checkout requires supplied Rust source closure provenance");
    }
} else if (gitHead === undefined &&
           (suppliedSourceModelRevision === undefined || suppliedSourceContentSha256 === undefined)) {
  throw new Error("Git metadata is unavailable; supplied Rust source closure provenance is required");
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
const sourceIdentityAfterBuild = sourceIdentity();
if (sourceIdentityAfterBuild.modelRevision !== sourceIdentityBeforeBuild.modelRevision ||
    sourceIdentityAfterBuild.sourceContentSha256 !== sourceIdentityBeforeBuild.sourceContentSha256) {
  throw new Error("Rust source closure changed during native compilation");
}
const runtimeSourceIdentityAfterBuild = runtimeSourceIdentity(root, runtimeManifest, target.rust);
if (runtimeSourceIdentityAfterBuild.closureSha256 !== runtimeSourceIdentityBeforeBuild.closureSha256 ||
    runtimeSourceIdentityAfterBuild.recipeSha256 !== runtimeSourceIdentityBeforeBuild.recipeSha256) {
  throw new Error("native Rust runtime source closure changed during compilation");
}

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
  if (typeof binding?.NativeInferenceClient !== "function") {
    throw new Error(`${packageManifest.name} did not expose NativeInferenceClient`);
  }
  if (typeof binding?.NativeInferenceCancellation !== "function") {
    throw new Error(`${packageManifest.name} did not expose NativeInferenceCancellation`);
  }
  const cancellation = new binding.NativeInferenceCancellation();
  if (cancellation.cancelled !== false) {
    throw new Error(`${packageManifest.name} cancellation handle did not start active`);
  }
  cancellation.cancel();
  if (cancellation.cancelled !== true) {
    throw new Error(`${packageManifest.name} cancellation handle did not transition to cancelled`);
  }
  installedConsumerVerified = true;
  if (process.platform !== "win32") rmSync(consumer, { recursive: true, force: true });
}

const binaryHash = createHash("sha256")
  .update(readFileSync(join(packageRoot, NATIVE_BINARY))).digest("hex");
const manifestHash = createHash("sha256")
  .update(readFileSync(join(packageRoot, "package.json"))).digest("hex");
const loaderHash = createHash("sha256")
  .update(readFileSync(join(packageRoot, "index.js"))).digest("hex");
const build = {
  schema: "acyclic.sdk.inference.native.build.v1",
  source_revision: revision,
  source_revision_kind: "git-oid",
  source_model_revision: sourceModelRevision,
  source_model_revision_kind: "rust-model-sha256",
  source_content_sha256: `sha256:${sourceContentSha256}`,
  source_content_sources: sourceIdentityBeforeBuild.contentSources,
  source_model_sources: sourceIdentityBeforeBuild.modelSources,
  runtime_source_closure_sha256: `sha256:${runtimeSourceIdentityBeforeBuild.closureSha256}`,
  runtime_build_recipe_sha256: `sha256:${runtimeSourceIdentityBeforeBuild.recipeSha256}`,
  runtime_source_files: runtimeSourceIdentityBeforeBuild.files,
  generator: {
    name: "scripts/build-inference-native-package.mjs",
    version: "1",
    targets_source: "rust/crates/sdk-inference-native/npm",
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
    schema: "acyclic.sdk.inference.native.provenance.v1",
    source_revision: revision,
    source_revision_kind: "git-oid",
    source_model_revision: sourceModelRevision,
    source_model_revision_kind: "rust-model-sha256",
    source_content_sha256: `sha256:${sourceContentSha256}`,
    runtime_source_closure_sha256: `sha256:${runtimeSourceIdentityBeforeBuild.closureSha256}`,
    runtime_build_recipe_sha256: `sha256:${runtimeSourceIdentityBeforeBuild.recipeSha256}`,
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
  schema: "acyclic.sdk.inference.native.build-result.v1",
  packageTarget,
  rustTarget: target.rust,
  packageRoot,
  provenance: provenancePath ? resolve(provenancePath) : undefined,
  installedConsumerVerified,
}));
