#!/usr/bin/env node

// Record the immutable build and installed-consumer evidence for one Inference
// native companion. The package builder owns compilation; this helper checks
// that the archive, package metadata, and cancellation export describe the
// same release target and source identity.
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";

const args = process.argv.slice(2);
const value = (name) => {
  const index = args.indexOf(name);
  return index < 0 ? undefined : args[index + 1];
};
const packageRoot = value("--package-root");
const archive = value("--archive");
const target = value("--target");
const sourceRevision = value("--source-revision");
const installedConsumer = value("--installed-consumer");
const output = value("--output");
if (!packageRoot || !archive || !target || !sourceRevision || !installedConsumer || !output) {
  throw new Error("usage: collect-inference-native-proof.mjs --package-root DIR --archive FILE --target ID --source-revision OID --installed-consumer FILE --output FILE");
}
if (!/^[0-9a-f]{40}$/i.test(sourceRevision)) {
  throw new Error("source revision must be an immutable Git OID");
}

const sha256 = (path) => `sha256:${createHash("sha256").update(readFileSync(path)).digest("hex")}`;
const root = resolve(packageRoot);
const manifestPath = join(root, "package.json");
const buildPath = join(root, "BUILD.json");
const loaderPath = join(root, "index.js");
const binaryPath = join(root, "acyclic_inference_native.node");
const inputs = [manifestPath, buildPath, loaderPath, binaryPath, archive, installedConsumer];
for (const path of inputs) {
  if (!existsSync(path)) throw new Error(`missing Inference native proof input: ${path}`);
}

const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
const build = JSON.parse(readFileSync(buildPath, "utf8"));
if (build.schema !== "acyclic.sdk.inference.native.build.v1") {
  throw new Error("Inference native BUILD.json schema mismatch");
}
if (build.source_revision !== sourceRevision || build.source_revision_kind !== "git-oid") {
  throw new Error("Inference native BUILD.json source revision differs from release source");
}
if (build.package?.target !== target || build.package?.name !== manifest.name ||
    build.package?.version !== manifest.version) {
  throw new Error("Inference native package identity differs from package.json or matrix target");
}
if (!manifest.name.startsWith("@acyclic-labs/inference-") || manifest.main !== "index.js") {
  throw new Error("Inference native package metadata has the wrong package identity");
}
if (!manifest.files?.includes("index.js") || !manifest.files?.includes("acyclic_inference_native.node")) {
  throw new Error("Inference native package metadata does not include its loader and binary");
}
if (build.artifacts?.binary_sha256 !== sha256(binaryPath) ||
    build.artifacts?.manifest_sha256 !== sha256(manifestPath) ||
    build.artifacts?.loader_sha256 !== sha256(loaderPath)) {
  throw new Error("Inference native BUILD.json artifact hashes are stale");
}

const installed = JSON.parse(readFileSync(installedConsumer, "utf8"));
if (installed.schema !== "acyclic.sdk.inference.native.installed-consumer.v1" ||
    installed.source_revision !== sourceRevision || installed.package_target !== target ||
    installed.status !== "passed") {
  throw new Error("installed Inference consumer receipt does not match the release identity");
}
for (const check of ["npm-install", "napi-loader", "NativeInferenceClient-export",
  "NativeInferenceCancellation-export", "cancellation-transition"]) {
  if (!installed.checks?.includes(check)) {
    throw new Error(`installed Inference consumer receipt is missing ${check}`);
  }
}
if (installed.archive_sha256 !== sha256(archive)) {
  throw new Error("installed Inference consumer receipt archive hash is stale");
}

const proof = {
  schema: "acyclic.sdk.inference.native.package-proof.v1",
  source_revision: sourceRevision,
  target,
  status: "passed",
  package: {
    name: manifest.name,
    version: manifest.version,
    root,
    archive: resolve(archive),
    archive_sha256: sha256(archive),
    build_sha256: sha256(buildPath),
    manifest_sha256: sha256(manifestPath),
    loader_sha256: sha256(loaderPath),
    binary_sha256: sha256(binaryPath),
  },
  installed_consumer: {
    receipt: resolve(installedConsumer),
    receipt_sha256: sha256(installedConsumer),
    checks: installed.checks,
  },
};
mkdirSync(resolve(output, ".."), { recursive: true });
writeFileSync(resolve(output), `${JSON.stringify(proof, null, 2)}\n`);
console.log(JSON.stringify({ schema: proof.schema, target, status: proof.status, output: resolve(output) }));
