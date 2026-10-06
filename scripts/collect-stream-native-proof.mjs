#!/usr/bin/env node

// Collect immutable package build evidence for every release target. This
// records a build and archive proof; it deliberately leaves RPC qualification
// pending until an installed consumer has exercised the complete scenario log.
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync, existsSync, mkdirSync } from "node:fs";
import { resolve, join } from "node:path";

const args = process.argv.slice(2);
const value = (name) => {
  const index = args.indexOf(name);
  return index < 0 ? undefined : args[index + 1];
};
const packageRoot = value("--package-root");
const archive = value("--archive");
const target = value("--target");
const sourceRevision = value("--source-revision");
const output = value("--output");
const installedConsumer = value("--installed-consumer");
const qualificationMetadata = value("--qualification-metadata");
// This is the canonical descriptor emitted from the Rust contract model. The
// archived handshake descriptor is intentionally a separate compatibility
// fixture and must not be used to define the qualification surface.
const streamDescriptor = value("--stream-descriptor");
if (!packageRoot || !archive || !target || !sourceRevision || !output || !qualificationMetadata || !streamDescriptor) {
  throw new Error("usage: collect-stream-native-proof.mjs --package-root DIR --archive FILE --target ID --source-revision OID --qualification-metadata FILE --stream-descriptor FILE --output FILE [--installed-consumer FILE]");
}
if (!/^[0-9a-f]{40}$/i.test(sourceRevision)) throw new Error("source revision must be an immutable Git OID");

const sha256 = (path) => `sha256:${createHash("sha256").update(readFileSync(path)).digest("hex")}`;
const root = resolve(packageRoot);
const manifestPath = join(root, "package.json");
const buildPath = join(root, "BUILD.json");
const loaderPath = join(root, "index.js");
const binaryPath = join(root, "acyclic_stream_native.node");
const metadataPath = resolve(qualificationMetadata);
const descriptorPath = resolve(streamDescriptor);
for (const path of [manifestPath, buildPath, loaderPath, binaryPath, archive, metadataPath, descriptorPath]) {
  if (!existsSync(path)) throw new Error(`missing native package proof input: ${path}`);
}
const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
const build = JSON.parse(readFileSync(buildPath, "utf8"));
if (build.schema !== "acyclic.sdk.stream.native.build.v1") throw new Error("native BUILD.json schema mismatch");
if (build.source_revision !== sourceRevision) throw new Error("native BUILD.json source revision differs from release source");
if (build.package?.target !== target) throw new Error("native BUILD.json target differs from matrix target");
if (build.package?.name !== manifest.name || build.package?.version !== manifest.version) {
  throw new Error("native BUILD.json package identity differs from package.json");
}
const qualification = JSON.parse(readFileSync(metadataPath, "utf8"));
if (qualification.schema !== "acyclic.sdk.stream.native.qualification-requirement.v1") {
  throw new Error("Stream qualification metadata schema mismatch");
}
if (qualification.source_revision !== sourceRevision) {
  throw new Error("Stream qualification metadata source revision differs from release source");
}
if (qualification.family !== "stream" || qualification.package !== "acyclic.stream.v2" || qualification.service !== "StreamService") {
  throw new Error("Stream qualification metadata family does not match the canonical Stream contract");
}
if (qualification.source !== "acyclic-sdk-contract-wire::stream") {
  throw new Error("Stream qualification metadata is not Rust-generated");
}
if (!/^sha256:[0-9a-f]{64}$/i.test(qualification.descriptor_sha256)) {
  throw new Error("Stream qualification metadata descriptor hash is missing or malformed");
}
if (qualification.descriptor_sha256 !== sha256(descriptorPath)) {
  throw new Error("Stream qualification metadata is stale for the supplied Rust descriptor");
}
if (!Array.isArray(qualification.required_rpcs) || qualification.required_rpcs.length === 0) {
  throw new Error("Stream qualification metadata required_rpcs is empty");
}
const requiredRpcs = qualification.required_rpcs;
if (new Set(requiredRpcs).size !== requiredRpcs.length || requiredRpcs.some((rpc) =>
  typeof rpc !== "string" || !/^acyclic\.stream\.v2\.StreamService\/[A-Za-z][A-Za-z0-9]*$/.test(rpc))) {
  throw new Error("Stream qualification metadata contains duplicate or foreign RPC identities");
}
const proof = {
  schema: "acyclic.sdk.stream.native.package-proof.v1",
  source_revision: sourceRevision,
  target,
  status: "built",
  qualification: "pending-installed-consumer-rpc-scenarios",
  package: {
    name: manifest.name,
    version: manifest.version,
    root,
    archive,
    archive_sha256: sha256(archive),
    build_sha256: sha256(buildPath),
    manifest_sha256: sha256(manifestPath),
    loader_sha256: sha256(loaderPath),
    binary_sha256: sha256(binaryPath),
  },
  installed_consumer: installedConsumer
    ? { path: resolve(installedConsumer), receipt_sha256: sha256(installedConsumer), observed: true }
    : { observed: false },
  qualification_requirement: {
    receipt_tool: "sdk-stream-native-receipt",
    source: qualification.source,
    metadata_path: metadataPath,
    metadata_sha256: sha256(metadataPath),
    descriptor_path: descriptorPath,
    descriptor_sha256: qualification.descriptor_sha256,
    required_rpcs: requiredRpcs,
  },
};
mkdirSync(resolve(output, ".."), { recursive: true });
writeFileSync(resolve(output), `${JSON.stringify(proof, null, 2)}\n`);
console.log(JSON.stringify({ schema: proof.schema, target, status: proof.status, output: resolve(output) }));
