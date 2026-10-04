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
if (!packageRoot || !archive || !target || !sourceRevision || !output) {
  throw new Error("usage: collect-stream-native-proof.mjs --package-root DIR --archive FILE --target ID --source-revision OID --output FILE [--installed-consumer FILE]");
}
if (!/^[0-9a-f]{40}$/i.test(sourceRevision)) throw new Error("source revision must be an immutable Git OID");

const sha256 = (path) => `sha256:${createHash("sha256").update(readFileSync(path)).digest("hex")}`;
const root = resolve(packageRoot);
const manifestPath = join(root, "package.json");
const buildPath = join(root, "BUILD.json");
const loaderPath = join(root, "index.js");
const binaryPath = join(root, "acyclic_stream_native.node");
for (const path of [manifestPath, buildPath, loaderPath, binaryPath, archive]) {
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
    required_rpcs: [
      "acyclic.stream.v2.StreamService/Append",
      "acyclic.stream.v2.StreamService/Read",
      "acyclic.stream.v2.StreamService/InspectIdempotency",
      "acyclic.stream.v2.StreamService/Commit",
      "acyclic.stream.v2.StreamService/ReadCommit",
      "acyclic.stream.v2.StreamService/ChildrenPage",
      "acyclic.stream.v2.StreamService/Follow",
      "acyclic.stream.v2.StreamService/Tail",
    ],
  },
};
mkdirSync(resolve(output, ".."), { recursive: true });
writeFileSync(resolve(output), `${JSON.stringify(proof, null, 2)}\n`);
console.log(JSON.stringify({ schema: proof.schema, target, status: proof.status, output: resolve(output) }));
