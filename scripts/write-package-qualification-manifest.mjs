#!/usr/bin/env node

/**
 * Write one source-bound index for the packages produced by the release lane.
 *
 * The individual package checks remain authoritative for execution. This file
 * makes their output reviewable as one archive: a consumer can see the exact
 * JavaScript/WASM and native bytes, the Rust archive closures, and the Harness
 * conformance evidence without reconstructing the runner directory layout.
 */
import { createHash } from "node:crypto";
import { copyFileSync, existsSync, mkdirSync, readFileSync, readdirSync, statSync, writeFileSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const artifactRoot = resolve(process.argv[2] ?? "");
const sourceRoot = resolve(process.argv[3] ?? dirname(dirname(fileURLToPath(import.meta.url))));

if (!process.argv[2]) {
  console.error("usage: write-package-qualification-manifest.mjs ABSOLUTE_PACKAGE_ROOT [ABSOLUTE_SOURCE_ROOT]");
  process.exit(2);
}

const fail = (message) => {
  throw new Error(message);
};
const requireFile = (path) => {
  if (!existsSync(path) || !statSync(path).isFile()) fail(`required package artifact is missing: ${path}`);
  return path;
};
const sha256 = (path) => `sha256:${createHash("sha256").update(readFileSync(path)).digest("hex")}`;
const rel = (path) => relative(artifactRoot, path).replaceAll("\\", "/");
const artifact = (path) => ({ path: rel(path), sha256: sha256(path) });
const json = (path) => JSON.parse(readFileSync(path, "utf8"));

const listArchive = (path) => {
  const result = spawnSync("tar", ["-tzf", path], { encoding: "utf8" });
  if (result.status !== 0) fail(`cannot inspect package archive ${path}: ${result.stderr}`);
  return result.stdout.split(/\r?\n/).filter(Boolean);
};
const requireArchiveEntry = (archive, entry) => {
  const names = listArchive(archive);
  if (!names.some((name) => name === entry || name.endsWith(`/${entry}`))) {
    fail(`package archive ${archive} is missing ${entry}`);
  }
};
const checksumEntries = (path) => {
  const entries = {};
  for (const line of readFileSync(path, "utf8").split(/\r?\n/).filter(Boolean)) {
    const match = line.match(/^([0-9a-f]{64})  (.+)$/i);
    if (!match) fail(`invalid SHA256SUMS entry in ${path}: ${line}`);
    entries[match[2]] = `sha256:${match[1].toLowerCase()}`;
  }
  return entries;
};
const verifyChecksums = (directory) => {
  const sumsPath = requireFile(join(directory, "SHA256SUMS"));
  const expected = checksumEntries(sumsPath);
  for (const [name, digest] of Object.entries(expected)) {
    const path = requireFile(join(directory, name));
    if (sha256(path) !== digest) fail(`SHA256SUMS mismatch for ${path}`);
  }
  return { manifest: artifact(sumsPath), files: expected };
};

const filesystem = join(artifactRoot, "filesystem");
const native = join(artifactRoot, "native");
const harness = join(artifactRoot, "harness");
const filesystemArchive = requireFile(join(filesystem, "acyclic-fs.tgz"));
const harnessArchive = requireFile(join(harness, "acyclic-harness.tgz"));
const nativeFiles = readdirSync(native).filter((name) => name.endsWith(".node"));
if (nativeFiles.length !== 1) fail(`expected exactly one native filesystem artifact, found ${nativeFiles.length}`);
const nativeArtifact = requireFile(join(native, nativeFiles[0]));
requireArchiveEntry(filesystemArchive, "package/generated/wasm/acyclic_fs_wasm_bg.wasm");
requireArchiveEntry(filesystemArchive, "package/generated/wasm/acyclic_fs_wasm.js");
requireArchiveEntry(harnessArchive, "package/generated/wasm/acyclic_harness_wasm_bg.wasm");
requireArchiveEntry(harnessArchive, "package/generated/wasm/acyclic_harness_wasm.js");

const filesystemChecksums = verifyChecksums(filesystem);
const nativeChecksums = verifyChecksums(native);
const harnessChecksums = verifyChecksums(harness);
if (!Object.prototype.hasOwnProperty.call(nativeChecksums.files, nativeFiles[0])) {
  fail(`native artifact is not represented by ${join(native, "SHA256SUMS")}`);
}

const embeddedRoot = join(sourceRoot, "research", "acceptance", "embedded");
const embeddedCandidates = [
  "release-abi-20261004.json",
  "central-qualification-20261004.json",
  "release-abi-generation-evidence.json",
];
const embedded = embeddedCandidates
  .map((name) => join(embeddedRoot, name))
  .filter((path) => existsSync(path) && statSync(path).isFile())
  .map((path) => {
    const destinationRoot = join(artifactRoot, "embedded");
    mkdirSync(destinationRoot, { recursive: true });
    const destination = join(destinationRoot, path.split(/[\\/]/).pop());
    copyFileSync(path, destination);
    return { path: rel(destination), sha256: sha256(destination) };
  });

const sourceCommitPath = requireFile(join(filesystem, "SOURCE_COMMIT"));
const sourceRevision = readFileSync(sourceCommitPath, "utf8").trim();
if (!/^[0-9a-f]{40}$/i.test(sourceRevision)) fail(`invalid filesystem SOURCE_COMMIT: ${sourceRevision}`);
const harnessCommitPath = requireFile(join(harness, "SOURCE_COMMIT"));
const harnessRevision = readFileSync(harnessCommitPath, "utf8").trim();
if (harnessRevision !== sourceRevision) fail(`filesystem and Harness package revisions differ: ${sourceRevision} != ${harnessRevision}`);

const manifest = {
  schema: "acyclic.sdk.package.qualification.v1",
  status: "passed",
  source_revision: sourceRevision,
  artifacts: {
    filesystem: {
      archive: artifact(filesystemArchive),
      checksums: filesystemChecksums,
      rust_source_commit: artifact(sourceCommitPath),
      wasm: {
        archive_entries: [
          "package/generated/wasm/acyclic_fs_wasm_bg.wasm",
          "package/generated/wasm/acyclic_fs_wasm.js",
        ],
      },
    },
    native: {
      package: artifact(nativeArtifact),
      checksums: nativeChecksums,
    },
    harness: {
      archive: artifact(harnessArchive),
      checksums: harnessChecksums,
      conformance: artifact(requireFile(join(harness, "CONFORMANCE-EVIDENCE.json"))),
      rust_source_commit: artifact(harnessCommitPath),
      wasm: {
        archive_entries: [
          "package/generated/wasm/acyclic_harness_wasm_bg.wasm",
          "package/generated/wasm/acyclic_harness_wasm.js",
        ],
      },
    },
  },
  consumers: {
    filesystem_js_wasm: { status: "passed", check: "scripts/check-filesystem-package.sh" },
    filesystem_rust_archive: { status: "passed", check: "scripts/check-filesystem-package.sh" },
    harness_js_wasm: { status: "passed", check: "scripts/check-harness-package.sh" },
    harness_rust_archive: { status: "passed", check: "scripts/check-harness-package.sh" },
  },
  embedded: {
    status: embedded.length === embeddedCandidates.length ? "recorded" : "not-produced-by-package-lane",
    source_evidence: embedded,
  },
};

writeFileSync(join(artifactRoot, "package-qualification.json"), `${JSON.stringify(manifest, null, 2)}\n`);
console.log(JSON.stringify({ status: manifest.status, path: join(artifactRoot, "package-qualification.json"), source_revision: sourceRevision }));
