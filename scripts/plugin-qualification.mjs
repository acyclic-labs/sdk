#!/usr/bin/env node

import { createHash } from "node:crypto";
import { existsSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { basename, resolve } from "node:path";
import { spawnSync } from "node:child_process";

const MAX_RECEIPT_BYTES = 65_536;
const PACKAGE_NAME = "@acyclic-labs/plugin";
const VERSION_PATTERN = /^\d+\.\d+\.\d+(?:[+-][0-9A-Za-z.-]+)?$/;
const OBJECT_ID_PATTERN = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;

function fail(message) {
  throw new Error(message);
}

function archiveManifest(path) {
  const result = spawnSync("tar", ["-xOf", path, "package/package.json"], {
    encoding: "utf8",
    maxBuffer: 1024 * 1024,
  });
  if (result.error || result.status !== 0) {
    fail(`could not read plugin archive manifest: ${result.error ?? result.stderr}`);
  }
  const manifest = JSON.parse(result.stdout);
  if (manifest.name !== PACKAGE_NAME || !VERSION_PATTERN.test(manifest.version ?? "")) {
    fail("plugin archive identity is invalid");
  }
  return manifest;
}

function archiveEvidence(path) {
  const bytes = readFileSync(path);
  return {
    sha512: createHash("sha512").update(bytes).digest("base64"),
    size: bytes.length,
  };
}

function canonicalAsset(version) {
  return `acyclic-labs-plugin-${version}.tgz`;
}

function readReceipt(path) {
  const metadata = statSync(path);
  if (!metadata.isFile() || metadata.size <= 0 || metadata.size > MAX_RECEIPT_BYTES) {
    fail("plugin qualification receipt size is invalid");
  }
  const receipt = JSON.parse(readFileSync(path, "utf8"));
  if (
    !receipt || typeof receipt !== "object" || Array.isArray(receipt)
    || Object.keys(receipt).sort().join() !== "package,revision,source_commit"
    || receipt.revision !== 1
    || typeof receipt.source_commit !== "string"
    || !OBJECT_ID_PATTERN.test(receipt.source_commit)
    || !receipt.package || typeof receipt.package !== "object" || Array.isArray(receipt.package)
    || Object.keys(receipt.package).sort().join() !== "asset,name,sha512,size,version"
    || receipt.package.name !== PACKAGE_NAME
    || typeof receipt.package.version !== "string" || !VERSION_PATTERN.test(receipt.package.version)
    || receipt.package.asset !== canonicalAsset(receipt.package.version)
    || typeof receipt.package.sha512 !== "string"
    || !/^[A-Za-z0-9+/]{86}==$/.test(receipt.package.sha512)
    || !Number.isInteger(receipt.package.size) || receipt.package.size <= 0
  ) {
    fail("plugin qualification receipt schema is invalid");
  }
  return receipt;
}

function create(archiveArgument, receiptArgument, sourceSha) {
  if (!OBJECT_ID_PATTERN.test(sourceSha ?? "")) {
    fail("source commit must be a full lowercase Git object ID");
  }
  const archive = resolve(archiveArgument);
  const receipt = resolve(receiptArgument);
  if (existsSync(receipt)) fail("plugin qualification receipt already exists");
  const manifest = archiveManifest(archive);
  const evidence = archiveEvidence(archive);
  writeFileSync(receipt, `${JSON.stringify({
    revision: 1,
    source_commit: sourceSha,
    package: {
      asset: canonicalAsset(manifest.version),
      name: manifest.name,
      version: manifest.version,
      ...evidence,
    },
  })}\n`);
}

function verify(receiptArgument, sourceSha, archiveArgument, version) {
  if (!OBJECT_ID_PATTERN.test(sourceSha ?? "") || !VERSION_PATTERN.test(version ?? "")) {
    fail("verification identity is invalid");
  }
  const archive = resolve(archiveArgument);
  const receipt = readReceipt(resolve(receiptArgument));
  const manifest = archiveManifest(archive);
  const evidence = archiveEvidence(archive);
  if (receipt.source_commit !== sourceSha) fail("plugin receipt belongs to another source commit");
  if (manifest.name !== PACKAGE_NAME || manifest.version !== version) fail("plugin release identity differs");
  if (basename(archive) !== canonicalAsset(version)) fail("plugin publication asset name is not canonical");
  if (
    receipt.package.asset !== basename(archive)
    || receipt.package.version !== version
    || receipt.package.sha512 !== evidence.sha512
    || receipt.package.size !== evidence.size
  ) {
    fail("plugin archive bytes differ from the qualified artifact");
  }
}

const [command, ...args] = process.argv.slice(2);
if (command === "create" && args.length === 3) create(...args);
else if (command === "verify" && args.length === 4) verify(...args);
else fail("usage: plugin-qualification.mjs create ARCHIVE RECEIPT SOURCE_SHA | verify RECEIPT SOURCE_SHA ARCHIVE VERSION");
