#!/usr/bin/env node

import { existsSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { basename, resolve } from "node:path";
import { readTarGzip, collectBody } from "./archive-utils.mjs";

const PACKAGE_NAME = "@acyclic-labs/plugin";
const VERSION_PATTERN = /^\d+\.\d+\.\d+(?:[+-][0-9A-Za-z.-]+)?$/;
const OBJECT_ID_PATTERN = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;

function fail(message) {
  throw new Error(message);
}

async function archiveEvidence(path) {
  let manifest;
  const evidence = await readTarGzip(path, async entry => {
    if (entry.path !== "package/package.json") return;
    if (manifest || entry.type !== "0") fail("plugin archive manifest is invalid");
    manifest = JSON.parse((await collectBody(entry)).toString("utf8"));
  }, { algorithm: "sha512", encoding: "base64" });
  if (manifest?.name !== PACKAGE_NAME || !VERSION_PATTERN.test(manifest?.version ?? "")) fail("plugin archive identity is invalid");
  return { manifest, evidence: { sha512: evidence.digest, size: evidence.size } };
}

function canonicalAsset(version) {
  return `acyclic-labs-plugin-${version}.tgz`;
}

function readReceipt(path) {
  const metadata = statSync(path);
  if (!metadata.isFile() || metadata.size <= 0) {
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

async function create(archiveArgument, receiptArgument, sourceSha) {
  if (!OBJECT_ID_PATTERN.test(sourceSha ?? "")) {
    fail("source commit must be a full lowercase Git object ID");
  }
  const archive = resolve(archiveArgument);
  const receipt = resolve(receiptArgument);
  if (existsSync(receipt)) fail("plugin qualification receipt already exists");
  const { manifest, evidence } = await archiveEvidence(archive);
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

async function verify(receiptArgument, sourceSha, archiveArgument, version) {
  if (!OBJECT_ID_PATTERN.test(sourceSha ?? "") || !VERSION_PATTERN.test(version ?? "")) {
    fail("verification identity is invalid");
  }
  const archive = resolve(archiveArgument);
  const receipt = readReceipt(resolve(receiptArgument));
  const { manifest, evidence } = await archiveEvidence(archive);
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
if (command === "create" && args.length === 3) await create(...args);
else if (command === "verify" && args.length === 4) await verify(...args);
else fail("usage: plugin-qualification.mjs create ARCHIVE RECEIPT SOURCE_SHA | verify RECEIPT SOURCE_SHA ARCHIVE VERSION");
