#!/usr/bin/env node
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";
import { resolve } from "node:path";

function successfulCommand(value) {
  return value && value.exitCode === 0 && typeof value.command === "string" && value.command.length > 0 && !/artifact present/i.test(value.command);
}
const identity = value => JSON.stringify([value?.scenario_id, value?.language]);
const hash = value => createHash("sha256").update(value).digest("hex");

export function verifyQualificationSummary(summary, { requireExecution = true, expectedProjections, readArtifact } = {}) {
  const errors = [];
  const digest = /^sha256:[a-f0-9]{64}$/;
  const artifactDigest = /^(?:sha256:)?[a-f0-9]{64}$/;
  const identities = new Set();
  const expected = new Map();
  if (!Array.isArray(expectedProjections) || expectedProjections.length !== 54) errors.push("projection_manifest");
  for (const projection of expectedProjections ?? []) {
    const key = identity(projection);
    if (expected.has(key)) errors.push("projection_manifest.duplicate");
    expected.set(key, projection);
    if (projection.source_sha256 !== summary?.source_sha256 || typeof projection.code !== "string") errors.push("projection_manifest.source");
  }
  if (summary?.schema !== "acyclic.sdk.guide-projection-qualification.v1") errors.push("schema");
  if (summary?.projection_count !== 54) errors.push("projection_count");
  if (typeof summary?.source_revision !== "string" || summary.source_revision !== `source-sha256:${summary.source_sha256}`) errors.push("source_revision");
  if (typeof summary?.source_sha256 !== "string" || !digest.test(summary.source_sha256)) errors.push("source_sha256");
  const receipts = Array.isArray(summary?.receipts) ? summary.receipts : [];
  if (receipts.length !== 54) errors.push("receipts");
  receipts.forEach((receipt, index) => {
    const prefix = `receipt[${index}]`;
    const scenario = receipt.scenario_id;
    const language = receipt.language;
    if (typeof scenario !== "string" || !scenario.trim() || typeof language !== "string" || !language.trim()) errors.push(`${prefix}.identity`);
    const key = identity(receipt);
    if (identities.has(key)) errors.push(`${prefix}.duplicate`);
    identities.add(key);
    const projection = expected.get(key);
    if (!projection) errors.push(`${prefix}.unexpected_projection`);
    else {
      for (const field of ["family", "operation", "mode", "source", "package_manager", "package_name", "artifact_path"]) {
        if (typeof projection[field] !== "string" || receipt[field] !== projection[field]) errors.push(`${prefix}.${field}`);
      }
      for (const phase of ["install", "compile", "execute"]) {
        if (!projection.qualification?.[phase] || receipt.qualification?.[phase] !== projection.qualification[phase]) errors.push(`${prefix}.recipe.${phase}`);
      }
      if (typeof projection.code !== "string" || receipt.snippet_sha256 !== hash(projection.code)) errors.push(`${prefix}.snippet_sha256`);
    }
    if (receipt.source_revision !== summary.source_revision) errors.push(`${prefix}.source_revision`);
    if (receipt.source_sha256 !== summary.source_sha256) errors.push(`${prefix}.source_sha256`);
    if (receipt.install_status !== "installed" || !successfulCommand(receipt.install)) errors.push(`${prefix}.install`);
    if (!successfulCommand(receipt.compile)) errors.push(`${prefix}.compile`);
    if (requireExecution && (receipt.status !== "executed" || !successfulCommand(receipt.execution))) errors.push(`${prefix}.execution`);
    if (typeof receipt.package_artifact !== "string" || !receipt.package_artifact.trim() || typeof receipt.package_sha256 !== "string" || !artifactDigest.test(receipt.package_sha256)) errors.push(`${prefix}.package`);
    if (readArtifact) {
      try {
        const bytes = readArtifact(receipt.package_artifact);
        if (hash(bytes) !== receipt.package_sha256?.replace(/^sha256:/, "")) errors.push(`${prefix}.package_bytes`);
      } catch {
        errors.push(`${prefix}.package_bytes`);
      }
    }
  });
  for (const key of expected.keys()) if (!identities.has(key)) errors.push("missing_projection:" + key);
  return { valid: errors.length === 0, errors };
}

if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
  const [path, manifestPath, artifactRoot = process.cwd()] = process.argv.slice(2);
  if (!path || !manifestPath) throw new Error("usage: verify-guide-projection-receipts.mjs qualification.json rust-projections.json [artifact-root]");
  const summary = JSON.parse(readFileSync(path, "utf8"));
  const expectedProjections = JSON.parse(readFileSync(manifestPath, "utf8"));
  const result = verifyQualificationSummary(summary, { expectedProjections, readArtifact: path => readFileSync(resolve(artifactRoot, path)) });
  console.log(JSON.stringify(result, null, 2));
  if (!result.valid) process.exit(1);
}