#!/usr/bin/env node
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { pathToFileURL } from "node:url";
import { resolve } from "node:path";
import { tarEntries } from "./archive-utils.mjs";

function successfulCommand(value) {
  return value && value.exitCode === 0 && typeof value.command === "string" && value.command.length > 0 && !/artifact present/i.test(value.command);
}
const identity = value => JSON.stringify([value?.scenario_id, value?.language]);
const hash = value => createHash("sha256").update(value).digest("hex");
const archivePattern = /\.(?:tgz|tar\.gz|crate)$/i;
const embeddedHarnessOperation = "acyclic_sdk_examples::harness_scenarios::execute_harness_scenario";

function sourceCheckoutPath(path) {
  const normalized = path.replaceAll("\\", "/").toLowerCase();
  return normalized.includes("/rust/crates/") || normalized.includes("/typescript/packages/");
}

function normalizedArchivePath(path) {
  const normalized = path.replaceAll("\\", "/").replace(/^\.\//, "");
  if (!normalized || normalized.startsWith("/") || /^[A-Za-z]:\//.test(normalized)) throw new Error("package archive contains an absolute member path");
  const parts = normalized.split("/").filter(Boolean);
  if (parts.some(part => part === "..")) throw new Error("package archive contains an unsafe member path");
  return parts.join("/");
}

function archiveTreeDigest(bytes) {
  const expanded = gunzipSync(bytes, { maxOutputLength: 1024 * 1024 * 1024 });
  const entries = tarEntries(expanded)
    .filter(entry => entry.type === "0" || entry.type === "\0")
    .map(entry => ({ path: normalizedArchivePath(entry.path), body: entry.body }));
  if (!entries.length) throw new Error("package archive contains no regular files");
  const first = entries[0].path.split("/")[0];
  const prefix = first && entries.every(entry => entry.path !== first && entry.path.startsWith(`${first}/`)) ? `${first}/` : "";
  const seen = new Set();
  const digest = createHash("sha256");
  const files = entries.map(entry => ({ path: prefix ? entry.path.slice(prefix.length) : entry.path, body: entry.body }));
  for (const entry of files) {
    if (!entry.path || seen.has(entry.path)) throw new Error("package archive contains duplicate members");
    seen.add(entry.path);
  }
  for (const entry of [...files].sort((left, right) => left.path.localeCompare(right.path))) {
    digest.update(entry.path);
    digest.update(Buffer.from([0]));
    digest.update(entry.body);
    digest.update(Buffer.from([0]));
  }
  for (const entry of tarEntries(expanded)) {
    if (entry.type !== "0" && entry.type !== "\0" && entry.type !== "5") throw new Error("package archive contains an unsupported member type");
  }
  return `sha256:${digest.digest("hex")}`;
}

export function verifyQualificationSummary(summary, {
  requireExecution = true,
  expectedProjections,
  expectedProjectionCount = 54,
  readArtifact,
} = {}) {
  const errors = [];
  const digest = /^sha256:[a-f0-9]{64}$/;
  const artifactDigest = /^(?:sha256:)?[a-f0-9]{64}$/;
  const identities = new Set();
  const expected = new Map();
  if (!Array.isArray(expectedProjections) || expectedProjections.length !== expectedProjectionCount) errors.push("projection_manifest");
  for (const projection of expectedProjections ?? []) {
    const key = identity(projection);
    if (expected.has(key)) errors.push("projection_manifest.duplicate");
    expected.set(key, projection);
    if (projection.source_sha256 !== summary?.source_sha256 || typeof projection.code !== "string") errors.push("projection_manifest.source");
  }
  if (summary?.schema !== "acyclic.sdk.guide-projection-qualification.v1") errors.push("schema");
  if (summary?.projection_count !== expectedProjectionCount) errors.push("projection_count");
  const expectedSourceRevision = typeof summary?.source_sha256 === "string"
    ? `source-sha256:${summary.source_sha256.replace(/^sha256:/, "")}`
    : null;
  if (typeof summary?.source_revision !== "string" || summary.source_revision !== expectedSourceRevision) errors.push("source_revision");
  if (typeof summary?.source_sha256 !== "string" || !digest.test(summary.source_sha256)) errors.push("source_sha256");
  const receipts = Array.isArray(summary?.receipts) ? summary.receipts : [];
  if (receipts.length !== expectedProjectionCount) errors.push("receipts");
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
      if (projection.family === "harness" && ["rust", "typescript"].includes(projection.language)) {
        if (projection.mode !== "embedded" || projection.operation !== embeddedHarnessOperation) errors.push(`${prefix}.embedded_harness_identity`);
        if (projection.language === "typescript") {
          if (!projection.code.includes("Harness.create") || !projection.code.includes("issueScope")) errors.push(`${prefix}.embedded_harness_facade`);
          if (projection.code.includes("CommandEnvelope")) errors.push(`${prefix}.embedded_harness_remote_marker`);
        }
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
    if (typeof receipt.package_artifact !== "string" || !receipt.package_artifact.trim() || !archivePattern.test(receipt.package_artifact) || sourceCheckoutPath(receipt.package_artifact)) errors.push(`${prefix}.package`);
    if (typeof receipt.package_sha256 !== "string" || !artifactDigest.test(receipt.package_sha256)) errors.push(`${prefix}.package_sha256`);
    if (typeof receipt.package_archive_sha256 !== "string" || receipt.package_archive_sha256 !== receipt.package_sha256 || !artifactDigest.test(receipt.package_archive_sha256)) errors.push(`${prefix}.package_archive_sha256`);
    if (receipt.package_archive_format !== "gzip+ustar") errors.push(`${prefix}.package_archive_format`);
    if (typeof receipt.package_tree_sha256 !== "string" || !artifactDigest.test(receipt.package_tree_sha256)) errors.push(`${prefix}.package_tree_sha256`);
    if (typeof receipt.resolved_package_root !== "string" || !receipt.resolved_package_root.trim() || sourceCheckoutPath(receipt.resolved_package_root)) errors.push(`${prefix}.resolved_package_root`);
    if (readArtifact) {
      try {
        const bytes = readArtifact(receipt.package_artifact);
        if (hash(bytes) !== receipt.package_sha256?.replace(/^sha256:/, "")) errors.push(`${prefix}.package_bytes`);
        if (archiveTreeDigest(bytes) !== receipt.package_tree_sha256) errors.push(`${prefix}.package_tree_bytes`);
      } catch {
        errors.push(`${prefix}.package_bytes`);
      }
    }
  });
  for (const key of expected.keys()) if (!identities.has(key)) errors.push("missing_projection:" + key);
  return { valid: errors.length === 0, errors };
}

if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
  const [path, manifestPath, artifactRoot = process.cwd()] = process.argv.slice(2).filter(value => !value.startsWith("--"));
  const expectedProjectionCountArg = process.argv.find(value => value.startsWith("--expected-count="))?.split("=", 2)[1];
  const expectedProjectionCount = expectedProjectionCountArg ? Number(expectedProjectionCountArg) : undefined;
  const requireExecution = !process.argv.includes("--no-execution");
  if (!path || !manifestPath) throw new Error("usage: verify-guide-projection-receipts.mjs qualification.json rust-projections.json [artifact-root] [--expected-count=N] [--no-execution]");
  const summary = JSON.parse(readFileSync(path, "utf8"));
  const expectedProjections = JSON.parse(readFileSync(manifestPath, "utf8"));
  const result = verifyQualificationSummary(summary, { expectedProjections, expectedProjectionCount, requireExecution, readArtifact: path => readFileSync(resolve(artifactRoot, path)) });
  console.log(JSON.stringify(result, null, 2));
  if (!result.valid) process.exit(1);
}
