#!/usr/bin/env node

import { readFileSync } from "node:fs";

function successfulCommand(value) {
  return value && value.exitCode === 0 && typeof value.command === "string" && value.command.length > 0 && !/artifact present/i.test(value.command);
}

export function verifyQualificationSummary(summary, { requireExecution = true } = {}) {
  const errors = [];
  const digest = /^sha256:[a-f0-9]{64}$/;
  const artifactDigest = /^(?:sha256:)?[a-f0-9]{64}$/;
  const identities = new Set();
  if (summary?.schema !== "acyclic.sdk.guide-projection-qualification.v1") errors.push("schema");
  if (summary?.projection_count !== 54) errors.push("projection_count");
  if (typeof summary?.source_revision !== "string" || summary.source_revision !== `source-sha256:${summary.source_sha256}`) errors.push("source_revision");
  if (typeof summary?.source_sha256 !== "string" || !digest.test(summary.source_sha256)) errors.push("source_sha256");
  const receipts = Array.isArray(summary?.receipts) ? summary.receipts : [];
  if (receipts.length !== 54) errors.push("receipts");
  const sourceSha256 = summary?.source_sha256;
  receipts.forEach((receipt, index) => {
    const prefix = `receipt[${index}]`;
    const scenario = receipt.scenario_id;
    const language = receipt.language;
    if (typeof scenario !== "string" || !scenario.trim() || typeof language !== "string" || !language.trim()) errors.push(`${prefix}.identity`);
    const identity = JSON.stringify([scenario, language]);
    if (identities.has(identity)) errors.push(`${prefix}.duplicate`);
    identities.add(identity);
    if (receipt.source_revision !== summary.source_revision) errors.push(`${prefix}.source_revision`);
    if (receipt.install_status !== "installed" || !successfulCommand(receipt.install)) errors.push(`${prefix}.install`);
    if (!successfulCommand(receipt.compile)) errors.push(`${prefix}.compile`);
    if (requireExecution && (receipt.status !== "executed" || !successfulCommand(receipt.execution))) errors.push(`${prefix}.execution`);
    if (receipt.source_sha256 !== sourceSha256) errors.push(`${prefix}.source_sha256`);
    if (typeof receipt.package_artifact !== "string" || !receipt.package_artifact.trim() || typeof receipt.package_sha256 !== "string" || !artifactDigest.test(receipt.package_sha256)) errors.push(`${prefix}.package`);
    if (typeof receipt.qualification?.install !== "string" || typeof receipt.qualification?.compile !== "string" || typeof receipt.qualification?.execute !== "string") errors.push(`${prefix}.recipe`);
  });
  return { valid: errors.length === 0, errors };
}

if (process.argv[1] && new URL(`file://${process.argv[1].replaceAll("\\", "/")}`).href === import.meta.url) {
  const path = process.argv[2];
  if (!path) throw new Error("usage: verify-guide-projection-receipts.mjs qualification.json");
  const summary = JSON.parse(readFileSync(path, "utf8"));
  const result = verifyQualificationSummary(summary);
  console.log(JSON.stringify(result, null, 2));
  if (!result.valid) process.exit(1);
}
