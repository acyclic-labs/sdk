#!/usr/bin/env node

// Assemble suite records into the locked receipt shape. Every matrix ID is
// emitted, including still-pending rows; final validation remains responsible
// for refusing an incomplete receipt.

import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { loadMatrix, makePendingReceipt, validateReceipt } from "./graphcoder-qualification.mjs";

function fail(message) {
  throw new Error(`assemble-graphcoder-receipt: ${message}`);
}

function readJson(path) {
  return JSON.parse(readFileSync(resolve(path), "utf8"));
}

function nonempty(value, label) {
  if (typeof value !== "string" || value.trim() === "") fail(`${label} must be nonempty`);
  return value;
}

function loadRecords(paths) {
  const suites = [];
  const artifacts = new Map();
  const suiteIds = new Set();
  for (const [index, path] of paths.entries()) {
    const record = readJson(nonempty(path, `suite_records[${index}]`));
    if (!record || typeof record !== "object" || !record.suite || !Array.isArray(record.artifacts)) fail(`suite record ${path} is invalid`);
    const suite = record.suite;
    if (suiteIds.has(suite.id)) fail(`duplicate suite id ${suite.id}`);
    suiteIds.add(suite.id);
    suites.push(suite);
    for (const artifact of record.artifacts) {
      if (!artifact || typeof artifact !== "object" || typeof artifact.path !== "string") fail(`suite record ${path} contains an invalid artifact`);
      const existing = artifacts.get(artifact.path);
      if (existing !== undefined && JSON.stringify(existing) !== JSON.stringify(artifact)) fail(`artifact ${artifact.path} has conflicting suite records`);
      artifacts.set(artifact.path, artifact);
    }
  }
  return { suites, artifacts: [...artifacts.values()] };
}

function caseEvidence(bindings, suiteById) {
  const evidence = [];
  for (const suiteId of bindings) {
    const suite = suiteById.get(suiteId);
    if (suite === undefined) fail(`case binding references unknown suite ${suiteId}`);
    evidence.push({
      suite: suite.id,
      descriptor_sha256: suite.descriptor_sha256,
      execution_kind: suite.execution_kind,
      artifact_paths: [...suite.artifact_paths],
    });
  }
  return evidence;
}

function caseStatus(evidence, suiteById) {
  if (evidence.length === 0) return "pending";
  const statuses = evidence.map(item => suiteById.get(item.suite).status);
  if (statuses.includes("failed")) return "failed";
  if (statuses.includes("flaky")) return "flaky";
  if (statuses.includes("skipped")) return "skipped";
  return "passed";
}

export function assemble(configPath, { gitOps } = {}) {
  const config = readJson(configPath);
  if (!config || typeof config !== "object") fail("config must be an object");
  const matrixPath = config.matrix ?? "docs/graphcoder-swarm/requirements.json";
  const matrix = loadMatrix(matrixPath);
  const records = loadRecords(config.suite_records ?? []);
  const suiteById = new Map(records.suites.map(suite => [suite.id, suite]));
  const bindings = new Map();
  for (const [index, binding] of (config.cases ?? []).entries()) {
    if (!binding || typeof binding !== "object") fail(`cases[${index}] is invalid`);
    const id = nonempty(binding.id, `cases[${index}].id`);
    if (bindings.has(id)) fail(`duplicate case binding ${id}`);
    const suites = binding.suites ?? (binding.suite === undefined ? [] : [binding.suite]);
    if (!Array.isArray(suites) || suites.some(suite => typeof suite !== "string" || suite.trim() === "")) fail(`cases[${index}].suites must be nonempty strings`);
    bindings.set(id, suites);
  }
  const receipt = makePendingReceipt(matrixPath, gitOps === undefined ? {} : { gitOps });
  receipt.suites = records.suites;
  receipt.artifacts = records.artifacts;
  receipt.cases = matrix.entries.map(entry => {
    const suites = bindings.get(entry.id) ?? [];
    const evidence = caseEvidence(suites, suiteById);
    return { id: entry.id, status: caseStatus(evidence, suiteById), evidence };
  });
  receipt.gate = {
    final: config.final === true,
    failed: receipt.cases.filter(record => record.status === "failed").length,
    skipped: receipt.cases.filter(record => record.status === "skipped").length,
    flaky: receipt.cases.filter(record => record.status === "flaky").length,
    missing: receipt.cases.filter(record => record.status === "pending" || record.status === "not-run").length,
  };
  validateReceipt(matrix, receipt, gitOps === undefined ? { final: false, matrixPath } : { final: false, matrixPath, gitOps });
  const output = nonempty(config.output, "output");
  writeFileSync(resolve(output), `${JSON.stringify(receipt, null, 2)}\n`, { flag: "wx" });
  process.stdout.write(`${JSON.stringify({ output: resolve(output), requirements: matrix.entries.length, suites: receipt.suites.length, artifacts: receipt.artifacts.length, pending: receipt.gate.missing }, null, 2)}\n`);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const [command, configPath] = process.argv.slice(2);
    if (command !== "assemble" || configPath === undefined || process.argv.length !== 4) fail("usage: assemble-graphcoder-receipt.mjs assemble CONFIG.json");
    assemble(configPath);
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}
