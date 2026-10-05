#!/usr/bin/env node

// Build and validate the machine-readable qualification index. The gap audit
// remains the human-readable source for row state; this tool prevents drift
// between that audit and the 68 locked requirements.

import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

const MATRIX_PATH = "docs/graphcoder-swarm/requirements.json";
const AUDIT_PATH = "docs/graphcoder-swarm/MATRIX-GAP-AUDIT.md";
const VALID_STATUSES = new Set(["checkpoint", "partial", "unmet"]);

function fail(message) { throw new Error(`graphcoder-evidence-index: ${message}`); }

function readJson(path) {
  try { return JSON.parse(readFileSync(resolve(path), "utf8")); }
  catch (error) { fail(`could not read JSON ${path}: ${error instanceof Error ? error.message : String(error)}`); }
}

function auditStatuses(auditText) {
  const statuses = new Map();
  for (const match of auditText.matchAll(/^\| (checkpoint|partial|unmet) \| ([^|]+) \| ([^|]+) \|$/gmu)) {
    const status = match[1];
    for (const rawId of match[2].split(",")) {
      const id = rawId.trim();
      if (!/^[A-Z0-9]+-[0-9]{2}$/u.test(id)) fail(`audit contains invalid requirement id ${id}`);
      if (statuses.has(id)) fail(`audit assigns more than one status to ${id}`);
      statuses.set(id, { status, blocker: match[3].trim() });
    }
  }
  return statuses;
}

export function buildIndex({ matrixPath = MATRIX_PATH, auditPath = AUDIT_PATH } = {}) {
  const matrix = readJson(matrixPath);
  if (!Array.isArray(matrix.entries)) fail("matrix entries are missing");
  const statuses = auditStatuses(readFileSync(resolve(auditPath), "utf8"));
  const matrixIds = new Set(matrix.entries.map(entry => entry.id));
  if (statuses.size !== matrix.entries.length) fail(`audit has ${statuses.size} rows for ${matrix.entries.length} requirements`);
  for (const id of statuses.keys()) if (!matrixIds.has(id)) fail(`audit references unknown matrix row ${id}`);
  const entries = matrix.entries.map(entry => {
    const state = statuses.get(entry.id);
    if (state === undefined) fail(`audit has no state for ${entry.id}`);
    return {
      id: entry.id,
      area: entry.area,
      status: state.status,
      contract: entry.contract,
      verification: entry.verification,
      required_execution_kinds: Array.isArray(entry.modes) ? [...entry.modes] : String(entry.modes).split(/\s+/u).filter(Boolean),
      evidence_kind: entry.evidence,
      evidence_refs: ["docs/graphcoder-swarm/requirements.json", auditPath, "docs/graphcoder-swarm/EVIDENCE.md"],
      prerequisite: state.blocker,
    };
  });
  return {
    protocol: "acyclic.graphcoder.evidence-index.v1",
    matrix: { path: matrixPath, protocol: matrix.protocol, requirements: entries.length },
    policy: "checkpoint and partial rows are not final qualification; unmet rows block completion",
    entries,
  };
}

export function validateIndex(index, { matrixPath = MATRIX_PATH, auditPath = AUDIT_PATH } = {}) {
  if (!index || typeof index !== "object" || index.protocol !== "acyclic.graphcoder.evidence-index.v1") fail("index protocol is invalid");
  const expected = buildIndex({ matrixPath, auditPath });
  if (!Array.isArray(index.entries) || index.entries.length !== expected.entries.length) fail("index does not contain every matrix entry");
  const expectedById = new Map(expected.entries.map(entry => [entry.id, entry]));
  const seen = new Set();
  for (const entry of index.entries) {
    if (!entry || typeof entry.id !== "string" || seen.has(entry.id)) fail(`index contains duplicate or invalid id ${entry?.id}`);
    seen.add(entry.id);
    const expectedEntry = expectedById.get(entry.id);
    if (expectedEntry === undefined) fail(`index references unknown matrix row ${entry.id}`);
    if (entry.status !== expectedEntry.status) fail(`${entry.id} status differs from the locked audit`);
    if (!Array.isArray(entry.evidence_refs) || entry.evidence_refs.length < 3) fail(`${entry.id} is missing evidence references`);
    for (const reference of entry.evidence_refs) if (!existsSync(resolve(reference))) fail(`${entry.id} evidence reference is missing: ${reference}`);
    if (typeof entry.prerequisite !== "string" || entry.prerequisite.trim() === "") fail(`${entry.id} has no prerequisite statement`);
  }
  return { requirements: expected.entries.length, statuses: Object.fromEntries([...new Set(expected.entries.map(entry => entry.status))].map(status => [status, expected.entries.filter(entry => entry.status === status).length])) };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const [command, outputPath] = process.argv.slice(2);
    const index = buildIndex();
    if (command === "write" && outputPath !== undefined && process.argv.length === 4) {
      validateIndex(index);
      writeFileSync(resolve(outputPath), `${JSON.stringify(index, null, 2)}\n`, { flag: "wx" });
      process.stdout.write(`${JSON.stringify({ output: resolve(outputPath), requirements: index.entries.length }, null, 2)}\n`);
    } else if (command === "check" && outputPath !== undefined && process.argv.length === 4) {
      process.stdout.write(`${JSON.stringify(validateIndex(readJson(outputPath)), null, 2)}\n`);
    } else fail("usage: graphcoder-evidence-index.mjs write|check OUTPUT.json");
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}
