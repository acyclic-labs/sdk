import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { loadMatrix, makePendingReceipt, validateReceipt } from "./graphcoder-qualification.mjs";

const matrix = loadMatrix();

test("the locked matrix has unique coverage for every requirement", () => {
  assert.equal(matrix.entries.length, 66);
  assert.equal(new Set(matrix.entries.map(entry => entry.id)).size, matrix.entries.length);
  assert.ok(matrix.entries.every(entry => entry.contract && entry.verification && entry.modes.length > 0));
});

test("a pending receipt is structurally valid but cannot be final", () => {
  const receipt = makePendingReceipt();
  const result = validateReceipt(matrix, receipt);
  assert.equal(result.requirements, 66);
  assert.equal(result.counts.missing, 66);
  assert.throws(() => validateReceipt(matrix, { ...receipt, gate: { ...receipt.gate, final: true } }, { final: true }), /required cases must be passed|missing/);
});

test("matrix digest prevents a receipt from silently changing its requirements", () => {
  const receipt = makePendingReceipt();
  const directory = mkdtempSync(join(tmpdir(), "graphcoder-qualification-"));
  const matrixCopy = join(directory, "requirements.json");
  const receiptPath = join(directory, "receipt.json");
  try {
    writeFileSync(matrixCopy, `${readFileSync("docs/graphcoder-swarm/requirements.json", "utf8")}\n`);
    writeFileSync(receiptPath, JSON.stringify({ ...receipt, matrix: { ...receipt.matrix, path: matrixCopy } }));
    assert.throws(() => validateReceipt(matrix, JSON.parse(readFileSync(receiptPath, "utf8"))), /locked matrix path/);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
