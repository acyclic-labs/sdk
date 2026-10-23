// The roadmap is a release contract, not a count of green component tests.
import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export const rows = Array.from({ length: 26 }, (_, index) => `A${index + 1}`);
const directory = dirname(fileURLToPath(import.meta.url));

export function inventory(root = directory) {
  return readdirSync(join(root, "cases")).filter(name => name.endsWith(".json")).sort()
    .map(name => JSON.parse(readFileSync(join(root, "cases", name), "utf8")));
}

export function coverage(manifest, cases) {
  assert.equal(manifest.version, 1, "unsupported coverage manifest version");
  assert.deepEqual(Object.keys(manifest.rows).sort(), [...rows].sort(), "coverage must declare exactly A1–A26");
  const ids = cases.map(test => test.id);
  assert.ok(ids.every(id => typeof id === "string" && id.length), "every case needs an identity");
  assert.equal(new Set(ids).size, ids.length, "case identities must be unique");
  const known = new Set(ids), mapped = new Set(), missing = [], gaps = [];
  for (const row of rows) {
    const entry = manifest.rows[row];
    assert.ok(entry && typeof entry === "object", `${row}: missing coverage entry`);
    assert.ok(typeof entry.target === "string" && entry.target.length, `${row}: missing target`);
    assert.ok(typeof entry.owner === "string" && entry.owner.length, `${row}: missing owner`);
    assert.ok(Array.isArray(entry.cases), `${row}: cases must be an array`);
    assert.ok(Array.isArray(entry.gaps ?? []), `${row}: gaps must be an array`);
    assert.equal(new Set(entry.cases).size, entry.cases.length, `${row}: duplicate case mapping`);
    for (const id of entry.cases) {
      assert.ok(known.has(id), `${row}: unknown case ${id}`);
      mapped.add(id);
    }
    if (!entry.cases.length) missing.push(row);
    for (const gap of entry.gaps ?? []) {
      assert.ok(typeof gap === "string" && gap.length, `${row}: empty gap`);
      gaps.push({ row, owner: entry.owner, gap });
    }
  }
  const unmapped = ids.filter(id => !mapped.has(id));
  return { rows: rows.length, mapped_rows: rows.length - missing.length, cases: ids.length,
    missing, unmapped, gaps, pass: !missing.length && !unmapped.length && !gaps.length };
}

export function readCoverage(cases = inventory(), root = directory) {
  return coverage(JSON.parse(readFileSync(join(root, "coverage.json"), "utf8")), cases);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const report = readCoverage();
  console.log(JSON.stringify(report, null, 2));
  process.exitCode = report.pass ? 0 : 1;
}
