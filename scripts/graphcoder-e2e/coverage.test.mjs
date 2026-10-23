import assert from "node:assert/strict";
import test from "node:test";
import { coverage, inventory, readCoverage, rows } from "./coverage.mjs";

const complete = () => ({ version: 1, rows: Object.fromEntries(rows.map(row =>
  [row, { target: `Target ${row}`, owner: "e2e-gate", cases: ["real-case"] }])) });

test("coverage requires all roadmap rows and real, uniquely identified cases", () => {
  const manifest = complete(), cases = [{ id: "real-case" }];
  assert.equal(coverage(manifest, cases).pass, true);
  for (const row of rows) {
    const removed = structuredClone(manifest);
    delete removed.rows[row];
    assert.throws(() => coverage(removed, cases), /exactly A1–A26/);
    const empty = structuredClone(manifest);
    empty.rows[row].cases = [];
    const result = coverage(empty, cases);
    assert.equal(result.pass, false);
    assert.deepEqual(result.missing, [row]);
  }
  assert.throws(() => coverage(manifest, []), /unknown case real-case/);
  assert.throws(() => coverage(manifest, [...cases, ...cases]), /identities must be unique/);
  const typo = complete(); typo.rows.A27 = typo.rows.A26;
  assert.throws(() => coverage(typo, cases), /exactly A1–A26/);
});

test("unmapped cases and documented gaps cannot turn into release exemptions", () => {
  const manifest = complete(), cases = [{ id: "real-case" }, { id: "new-case" }];
  assert.deepEqual(coverage(manifest, cases).unmapped, ["new-case"]);
  assert.equal(coverage(manifest, cases).pass, false);
  manifest.rows.A1.cases.push("new-case");
  assert.equal(coverage(manifest, cases).pass, true);
  manifest.rows.A1.gaps = ["installed host still unqualified"];
  assert.equal(coverage(manifest, cases).pass, false);
  manifest.rows.A1.cases.push("new-case");
  assert.throws(() => coverage(manifest, cases), /duplicate case mapping/);
});

test("the checked-in inventory has valid references and every case is mapped", () => {
  const result = readCoverage(inventory());
  assert.equal(result.rows, 26);
  assert.deepEqual(result.unmapped, []);
  // readCoverage's pass field remains false until every recorded product gap
  // has an executable acceptance case. Inventory validity is not qualification.
  assert.equal(result.pass, !result.missing.length && !result.gaps.length);
});
