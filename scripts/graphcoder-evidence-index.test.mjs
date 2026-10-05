import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { buildIndex, validateIndex } from "./graphcoder-evidence-index.mjs";

test("evidence index traces every locked requirement to status and prerequisites", () => {
  const index = buildIndex();
  assert.equal(index.entries.length, 68);
  assert.deepEqual(index.entries.reduce((counts, entry) => ({ ...counts, [entry.status]: (counts[entry.status] ?? 0) + 1 }), {}), { checkpoint: 22, partial: 18, unmet: 28 });
  assert.equal(new Set(index.entries.map(entry => entry.id)).size, 68);
  assert.ok(index.entries.every(entry => entry.evidence_refs.length >= 3 && entry.prerequisite.length > 0));
  assert.deepEqual(validateIndex(index), { requirements: 68, statuses: { checkpoint: 22, partial: 18, unmet: 28 } });
});

test("checked evidence index rejects a status drift", () => {
  const index = buildIndex();
  index.entries[0].status = "passed";
  assert.throws(() => validateIndex(index), /status differs from the locked audit/);
  assert.match(readFileSync("docs/graphcoder-swarm/MATRIX-GAP-AUDIT.md", "utf8"), /Only a final\s+receipt with every row passed can close the matrix/u);
});
