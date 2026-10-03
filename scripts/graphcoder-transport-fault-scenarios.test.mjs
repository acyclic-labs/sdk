import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

const scenario = JSON.parse(readFileSync("docs/graphcoder-swarm/graphcoder-transport-fault-scenarios.json", "utf8"));
const requirements = JSON.parse(readFileSync("docs/graphcoder-swarm/requirements.json", "utf8"));

test("installed transport fault scenarios map to exact requirements and diagnostics", () => {
  assert.equal(scenario.protocol, "acyclic.graphcoder.transport-fault-scenarios.v1");
  assert.equal(scenario.execution_kind, "package");
  assert.equal(scenario.driver, "scripts/graphcoder-installed-transport-faults.mjs");
  assert.deepEqual(scenario.requirements, ["INPUT-04", "API-07", "EFFECT-06", "FAULT-01", "FAULT-02", "QUAL-01", "QUAL-03"]);
  const lockedIds = new Set(requirements.entries.map(entry => entry.id));
  assert.equal(scenario.requirements.every(id => lockedIds.has(id)), true);
  assert.equal(new Set(scenario.cases.map(item => item.id)).size, scenario.cases.length);
  assert.deepEqual(scenario.cases.map(item => item.id), ["malformed-framing", "correlation-rejection", "cancelled-response"]);
  assert.deepEqual(scenario.cases.map(item => item.expected_diagnostic), ["malformed_line", "unmatched_response", "cancelled_response"]);
  assert.match(scenario.evidence, /installed package export/u);
  assert.match(scenario.evidence, /does not qualify native runtime effects/u);
});
