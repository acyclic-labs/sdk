import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

const scenario = JSON.parse(readFileSync("docs/graphcoder-swarm/graphcoder-installed-swarm-scenarios.json", "utf8"));
const driver = readFileSync("scripts/graphcoder-installed-swarm-e2e.mjs", "utf8");

test("installed swarm scenario requires exact recursive/effect evidence", () => {
  assert.equal(scenario.driver, "scripts/graphcoder-installed-swarm-e2e.mjs");
  for (const marker of ["exactly four agents with distinct identities and root -> child-a/child-b -> grandchild relationships", "strictly ordered activity and message identities", "distinct typed command and writeback approvals with operation/action binding", "host approval before each public resolution", "durable command completion receipt with operation, attempt, request digest, exact output, marker, and descendant cleanup", "physical checkout bytes unchanged before approval and equal to expected bytes after writeback", "a separate declared concurrent user edit is written to the checkout before writeback and survives reconciliation", "verified bridge cleanup"]) {
    assert.equal(scenario.required_effects.includes(marker), true, marker);
  }
  assert.match(driver, /agent\.task === "child-a"/u);
  assert.match(driver, /agent\.task === "child-b"/u);
  assert.match(driver, /agent\.task === "grandchild"/u);
  assert.match(driver, /operator_approve/u);
  assert.match(driver, /action_digest/u);
  assert.match(driver, /concurrent_user_edit_preserved/u);
  assert.match(driver, /before_sha256/u);
  assert.match(driver, /after_sha256/u);
  assert.match(driver, /command\.executable !== expectedCommand\.executable/u);
  assert.match(driver, /item\.operation_id !== expected\.operation_id/u);
  assert.match(driver, /beforeFiles\[path\] !== expectation\.before_sha256/u);
  assert.match(driver, /expectation\.workspace_sha256/u);
  assert.match(driver, /concurrentEdit\.after_sha256/u);
  assert.match(driver, /writeFileSync\(resolve\(checkoutRoot, concurrentPath\)/u);
  assert.match(driver, /execution\.attempt_id !== expectedExecution\.attempt_id/u);
  assert.match(driver, /execution\.cleanup_verified !== true/u);
  assert.match(driver, /executionMarker !== expectedExecution\.marker_sha256/u);
  assert.match(driver, /agents\.length !== 4/u);
  assert.match(driver, /new Set\(agents\.map\(agent => agent\.id\)\)\.size !== 4/u);
  assert.match(driver, /expectation\.after_sha256/u);
  assert.match(driver, /concurrent_user_edit_preserved !== true/u);
  assert.match(driver, /cleanup_verified: true/u);
});

test("installed swarm driver rejects ambient bridge configuration and unsupported routes", () => {
  assert.match(driver, /required\("GRAPHCODER_BRIDGE_CWD"\)/u);
  assert.match(driver, /contains an undeclared key/u);
  assert.match(driver, /list_changes is not available/u);
  assert.match(driver, /read_change is not available/u);
  assert.match(driver, /if \(bridge !== undefined\)/u);
  assert.doesNotMatch(driver, /responseError\(/u);
});
