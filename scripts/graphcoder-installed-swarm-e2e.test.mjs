import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

const scenario = JSON.parse(readFileSync("docs/graphcoder-swarm/graphcoder-installed-swarm-scenarios.json", "utf8"));
const driver = readFileSync("scripts/graphcoder-installed-swarm-e2e.mjs", "utf8");

test("installed swarm scenario requires exact recursive/effect evidence", () => {
  assert.equal(scenario.driver, "scripts/graphcoder-installed-swarm-e2e.mjs");
  for (const marker of ["two direct children with exact tasks child-a and child-b", "typed command or writeback approval with operation/action binding", "host approval before public resolution", "concurrent-user reconciliation", "verified bridge cleanup"]) {
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
  assert.match(driver, /pending\.executable !== expectedCommand\.executable/u);
  assert.match(driver, /pending\.operation_id !== expectedApproval\.operation_id/u);
  assert.match(driver, /beforeFiles\[path\] !== expectation\.before_sha256/u);
  assert.match(driver, /digest !== expectation\.after_sha256/u);
  assert.match(driver, /concurrent_user_edit_preserved !== true/u);
  assert.match(driver, /cleanup_verified: true/u);
});

test("installed swarm driver rejects ambient bridge configuration and unsupported routes", () => {
  assert.match(driver, /required\("GRAPHCODER_BRIDGE_CWD"\)/u);
  assert.match(driver, /contains an undeclared key/u);
  assert.match(driver, /list_changes is not available/u);
  assert.match(driver, /read_change is not available/u);
  assert.doesNotMatch(driver, /responseError\(/u);
});
