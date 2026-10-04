import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { resolve } from "node:path";
import { test } from "node:test";

const scenario = JSON.parse(readFileSync("docs/graphcoder-swarm/graphcoder-installed-swarm-scenarios.json", "utf8"));
const driver = readFileSync("scripts/graphcoder-installed-swarm-e2e.mjs", "utf8");

test("installed swarm scenario requires exact recursive/effect evidence", () => {
  assert.equal(scenario.driver, "scripts/graphcoder-installed-swarm-e2e.mjs");
  for (const marker of ["two direct children with exact tasks child-a and child-b", "distinct typed command and writeback approvals with operation/action binding", "host approval before each public resolution", "physical checkout bytes unchanged before approval and equal to expected bytes after writeback", "a separate declared concurrent user edit is written to the checkout before writeback and survives reconciliation", "verified bridge cleanup"]) {
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
  assert.match(driver, /expectation\.after_sha256/u);
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

test("driver rejects invalid qualification configuration before launching or publishing evidence", () => {
  const evidencePath = resolve("target", `rejected-swarm-${randomUUID()}.json`);
  const base = {
    ...(process.env.SystemRoot ? { SystemRoot: process.env.SystemRoot } : {}),
    GRAPHCODER_PACKAGE_ROOT: process.cwd(),
    GRAPHCODER_BRIDGE_EXECUTABLE: process.execPath,
    GRAPHCODER_BRIDGE_CWD: process.cwd(),
    GRAPHCODER_SWARM_CHECKOUT_ROOT: process.cwd(),
    GRAPHCODER_BRIDGE_ENV_JSON: JSON.stringify({ GRAPHCODER_OPERATOR_TOKEN: "test-only" }),
    GRAPHCODER_SWARM_EXPECTED_FILES_JSON: JSON.stringify({ "fixture.txt": {} }),
    GRAPHCODER_SWARM_EXPECTED_APPROVAL_JSON: JSON.stringify({ command: {}, writeback: {} }),
    GRAPHCODER_SWARM_EXPECTED_COMMAND_JSON: "{}",
    GRAPHCODER_SWARM_CONCURRENT_EDIT_JSON: "{}",
    GRAPHCODER_SWARM_EVIDENCE_PATH: evidencePath,
  };
  for (const [patch, expected] of [
    [{ GRAPHCODER_BRIDGE_CWD: "" }, /GRAPHCODER_BRIDGE_CWD is required/u],
    [{ GRAPHCODER_SWARM_EXPECTED_FILES_JSON: "{}" }, /at least one checkout file/u],
    [{ GRAPHCODER_SWARM_EXPECTED_APPROVAL_JSON: "{}" }, /EXPECTED_APPROVAL_JSON.command/u],
    [{ GRAPHCODER_BRIDGE_ENV_JSON: JSON.stringify({ GRAPHCODER_OPERATOR_TOKEN: "test-only", OPENAI_API_KEY: "not-a-real-key" }) }, /undeclared key/u],
    [{ GRAPHCODER_BRIDGE_ENV_JSON: "{}" }, /explicit operator approval token/u],
  ]) {
    const result = spawnSync(process.execPath, ["scripts/graphcoder-installed-swarm-e2e.mjs"], {
      cwd: process.cwd(), env: { ...base, ...patch }, encoding: "utf8", timeout: 10_000, windowsHide: true,
    });
    assert.equal(result.error, undefined);
    assert.equal(result.status, 1);
    assert.match(result.stderr, expected);
    assert.equal(existsSync(evidencePath), false);
  }
});
