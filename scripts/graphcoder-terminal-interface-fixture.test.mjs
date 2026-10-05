import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

const driver = readFileSync("scripts/graphcoder-terminal-interface-fixture.mjs", "utf8");

test("terminal fixture drives one generic public controller surface", () => {
  for (const method of [
    "list_sessions",
    "start_session",
    "open_session",
    "resume_session",
    "input_session",
    "read_activity",
    "read_messages",
    "send_message",
    "list_approvals",
    "list_changes",
    "inspect_writeback",
    "apply_writeback",
    "recover_writeback",
    "cancel_session",
  ]) assert.equal(driver.includes(`"${method}"`), true, method);
  assert.match(driver, /limit: 64/u);
  assert.match(driver, /generation fence/u);
  assert.match(driver, /Harness-sealed inspection/u);
  assert.match(driver, /exact selected checkout/u);
  assert.match(driver, /exact selected project identity/u);
});

test("terminal fixture rejects ambient credentials and unbound writeback", () => {
  assert.match(driver, /contains an undeclared key/u);
  assert.match(driver, /bare checkout path cannot authorize/u);
  assert.match(driver, /writeback accepted an unbound inspection handle/u);
  assert.doesNotMatch(driver, /process\.env\.PATH.*bridgeEnvironment/u);
});

test("terminal fixture closes its owned bridge on success and failure", () => {
  assert.match(driver, /bridge\.close\("terminal interface fixture finished"\)/u);
  assert.match(driver, /await bridge\.waitForExit\(5_000\)/u);
  assert.match(driver, /cleanup_verified: true/u);
});
