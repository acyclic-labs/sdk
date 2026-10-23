import assert from "node:assert/strict";
import test from "node:test";
import { qualification, qualified } from "./qualification.mjs";

test("external acceptance requires executed tests, not a successful empty or ignored selection", () => {
  const result = stdout => ({ code: 0, stdout, stderr: "" });
  const summary = (passed, failed = 0, ignored = 0) => `test result: ok. ${passed} passed; ${failed} failed; ${ignored} ignored;`;
  qualified(result(summary(1)), "libtest", 1);
  for (const output of ["", summary(0), summary(1, 1), summary(1, 0, 1), summary(1) + summary(1)]) {
    assert.throws(() => qualified(result(output), "libtest", 1));
  }
  qualified(result("1 pass\n0 fail"), "bun", 1);
  for (const output of ["0 pass\n0 fail", "1 pass\n1 fail", "1 pass\n0 fail\n1 skip", "1 pass\n0 fail\n1 fail", "1 pass\n1 pass\n0 fail"]) {
    assert.throws(() => qualified(result(output), "bun", 1));
  }
});

test("installed-host entry points receive the same prebuilt plugin and fixture profile", async () => {
  const fixture = { workspace: "fixture-workspace", env: { E2E_BUN: process.execPath,
    E2E_PLUGIN_BINARY: process.execPath, CODEX_HOME: "fixture-profile" } };
  let calls = 0, captures = 0;
  const processes = { async runOwned(binary, args, options) {
    calls++;
    assert.equal(binary, process.execPath);
    assert.equal(options.cwd, fixture.workspace);
    assert.equal(options.env.CODEX_HOME, "fixture-profile");
    assert.equal(options.env.ACYCLIC_TEST_BINARY, process.execPath);
    assert.equal(options.env.ACYCLIC_BINARY, process.execPath);
    assert.equal(options.env.RUST_TEST_THREADS, "1");
    return { code: 0, stdout: "1 pass\n0 fail", stderr: "" };
  } };
  const input = { binary_env: "E2E_BUN", fixture_binary_env: "E2E_PLUGIN_BINARY",
    args: ["test"], format: "bun", minimum_tests: 1 };
  await qualification(processes, input, fixture, () => captures++);
  assert.equal(calls, 1); assert.equal(captures, 1);
  await assert.rejects(qualification(processes, { ...input, fixture_binary_env: "ABSENT" }, fixture, () => {}), /prebuilt plugin/);
  assert.equal(calls, 1, "missing fixture must fail before launching a host");
});
