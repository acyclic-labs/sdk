import assert from "node:assert/strict";
import { test } from "node:test";
import { assertTypedTerminalRecords, bridgeEnvironment, commandContext, framedPromptCount, installChildSignalCleanup, validateFreshInputSequence } from "./graphcoder-production-pty.mjs";

test("PTY environment keeps only platform and explicit GraphCoder variables", () => {
  const filtered = bridgeEnvironment({
    PATH: "path",
    SystemRoot: "root",
    GRAPHCODER_PACKAGE_ROOT: "package",
    GRAPHCODER_BRIDGE_ENV_JSON: "{}",
    GRAPHCODER_UNDECLARED_SECRET: "must-not-pass",
    AWS_SECRET_ACCESS_KEY: "must-not-pass",
    HOME: "must-not-pass",
  });
  assert.deepEqual(filtered, {
    PATH: "path",
    SystemRoot: "root",
    GRAPHCODER_PACKAGE_ROOT: "package",
    GRAPHCODER_BRIDGE_ENV_JSON: "{}",
  });
});

test("PTY environment does not forward undeclared GraphCoder variables", () => {
  const filtered = bridgeEnvironment({
    PATH: "path",
    GRAPHCODER_PACKAGE_ROOT: "package",
    GRAPHCODER_BRIDGE_EXECUTABLE: "runtime.exe",
    GRAPHCODER_BRIDGE_ARGS_JSON: "[]",
    GRAPHCODER_BRIDGE_ENV_JSON: "{}",
    GRAPHCODER_BRIDGE_CWD: "C:\\qualification",
    GRAPHCODER_UNDECLARED_SECRET: "must-not-pass",
  });
  assert.deepEqual(filtered, {
    PATH: "path",
    GRAPHCODER_PACKAGE_ROOT: "package",
    GRAPHCODER_BRIDGE_EXECUTABLE: "runtime.exe",
    GRAPHCODER_BRIDGE_ARGS_JSON: "[]",
    GRAPHCODER_BRIDGE_ENV_JSON: "{}",
    GRAPHCODER_BRIDGE_CWD: "C:\\qualification",
  });
});

test("PTY prompt framing ignores ANSI and only counts complete prompt lines", () => {
  const transcript = "\u001b[32mgraphcoder>\u001b[0m start\r\n{\"ok\":true}\r\ngraphcoder> ";
  assert.equal(framedPromptCount(transcript), 2);
  assert.equal(framedPromptCount("prefix graphcoder> text"), 0);
});

test("PTY command context recovers dynamic identities from terminal projections", () => {
  const transcript = [
    "graphcoder> start inspect",
    '{"ok":true,"value":{"selectedSession":{"id":"session-1"}}}',
    "graphcoder> approvals",
    '{"ok":true,"value":[{"id":"approval-1","operationId":"op-1","state":"pending"}]}',
    "graphcoder> changes",
    '{"ok":true,"value":{"generation":"7","items":[]}}',
  ].join("\r\n");
  assert.deepEqual(commandContext(transcript), {
    session_id: "session-1",
    approval_id: "approval-1",
    writeback_operation_id: "op-1",
    workspace_generation: "7",
  });
});

test("PTY transcript validation requires typed native projections and exit", () => {
  const transcript = [
    '{"ok":true,"value":{"selectedSession":{"id":"session-1"}}}',
    '{"ok":true,"value":[{"id":"activity-1"}]}',
    '{"ok":true,"value":{"unifiedDiff":"fixture"}}',
    '{"ok":true,"value":{"mediaType":"text/plain"}}',
    '{"ok":true,"value":{"applied":true}}',
    '{"ok":true,"value":{"state":"cancelled"}}',
    '{"ok":true,"exited":true}',
  ].join("\r\n");
  assert.equal(assertTypedTerminalRecords(transcript).length, 7);
  assert.throws(() => assertTypedTerminalRecords(transcript.replace('"applied":true', '"applied":false')), /writeback receipt/);
});

test("PTY child signal listeners forward termination and are removed after close handling", () => {
  const beforeInt = process.listenerCount("SIGINT");
  const beforeTerm = process.listenerCount("SIGTERM");
  const signals = [];
  const cleanup = installChildSignalCleanup({ kill(signal) { signals.push(signal); return true; } });
  assert.equal(process.listenerCount("SIGINT"), beforeInt + 1);
  assert.equal(process.listenerCount("SIGTERM"), beforeTerm + 1);
  process.rawListeners("SIGTERM").at(-1)?.();
  assert.deepEqual(signals, ["SIGTERM"]);
  cleanup();
  assert.equal(process.listenerCount("SIGINT"), beforeInt);
  assert.equal(process.listenerCount("SIGTERM"), beforeTerm);
});

test("PTY fresh-input scenario follows a reopened session and uses a new operation", () => {
  assert.doesNotThrow(() => validateFreshInputSequence([
    "start op-first inspect",
    "resume session-1",
    "input op-follow-up follow-up  with exact bytes",
  ]));
  assert.throws(
    () => validateFreshInputSequence(["start op-first inspect", "input op-follow-up follow-up"]),
    /follow an open or resume/u,
  );
  assert.throws(
    () => validateFreshInputSequence(["start op-first inspect", "resume session-1", "input op-first follow-up"]),
    /reuses an earlier operation identity/u,
  );
});
