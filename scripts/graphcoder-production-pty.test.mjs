import assert from "node:assert/strict";
import { test } from "node:test";
import { bridgeEnvironment, commandContext, framedPromptCount, installChildSignalCleanup } from "./graphcoder-production-pty.mjs";

test("PTY environment keeps only platform and explicit GraphCoder variables", () => {
  const filtered = bridgeEnvironment({
    PATH: "path",
    SystemRoot: "root",
    GRAPHCODER_PACKAGE_ROOT: "package",
    GRAPHCODER_BRIDGE_ENV_JSON: "{}",
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
