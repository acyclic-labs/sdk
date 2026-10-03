import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

const scenario = JSON.parse(readFileSync("docs/graphcoder-swarm/graphcoder-real-backend-scenarios.json", "utf8"));

test("the production GraphCoder scenario keeps mock, native, package, and PTY lanes distinct", () => {
  assert.equal(scenario.protocol, "acyclic.graphcoder.real-backend-scenarios.v1");
  assert.equal(scenario.backend.transport, "HarnessGraphCoderTransport");
  assert.equal(scenario.backend.provider, "PersistentLocalSwarm");
  assert.match(scenario.backend.model, /deterministic mock provider/);
  assert.match(scenario.backend.fixture_policy, /cannot satisfy native or PTY rows/);
  assert.ok(Array.isArray(scenario.session.headless_commands) && scenario.session.headless_commands.length > 0);
  assert.ok(Array.isArray(scenario.session.pty_commands) && scenario.session.pty_commands.length > 0);
  assert.equal(scenario.session.pty_commands.at(-1), "quit");
  for (const command of [...scenario.session.headless_commands, ...scenario.session.pty_commands]) {
    assert.equal(command.includes("--fixture"), false, `production command contains fixture flag: ${command}`);
  }
  for (const name of ["approval_id", "writeback_operation_id", "workspace_generation"]) {
    assert.equal(typeof scenario.session.substitutions[name], "string");
  }
  assert.ok(scenario.session.required_markers.length >= 8);
  assert.ok(scenario.wire_assertions.some(value => /request_id exactly matches/u.test(value)));
  assert.ok(scenario.wire_assertions.some(value => /no host credentials/u.test(value)));
  assert.equal(scenario.evidence.headless_execution_kind, "native");
  assert.equal(scenario.evidence.pty_execution_kind, "pty");
  assert.equal(scenario.evidence.package_execution_kind, "package");
  assert.match(scenario.evidence.failure_policy, /missing PTY support.*remain unmet/u);
});

test("the PTY scenario is mapped to the native helper and captured artifact boundary", () => {
  const lanes = readFileSync("docs/graphcoder-swarm/PRODUCTION-LANES.md", "utf8");
  assert.match(lanes, /graphcoder-production-pty\.mjs/u);
  assert.match(lanes, /graphcoder-qualification-suite\.mjs capture/u);
  assert.match(scenario.evidence.artifact_binding, /exact GraphCoder package artifact/u);
  assert.match(scenario.evidence.artifact_binding, /bridge executable/u);
});
