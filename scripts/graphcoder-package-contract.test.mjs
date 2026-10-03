import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { test } from "node:test";
import { assertLazyCounters, inspectInstalledPackage } from "./fixtures/graphcoder-qualification/package-contract.mjs";

function fixturePackage() {
  const root = mkdtempSync(join(tmpdir(), "graphcoder-package-contract-"));
  mkdirSync(join(root, "dist"));
  const exports = {
    ".": "./dist/index.js",
    "./mock": "./dist/mock.js",
    "./bridge": "./dist/bridge.js",
    "./node": "./dist/node.js",
    "./terminal": "./dist/terminal.js",
    "./node-dispatcher": "./dist/node-dispatcher.js",
    "./native-cli": "./dist/native-cli.js",
  };
  for (const target of Object.values(exports)) writeFileSync(join(root, target), "export {};\n");
  writeFileSync(join(root, "package.json"), `${JSON.stringify({
    name: "@acyclic-labs/graphcoder",
    version: "0.2.0",
    type: "module",
    exports,
    bin: { graphcoder: "./dist/cli.js", "graphcoder-native": "./dist/native-cli.js" },
  })}\n`);
  writeFileSync(join(root, "dist/cli.js"), "#!/usr/bin/env node\n");
  return root;
}

test("installed package contract records export and bin identities", () => {
  const root = fixturePackage();
  try {
    const identity = inspectInstalledPackage(root);
    assert.equal(identity.package, "@acyclic-labs/graphcoder");
    assert.equal(Object.keys(identity.exports).length, 7);
    assert.match(identity.package_json_sha256, /^[0-9a-f]{64}$/u);
    assert.ok(identity.bins.graphcoder.endsWith("dist\\cli.js"));
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("real-host lazy observations require zero listing side effects", () => {
  const root = mkdtempSync(join(tmpdir(), "graphcoder-lazy-observation-"));
  try {
    const observation = join(root, "counters.json");
    writeFileSync(observation, `${JSON.stringify({
      schema: "graphcoder.lazy-observation.v1",
      runtime: { pid: 42, executable: process.execPath },
      request: { request_id: "list-1", method: "list_sessions" },
      during_list_sessions: { worker_starts: 0, workspace_reads: 0, model_dispatches: 0 },
    })}\n`);
    assert.deepEqual(assertLazyCounters(observation, {
      require: true,
      expectedRequestId: "list-1",
      expectedMethod: "list_sessions",
      expectedExecutable: process.execPath,
    }).during_list_sessions, {
      worker_starts: 0,
      workspace_reads: 0,
      model_dispatches: 0,
    });
    writeFileSync(observation, `${JSON.stringify({
      schema: "graphcoder.lazy-observation.v1",
      runtime: { pid: 42, executable: process.execPath },
      request: { request_id: "list-1", method: "list_sessions" },
      during_list_sessions: { worker_starts: 1, workspace_reads: 0, model_dispatches: 0 },
    })}\n`);
    assert.throws(() => assertLazyCounters(observation, { require: true }), /worker starts/u);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
