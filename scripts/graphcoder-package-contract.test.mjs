import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { test } from "node:test";
import { gzipSync } from "node:zlib";
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

function archiveEntry(path, body) {
  const header = Buffer.alloc(512);
  header.write(path, 0, 100, "utf8");
  header.write("0000644", 100, 8, "ascii");
  header.write("0000000", 108, 8, "ascii");
  header.write("0000000", 116, 8, "ascii");
  header.write(body.length.toString(8).padStart(11, "0"), 124, 11, "ascii");
  header.write("00000000000", 136, 11, "ascii");
  header[156] = 48;
  header.write("        ", 148, 8, "ascii");
  const checksum = header.reduce((sum, value) => sum + value, 0);
  header.write(`${checksum.toString(8).padStart(6, "0")}\0 `, 148, 8, "ascii");
  const padding = Buffer.alloc((512 - (body.length % 512)) % 512);
  return Buffer.concat([header, body, padding]);
}

function fixtureArchive(root) {
  const entries = [archiveEntry("package/package.json", readFileSync(join(root, "package.json")))];
  for (const name of readdirSync(join(root, "dist"))) entries.push(archiveEntry(`package/dist/${name}`, readFileSync(join(root, "dist", name))));
  const archive = join(root, "graphcoder.tgz");
  writeFileSync(archive, gzipSync(Buffer.concat([...entries, Buffer.alloc(1024)])));
  return archive;
}

test("installed package contract records export and bin identities", () => {
  const root = fixturePackage();
  try {
    const identity = inspectInstalledPackage(root, { artifactPath: fixtureArchive(root) });
    assert.equal(identity.package, "@acyclic-labs/graphcoder");
    assert.equal(Object.keys(identity.exports).length, 7);
    assert.match(identity.package_json_sha256, /^[0-9a-f]{64}$/u);
    assert.ok(identity.bins.graphcoder.endsWith("dist\\cli.js"));
    assert.match(identity.artifact.manifest_sha256, /^[0-9a-f]{64}$/u);
    assert.equal(identity.artifact.exports.length, 7);
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
      during_list_sessions: {
        counters_before: { worker_starts: 4, workspace_reads: 7, model_dispatches: 2 },
        counters_after: { worker_starts: 4, workspace_reads: 7, model_dispatches: 2 },
        worker_starts: 0, workspace_reads: 0, model_dispatches: 0,
      },
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
      during_list_sessions: {
        counters_before: { worker_starts: 4, workspace_reads: 7, model_dispatches: 2 },
        counters_after: { worker_starts: 5, workspace_reads: 7, model_dispatches: 2 },
        worker_starts: 1, workspace_reads: 0, model_dispatches: 0,
      },
    })}\n`);
    assert.throws(() => assertLazyCounters(observation, { require: true }), /worker starts/u);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
