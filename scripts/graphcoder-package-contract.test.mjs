import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { test } from "node:test";
import { gzipSync } from "node:zlib";
import { fileURLToPath } from "node:url";
import {
  assertLazyCounters,
  GRAPH_CODER_EXPORT_COUNT,
  inspectInstalledPackage,
  LAZY_LISTING_COUNTERS,
} from "./fixtures/graphcoder-qualification/package-contract.mjs";

const sdkRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const graphCoderPackageRoot = join(sdkRoot, "typescript", "packages", "graphcoder");

function fixturePackage(exportsOverride) {
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
  const packageExports = exportsOverride ?? exports;
  for (const target of Object.values(packageExports)) writeFileSync(join(root, target), "export {};\n");
  writeFileSync(join(root, "package.json"), `${JSON.stringify({
    name: "@acyclic-labs/graphcoder",
    version: "0.2.0",
    type: "module",
    exports: packageExports,
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
    assert.equal(Object.keys(identity.exports).length, GRAPH_CODER_EXPORT_COUNT);
    assert.match(identity.package_json_sha256, /^[0-9a-f]{64}$/u);
    assert.ok(identity.bins.graphcoder.endsWith("dist\\cli.js"));
    assert.match(identity.artifact.manifest_sha256, /^[0-9a-f]{64}$/u);
    assert.equal(identity.artifact.exports.length, GRAPH_CODER_EXPORT_COUNT);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("installed package contract rejects manifest bytes that differ from the archive", () => {
  const root = fixturePackage();
  try {
    const archive = fixtureArchive(root);
    const packageJsonPath = join(root, "package.json");
    writeFileSync(packageJsonPath, `${readFileSync(packageJsonPath, "utf8")}\n`);
    assert.throws(
      () => inspectInstalledPackage(root, { artifactPath: archive }),
      /manifest bytes differ from package artifact/u,
    );
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("installed package contract rejects an unpinned public export", () => {
  const root = fixturePackage({
    ".": "./dist/index.js",
    "./mock": "./dist/mock.js",
    "./bridge": "./dist/bridge.js",
    "./node": "./dist/node.js",
    "./terminal": "./dist/terminal.js",
    "./node-dispatcher": "./dist/node-dispatcher.js",
    "./native-cli": "./dist/native-cli.js",
    "./private": "./dist/private.js",
  });
  try {
    assert.throws(() => inspectInstalledPackage(root), /export count\/map does not match/u);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("installed package contract rejects exported bytes that differ from the archive", () => {
  const root = fixturePackage();
  try {
    const archive = fixtureArchive(root);
    writeFileSync(join(root, "dist", "index.js"), "export const tampered = true;\n");
    assert.throws(
      () => inspectInstalledPackage(root, { artifactPath: archive }),
      /installed package file differs from package artifact/u,
    );
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("GraphCoder release metadata includes the public package contract", () => {
  const release = JSON.parse(readFileSync(join(sdkRoot, "release", "npm-packages.json"), "utf8"));
  const entry = release.find(item => item?.name === "@acyclic-labs/graphcoder");
  assert.deepEqual(entry, {
    slug: "graphcoder",
    directory: "graphcoder",
    name: "@acyclic-labs/graphcoder",
    source: "typescript",
  });
  const manifest = JSON.parse(readFileSync(join(graphCoderPackageRoot, "package.json"), "utf8"));
  assert.equal(manifest.private, false);
  assert.ok(manifest.files.includes("CHANGELOG.md"));
  assert.match(readFileSync(join(graphCoderPackageRoot, "CHANGELOG.md"), "utf8"), /## 0\.2\.0\b/u);
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
        counters_before: Object.fromEntries(LAZY_LISTING_COUNTERS.map((field, index) => [field, index + 2])),
        counters_after: Object.fromEntries(LAZY_LISTING_COUNTERS.map((field, index) => [field, index + 2])),
        ...Object.fromEntries(LAZY_LISTING_COUNTERS.map(field => [field, 0])),
      },
    })}\n`);
    assert.deepEqual(assertLazyCounters(observation, {
      require: true,
      expectedRequestId: "list-1",
      expectedMethod: "list_sessions",
      expectedExecutable: process.execPath,
    }).during_list_sessions, {
      ...Object.fromEntries(LAZY_LISTING_COUNTERS.map(field => [field, 0])),
    });
    writeFileSync(observation, `${JSON.stringify({
      schema: "graphcoder.lazy-observation.v1",
      runtime: { pid: 42, executable: process.execPath },
      request: { request_id: "list-1", method: "list_sessions" },
      during_list_sessions: {
        counters_before: Object.fromEntries(LAZY_LISTING_COUNTERS.map((field, index) => [field, index + 2])),
        counters_after: Object.fromEntries(LAZY_LISTING_COUNTERS.map((field, index) => [field, field === "workspace_body_reads" ? index + 3 : index + 2])),
        ...Object.fromEntries(LAZY_LISTING_COUNTERS.map(field => [field, field === "workspace_body_reads" ? 1 : 0])),
      },
    })}\n`);
    assert.throws(() => assertLazyCounters(observation, { require: true }), /workspace body reads/u);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
