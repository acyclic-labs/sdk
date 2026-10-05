#!/usr/bin/env node

import { createHash } from "node:crypto";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const checker = resolve(root, "scripts/check-harness-bindings-static.mjs");
const generatedDeclaration = resolve(root, "typescript/packages/harness/generated/wasm/acyclic_harness_wasm.d.ts");
const sha256 = value => createHash("sha256").update(value).digest("hex");

function runChecker(directory) {
  const result = spawnSync(process.execPath, [checker, "--generated-wasm-dir", directory], {
    cwd: root,
    encoding: "utf8",
    windowsHide: true,
    timeout: 30_000,
    maxBuffer: 4 * 1024 * 1024,
  });
  if (result.error) throw new Error(`checker process failed: ${result.error.message}`);
  if (result.signal !== null) throw new Error(`checker process did not exit cleanly: ${result.signal}`);
  if (result.stdout.trim() === "") throw new Error(`checker produced no JSON (exit ${result.status})\n${result.stderr}`);
  return { status: result.status, report: JSON.parse(result.stdout) };
}

const baselineRun = spawnSync(process.execPath, [checker], {
  cwd: root,
  encoding: "utf8",
  windowsHide: true,
  timeout: 30_000,
  maxBuffer: 4 * 1024 * 1024,
});
if (baselineRun.error) throw new Error(`baseline checker process failed: ${baselineRun.error.message}`);
if (baselineRun.signal !== null) throw new Error(`baseline checker process did not exit cleanly: ${baselineRun.signal}`);
if (baselineRun.status !== 0) throw new Error(`baseline static qualification failed:\n${baselineRun.stderr}`);
const baseline = JSON.parse(baselineRun.stdout);
const sourceSnapshot = baseline.package.generatedWasm.sourceInputs.map(({ path, sha256: digest }) => ({ path, sha256: digest }));
const requiredExports = baseline.wasm.requiredExports;
const declaration = readFileSync(generatedDeclaration);
const firstPartyDependency = sourceSnapshot.find(({ path }) =>
  path.startsWith("rust/crates/") && !path.startsWith("rust/crates/harness/"),
);
if (firstPartyDependency === undefined) throw new Error("baseline source closure did not include a first-party dependency");

function writeFixture(directory, manifestOverrides = {}) {
  mkdirSync(directory, { recursive: true });
  const javascript = Buffer.from(requiredExports.map(name => `export function ${name}() {}`).join("\n"));
  const wasm = Buffer.from([0, 97, 115, 109, 1, 0, 0, 0]);
  const backgroundDeclaration = Buffer.from("export interface InitOutput {}\n");
  const artifacts = [
    ["acyclic_harness_wasm.js", javascript],
    ["acyclic_harness_wasm.d.ts", declaration],
    ["acyclic_harness_wasm_bg.wasm", wasm],
    ["acyclic_harness_wasm_bg.wasm.d.ts", backgroundDeclaration],
  ];
  for (const [name, bytes] of artifacts) writeFileSync(join(directory, name), bytes);
  writeFileSync(join(directory, "acyclic_harness_wasm.manifest.json"), `${JSON.stringify({
    version: 1,
    generator: "scripts/build-harness-wasm.mjs",
    wasmBindgen: "0.2.117",
    target: "web",
    cargoProfile: "wasm-release",
    sourceCommit: baseline.repository.commit,
    sourceSnapshot,
    artifacts: artifacts.map(([path, bytes]) => ({ path, sha256: sha256(bytes) })),
    ...manifestOverrides,
  }, null, 2)}\n`);
}

function expectRejected(label, mutate) {
  const directory = mkdtempSync(join(tmpdir(), "harness-bindings-negative-"));
  try {
    writeFixture(directory);
    mutate(directory);
    const result = runChecker(directory);
    if (
      result.report.package.generatedWasm.ready
      || result.status === 0
      || result.report.status !== "failed"
      || result.report.qualificationCommands[0]?.status !== "failed"
    ) {
      throw new Error(`${label}: stale/corrupt generated artifacts were accepted as ready`);
    }
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

const missingDirectory = mkdtempSync(join(tmpdir(), "harness-bindings-missing-"));
try {
  const result = runChecker(missingDirectory);
  if (
    result.status !== 0
    || result.report.status !== "unverified"
    || result.report.package.generatedWasm.status !== "unverified"
    || result.report.package.generatedWasm.ready
    || result.report.qualificationCommands[0]?.status !== "passed"
  ) {
    throw new Error("missing generated WASM artifacts were not reported as unverified");
  }
} finally {
  rmSync(missingDirectory, { recursive: true, force: true });
}

const validDirectory = mkdtempSync(join(tmpdir(), "harness-bindings-valid-"));
try {
  writeFixture(validDirectory);
  const result = runChecker(validDirectory);
  if (
    result.status !== 0
    || result.report.status !== "passed"
    || result.report.package.generatedWasm.status !== "passed"
    || !result.report.package.generatedWasm.ready
  ) {
    throw new Error("valid generated WASM manifest fixture was not accepted");
  }
} finally {
  rmSync(validDirectory, { recursive: true, force: true });
}

expectRejected("modified JavaScript", directory => {
  writeFileSync(join(directory, "acyclic_harness_wasm.js"), "export function prepareModelRequest() {}\n");
});
expectRejected("modified WASM", directory => {
  writeFileSync(join(directory, "acyclic_harness_wasm_bg.wasm"), Buffer.from([1, 2, 3]));
});
expectRejected("stale source snapshot", directory => {
  const manifestPath = join(directory, "acyclic_harness_wasm.manifest.json");
  const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
  manifest.sourceSnapshot[0].sha256 = "0".repeat(64);
  writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
});
expectRejected("modified first-party dependency", directory => {
  const manifestPath = join(directory, "acyclic_harness_wasm.manifest.json");
  const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
  const dependency = manifest.sourceSnapshot.find(item => item.path === firstPartyDependency.path);
  dependency.sha256 = "f".repeat(64);
  writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
});
expectRejected("unsupported generator version", directory => {
  const manifestPath = join(directory, "acyclic_harness_wasm.manifest.json");
  const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
  manifest.wasmBindgen = "0.2.116";
  writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
});
expectRejected("missing declaration", directory => {
  rmSync(join(directory, "acyclic_harness_wasm.d.ts"));
});

process.stdout.write("stale generated WASM manifest cases passed\n");
