#!/usr/bin/env node

import { createHash } from "node:crypto";
import { gzipSync } from "node:zlib";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { readBoundedGzip, tarEntries } from "./archive-utils.mjs";
import { verifyQualificationSummary } from "./verify-guide-projection-receipts.mjs";

const repo = resolve(fileURLToPath(new URL("..", import.meta.url)));
const qualifier = join(repo, "scripts", "qualify-guide-projections.mjs");
mkdirSync(join(repo, "work"), { recursive: true });
const root = mkdtempSync(join(repo, "work", "archive-gate-test-"));
const artifact = join(root, "acyclic-test-0.0.1.tgz");
const manifest = join(root, "projection.json");
const sourceSha = `sha256:${"a".repeat(64)}`;

function writeOctal(buffer, offset, length, value) {
  const text = value.toString(8).padStart(length - 1, "0");
  buffer.write(`${text}\0`, offset, length, "ascii");
}

function tarEntry(path, body = Buffer.alloc(0), type = "0") {
  const header = Buffer.alloc(512, 0);
  header.write(path, 0, 100, "utf8");
  writeOctal(header, 100, 8, 0o644);
  writeOctal(header, 108, 8, 0);
  writeOctal(header, 116, 8, 0);
  writeOctal(header, 124, 12, body.length);
  writeOctal(header, 136, 12, 0);
  header.fill(0x20, 148, 156);
  header.write(type, 156, 1, "ascii");
  header.write("ustar\0", 257, 6, "ascii");
  header.write("00", 263, 2, "ascii");
  writeOctal(header, 148, 8, header.reduce((sum, byte) => sum + byte, 0));
  const padding = Buffer.alloc((512 - (body.length % 512)) % 512);
  return Buffer.concat([header, body, padding]);
}

function writeArchive(entries) {
  const tar = Buffer.concat([
    ...entries.map(({ path, body, type }) => tarEntry(path, Buffer.from(body ?? ""), type ?? "0")),
    Buffer.alloc(1024),
  ]);
  writeFileSync(artifact, gzipSync(tar, { mtime: 0 }));
}

function projectionFor(artifactPath = artifact) {
  return {
    scenario_id: "archive-gate",
    family: "filesystem",
    operation: "acyclic.filesystem.v2.FilesystemService/Handshake",
    language: "typescript",
    mode: "remote",
    source: "rust/crates/sdk-examples/src/guide_projections.rs",
    source_sha256: sourceSha,
    source_git_revision: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    capability: "supported",
    package_manager: "npm",
    package_name: "@acyclic/test",
    artifact_path: artifactPath,
    qualification: { install: "bun-package-install", compile: "tsc-strict-consumer", execute: "bun-installed-package" },
    code: 'import { answer } from "@acyclic/test";\nconst value: number = answer;\nconsole.log(value);',
  };
}

function runQualifier(output, artifactPath = artifact) {
  writeFileSync(manifest, `${JSON.stringify([projectionFor(artifactPath)], null, 2)}\n`);
  return spawnSync(process.execPath, [qualifier, "--projections", manifest, "--output", output, "--expected-count", "1", "--strict"], {
    cwd: repo,
    encoding: "utf8",
    windowsHide: true,
    timeout: 120_000,
  });
}

function readSummary(output) {
  return JSON.parse(readFileSync(join(output, "qualification.json"), "utf8"));
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

try {
  const validEntries = [
    { path: "package/package.json", body: JSON.stringify({ name: "@acyclic/test", version: "0.0.1", type: "module", exports: { ".": { types: "./dist/index.d.ts", default: "./dist/index.js" } } }) },
    { path: "package/dist/index.js", body: "export const answer = 42;\n" },
    { path: "package/dist/index.d.ts", body: "export declare const answer: number;\n" },
  ];
  writeArchive(validEntries);
  assert(tarEntries(readBoundedGzip(artifact, 1024 * 1024, 1024 * 1024).expanded).length === validEntries.length, "test archive could not be parsed");

  const staleOutput = join(root, "valid");
  mkdirSync(join(staleOutput, "installed-ts-packages", "stale"), { recursive: true });
  writeFileSync(join(staleOutput, "installed-ts-packages", "stale", "sentinel"), "must disappear");
  const valid = runQualifier(staleOutput);
  const validSummary = readSummary(staleOutput);
  assert(validSummary.receipts[0].package_archive_format === "gzip+ustar", "valid archive was not extracted by the qualifier");
  assert(typeof validSummary.receipts[0].package_tree_sha256 === "string", "valid archive tree digest was not recorded");
  if (valid.status !== 0) {
    assert(/ENOENT|Bun could not find/i.test(validSummary.receipts[0].install?.stderr ?? ""), `valid archive failed for an unexpected reason: ${valid.stderr || valid.stdout}`);
  } else {
    assert(validSummary.verification.valid === true, "valid archive summary did not verify");
  }
  assert(validSummary.receipts[0].package_archive_format === "gzip+ustar", "archive format was not recorded");
  assert(typeof validSummary.receipts[0].package_tree_sha256 === "string", "package tree digest was not recorded");
  assert(!existsSync(join(staleOutput, "installed-ts-packages", "stale", "sentinel")), "stale installed root survived extraction");

  const mutated = readFileSync(artifact);
  mutated[mutated.length - 1] ^= 1;
  writeFileSync(artifact, mutated);
  const mutatedResult = verifyQualificationSummary(validSummary, {
    expectedProjections: [projectionFor(artifact)],
    expectedProjectionCount: 1,
    readArtifact: path => readFileSync(resolve(repo, path)),
  });
  assert(!mutatedResult.valid && (mutatedResult.errors.includes("receipt[0].package_bytes") || mutatedResult.errors.includes("receipt[0].package_tree_bytes")), "mutated archive was accepted");

  writeArchive([...validEntries, { path: "package/../escape.txt", body: "escape" }]);
  const traversal = runQualifier(join(root, "traversal"));
  assert(traversal.status !== 0 && readSummary(join(root, "traversal")).failed === 1, "traversal archive was accepted");

  writeArchive([...validEntries, { path: "package/dist/index.js", body: "duplicate" }]);
  const duplicate = runQualifier(join(root, "duplicate"));
  assert(duplicate.status !== 0 && readSummary(join(root, "duplicate")).failed === 1, "duplicate archive was accepted");

  writeArchive([...validEntries, { path: "package/link", body: "", type: "2" }]);
  const symlink = runQualifier(join(root, "symlink"));
  assert(symlink.status !== 0 && readSummary(join(root, "symlink")).failed === 1, "symlink archive was accepted");

  const fake = runQualifier(join(root, "fake"), join(repo, "typescript", "packages", "filesystem", "package.json"));
  assert(fake.status !== 0 && readSummary(join(root, "fake")).artifact_missing === 1, "metadata-only package artifact was accepted");

  console.log(JSON.stringify({ status: "passed", cases: ["valid archive extraction", "mutated archive", "traversal", "duplicate", "symlink", "stale extraction root", "metadata fake"], note: valid.status === 0 ? "full local package qualification" : "archive gate passed; local Bun install unavailable" }, null, 2));
} finally {
  rmSync(root, { recursive: true, force: true });
}
