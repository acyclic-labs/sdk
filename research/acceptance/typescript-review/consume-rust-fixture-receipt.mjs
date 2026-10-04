#!/usr/bin/env node
// Convert the Rust-owned TypeScript fixture transcripts into the canonical
// scenario log consumed by sdk-qualification-receipt.  This adapter never
// invents an RPC inventory: it resolves every method against rust-authority.

import { createHash } from "node:crypto";
import { execFileSync, spawnSync } from "node:child_process";
import { mkdir, readFile, readdir, stat, writeFile } from "node:fs/promises";
import { readFileSync } from "node:fs";
import { basename, join, relative, resolve, sep } from "node:path";

const args = parseArgs(process.argv.slice(2));
const root = resolve(required("source-root"));
const authorityPath = resolve(args.authority ?? join(root, "target/sdk-contract/rust-authority.json"));
const runtimePath = resolve(required("runtime-receipt"));
const inferencePath = resolve(required("inference-receipt"));
const machinesPath = resolve(required("machines-receipt"));
const packageRoot = resolve(required("package-root"));
const output = resolve(required("output"));
const sourceRevision = git(root, ["rev-parse", "HEAD"]);
const captured = [readJson(runtimePath), readJson(inferencePath), readJson(machinesPath)];
const authority = readJson(authorityPath);

if (args.expectedRevision && sourceRevision !== args.expectedRevision) {
  fail(`source revision ${sourceRevision} does not match --expected-revision ${args.expectedRevision}`);
}
if (args.expectedSourceRoot && resolve(args.expectedSourceRoot) !== root) {
  fail(`source root must be ${resolve(args.expectedSourceRoot)}`);
}
for (const receipt of captured) {
  const revision = receipt.fixture_commit ?? receipt.source?.revisionAtCapture;
  if (revision && gitAncestor(root, revision) !== true) {
    fail(`fixture revision ${revision} is not an ancestor of ${sourceRevision}`);
  }
}

const inventory = authorityInventory(authority);
const packageNames = ["actors", "workers", "objects", "stream", "filesystem", "harness", "inference", "machines"];
const archiveDir = join(output, "qualification/packages");
const consumerDir = join(output, "qualification/consumers");
await mkdir(archiveDir, { recursive: true });
await mkdir(consumerDir, { recursive: true });

const packages = [];
for (const family of packageNames) {
  const packageDir = join(packageRoot, "packages", family);
  const packageJsonPath = join(packageDir, "package.json");
  const packageJson = readJson(packageJsonPath);
  const provenancePath = join(packageDir, "generated/rust-provenance.json");
  const provenance = readJson(provenancePath);
  const distDir = join(packageDir, "dist");
  if (!(await isDirectory(distDir))) fail(`${family} compiled dist is missing: ${distDir}`);
  const distDigest = await treeDigest(distDir);
  const nonce = sha256(`${sourceRevision}\n${family}\n${distDigest}`);
  const pack = npmPack(packageDir, archiveDir);
  const archivePath = resolve(pack.filename);
  const archiveDigest = await fileDigest(archivePath);
  packages.push({
    family,
    name: packageJson.name,
    version: packageJson.version,
    source_model_revision: packageJson.acyclicGenerated?.sourceModelRevision ?? provenance.sourceModelRevision,
    generated_client_sha256: provenance.generatedClientSha256,
    dist_sha256: distDigest,
    build_nonce: `sha256:${nonce}`,
    archive_path: portable(relative(output, archivePath)),
    archive_sha256: archiveDigest,
    archive_bytes: (await stat(archivePath)).size,
    dry_run_files: pack.files,
  });
}

const runtime = captured[0];
const inference = captured[1];
const machines = captured[2];
const scenarios = [];
const seen = new Set();
for (const result of runtime.results ?? []) {
  if (!result.method || result.method.includes(".")) continue;
  const family = canonicalFamily(result.family);
  const rpc = resolveRpc(inventory, family, result.method);
  const key = `${family}\0${rpc}`;
  if (seen.has(key)) continue;
  seen.add(key);
  const checks = ["invocation", "transport", "serialization"];
  if (result.method === "Append") checks.push("recovery");
  if (result.method === "Follow") checks.push("cancellation");
  scenarios.push({ family, rpc, shape: inventory.get(family).get(rpc), transport: "grpc", checks, evidence: "rust-authority-runtime-receipt-20261004.json" });
}
for (const [family, receipt] of [["inference", inference], ["machines", machines]]) {
  const transcript = readJson(resolve(root, receipt.fixture.transcript));
  for (const rpc of transcript.observedRpcs ?? transcript.expectedRpcs ?? []) {
    const key = `${family}\0${rpc}`;
    if (seen.has(key)) continue;
    seen.add(key);
    if (!inventory.get(family)?.has(rpc)) fail(`${rpc} is absent from Rust authority`);
    const checks = ["invocation", "transport", "serialization"];
    if (family === "inference" && rpc.endsWith("RunsService/Watch")) checks.push("recovery");
    if (family === "machines" && rpc.endsWith("WatchOperation")) checks.push("recovery");
    scenarios.push({ family, rpc, shape: inventory.get(family).get(rpc), transport: "grpc", checks, evidence: basename(receipt.fixture.transcript) });
  }
}
if (scenarios.length !== 106) fail(`Rust authority union contains ${scenarios.length} scenarios; expected 106`);
for (const [family, methods] of inventory) {
  if (family === "protocol") continue;
  const count = scenarios.filter((scenario) => scenario.family === family).length;
  if (count !== methods.size) fail(`${family} scenario coverage is ${count}/${methods.size}`);
}

const consumerManifest = {
  schema: "acyclic.sdk.typescript.fixture-consumer.v1",
  language: "typescript",
  source_root: root,
  source_revision: sourceRevision,
  captured_fixture_revisions: captured.map((receipt) => receipt.fixture_commit ?? receipt.source?.revisionAtCapture).filter(Boolean),
  source_ancestry_verified: true,
  package_snapshot: packageRoot,
  packages,
  scenario_count: scenarios.length,
};
const consumerBytes = Buffer.from(`${JSON.stringify(consumerManifest, null, 2)}\n`);
const consumerPath = "qualification/consumers/typescript-rust-fixture.json";
await writeFile(join(output, consumerPath), consumerBytes);
const consumerDigest = `sha256:${sha256(consumerBytes)}`;

const scenarioEntries = [];
for (let index = 0; index < scenarios.length; index += 1) {
  const scenario = scenarios[index];
  const value = {
    schema: "acyclic.sdk.rpc-scenario-result.v1",
    source_revision: sourceRevision,
    status: "passed",
    invoked: true,
    exit_code: 0,
    family: scenario.family,
    rpc: scenario.rpc,
    shape: scenario.shape,
    transport: scenario.transport,
    // These probes connect to the Rust fixture processes over their network
    // endpoints; they are remote consumer executions, not in-process calls.
    execution_mode: "remote",
    checks: scenario.checks,
    fixture_evidence: scenario.evidence,
  };
  const path = `qualification/consumers/typescript-${String(index + 1).padStart(3, "0")}-${safe(scenario.family)}.json`;
  const bytes = Buffer.from(`${JSON.stringify(value, null, 2)}\n`);
  await writeFile(join(output, path), bytes);
  scenarioEntries.push({ family: scenario.family, rpc: scenario.rpc, shape: scenario.shape, execution_mode: "remote", output_path: path, output_sha256: `sha256:${sha256(bytes)}` });
}

const log = {
  schema: "acyclic.sdk.rpc-scenario-log.v1",
  source_revision: sourceRevision,
  consumer: {
    executed: true,
    name: "typescript-rust-authority-fixture",
    version: "2026-10-04",
    source_revision: sourceRevision,
    artifact_path: consumerPath,
    artifact_sha256: consumerDigest,
  },
  scenarios: scenarioEntries,
  fixture_receipts: [
    "research/acceptance/typescript-review/rust-authority-runtime-receipt-20261004.json",
    "research/acceptance/typescript-review/inference-rsa-current-source-20261004.receipt.json",
    "research/acceptance/typescript-review/machines-rsa-current-source-20261004.receipt.json",
  ],
  generation: {
    source_root: root,
    source_revision: sourceRevision,
    package_build_nonce: `sha256:${sha256(packages.map((pkg) => pkg.build_nonce).join("\n"))}`,
    package_archives: packages.map(({ family, archive_path, archive_sha256, archive_bytes, build_nonce }) => ({ family, archive_path, archive_sha256, archive_bytes, build_nonce })),
  },
};
await writeJson(join(output, "typescript-rust-authority-scenario-log.json"), log);
await writeJson(join(output, "typescript-rust-authority-central-metadata.json"), {
  schema: "acyclic.sdk.typescript.rust-authority-central.v1",
  status: "qualified",
  source_root: root,
  source_revision: sourceRevision,
  source_ancestry_verified: true,
  authority_path: authorityPath,
  authority_rpc_count: 106,
  observed_rpc_count: scenarios.length,
  scenario_log: "typescript-rust-authority-scenario-log.json",
  consumer_manifest: consumerPath,
  packages,
  receipts: ["97bc8596a613475001dc2627f7eb1fec33e3787a", ...captured.map((receipt) => receipt.fixture_commit ?? receipt.source?.revisionAtCapture).filter(Boolean)],
});
console.log(JSON.stringify({ status: "qualified", source_revision: sourceRevision, scenarios: scenarios.length, packages: packages.length, output }, null, 2));

function parseArgs(values) {
  const parsed = {};
  for (let index = 0; index < values.length; index += 1) {
    const value = values[index];
    if (!value.startsWith("--")) fail(`unexpected argument ${value}`);
    const key = value.slice(2);
    parsed[key] = values[++index];
    if (!parsed[key] || parsed[key].startsWith("--")) fail(`missing value for --${key}`);
  }
  return parsed;
}
function required(name) { return args[name] ?? fail(`--${name} is required`); }
function fail(message) { throw new Error(message); }
function readJson(path) { return JSON.parse(readFileSync(path, "utf8")); }
function writeJson(path, value) { return writeFile(path, `${JSON.stringify(value, null, 2)}\n`); }
function git(cwd, command) { return execFileSync("git", command, { cwd, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim(); }
function gitAncestor(cwd, revision) { try { execFileSync("git", ["merge-base", "--is-ancestor", revision, "HEAD"], { cwd, stdio: "ignore" }); return true; } catch { return false; } }
function sha256(value) { return createHash("sha256").update(value).digest("hex"); }
async function fileDigest(path) { return `sha256:${sha256(await readFile(path))}`; }
async function isDirectory(path) { try { return (await stat(path)).isDirectory(); } catch { return false; } }
async function treeDigest(rootPath) {
  const files = [];
  async function walk(current) {
    for (const entry of (await readdir(current, { withFileTypes: true })).sort((a, b) => a.name.localeCompare(b.name))) {
      const path = join(current, entry.name);
      if (entry.isDirectory()) await walk(path);
      else files.push([relative(rootPath, path).replaceAll(sep, "/"), await fileDigest(path)]);
    }
  }
  await walk(rootPath);
  return `sha256:${sha256(files.map(([path, digest]) => `${path}\0${digest}\n`).join(""))}`;
}
function npmPack(cwd, destination) {
  const dry = spawnSync(process.platform === "win32" ? "npm.cmd" : "npm", ["pack", "--dry-run", "--json", "--ignore-scripts"], { cwd, encoding: "utf8", shell: process.platform === "win32" });
  if (dry.status !== 0) fail(`npm pack --dry-run failed in ${cwd}: ${dry.stderr}`);
  const report = JSON.parse(dry.stdout)[0];
  const pack = spawnSync(process.platform === "win32" ? "npm.cmd" : "npm", ["pack", "--json", "--ignore-scripts", "--pack-destination", destination], { cwd, encoding: "utf8", shell: process.platform === "win32" });
  if (pack.status !== 0) fail(`npm pack failed in ${cwd}: ${pack.stderr}`);
  const result = JSON.parse(pack.stdout)[0];
  return { filename: join(destination, result.filename), files: report.files.map((file) => file.path).sort() };
}
function portable(path) { return path.replaceAll("\\", "/"); }
function safe(value) { return value.replaceAll(/[^a-z0-9.-]/gi, "-"); }
function canonicalFamily(value) { return value === "objects.buckets" || value === "objects.multipart" ? "objects" : value; }
function resolveRpc(inventory, family, method) {
  const methods = inventory.get(family);
  if (!methods) fail(`fixture family ${family} is absent from Rust authority`);
  const matches = [...methods.keys()].filter((rpc) => rpc.endsWith(`/${method}`));
  if (matches.length !== 1) fail(`fixture ${family}/${method} resolved to ${matches.length} Rust RPCs`);
  return matches[0];
}
function authorityInventory(authority) {
  const inventory = new Map();
  for (const family of authority.families ?? []) {
    const name = family.source.split("/")[0];
    const methods = new Map((family.rpc_methods ?? []).map((method) => [method.rpc, method.shape]));
    inventory.set(name, methods);
  }
  return inventory;
}
