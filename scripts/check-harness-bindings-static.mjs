#!/usr/bin/env node

/**
 * Source-level qualification for the Harness public contract.
 *
 * This command deliberately does not build or execute Rust, WASM, or the
 * installed package. It catches source and generated-declaration drift and
 * records the exact inputs needed to interpret the runtime qualification
 * lanes. Missing runtime artifacts are reported as unverified evidence.
 */

import { createHash } from "node:crypto";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { harnessWasmSourceClosure } from "./harness-wasm-source-closure.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const args = new Set(process.argv.slice(2));
const outputIndex = process.argv.indexOf("--output");
const outputPath = outputIndex >= 0 ? process.argv[outputIndex + 1] : undefined;
const generatedWasmDirectoryIndex = process.argv.indexOf("--generated-wasm-dir");
const generatedWasmDirectoryArgument = generatedWasmDirectoryIndex >= 0
  ? process.argv[generatedWasmDirectoryIndex + 1]
  : undefined;

function pathFor(value) {
  return resolve(root, value);
}

function read(value) {
  return readFileSync(pathFor(value), "utf8");
}

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

function fileDigest(value) {
  const path = pathFor(value);
  return existsSync(path)
    ? { path: value, present: true, sha256: sha256(readFileSync(path)) }
    : { path: value, present: false };
}

const defaultGeneratedWasmDirectory = pathFor("typescript/packages/harness/generated/wasm");
const generatedWasmDirectory = generatedWasmDirectoryArgument === undefined
  ? defaultGeneratedWasmDirectory
  : resolve(generatedWasmDirectoryArgument);
function generatedArtifactPath(value) {
  return join(generatedWasmDirectory, value.slice(value.lastIndexOf("/") + 1));
}
function generatedArtifactDigest(value) {
  const path = generatedArtifactPath(value);
  return existsSync(path)
    ? { path: value, present: true, sha256: sha256(readFileSync(path)) }
    : { path: value, present: false };
}
function readGeneratedArtifact(value) {
  return readFileSync(generatedArtifactPath(value));
}

function git(args) {
  const result = spawnSync("git", args, {
    cwd: root,
    encoding: "utf8",
    windowsHide: true,
  });
  if (result.status !== 0) return undefined;
  return result.stdout.trim();
}

function fail(message) {
  failures.push(message);
}

const failures = [];
const wasmRustPath = "rust/crates/harness/src/wasm.rs";
const wasmRuntimePath = "typescript/packages/harness/src/wasm-runtime.ts";
const wasmDeclarationPath = "typescript/packages/harness/generated/wasm/acyclic_harness_wasm.d.ts";
const nativeContractsPath = "typescript/packages/harness/src/native-contracts.ts";

const wasmRust = read(wasmRustPath);
const wasmRuntime = read(wasmRuntimePath);
const wasmDeclaration = read(wasmDeclarationPath);
const nativeContracts = read(nativeContractsPath);

const wasmExports = [...wasmRust.matchAll(/js_name\s*=\s*([A-Za-z0-9_]+)/g)].map(match => match[1]);
const requiredArray = wasmRuntime.match(/REQUIRED_HARNESS_WASM_EXPORTS\s*=\s*\[([\s\S]*?)\]\s*as const/);
if (requiredArray === null) fail("required WASM export declaration is missing from TypeScript source");
const requiredExports = requiredArray === null
  ? []
  : [...requiredArray[1].matchAll(/\"([A-Za-z0-9_]+)\"/g)].map(match => match[1]);
const declarationExports = [...wasmDeclaration.matchAll(/^export function ([A-Za-z0-9_]+)/gm)].map(match => match[1]);
const wasmExportSet = new Set(wasmExports);
const declarationExportSet = new Set(declarationExports);

for (const name of requiredExports) {
  if (!wasmExportSet.has(name)) fail(`required WASM export is absent from Rust source: ${name}`);
  if (!declarationExportSet.has(name)) fail(`required WASM export is absent from generated declaration: ${name}`);
}

const nativeMethods = [
  "admitModelEvent",
  "prepareModelRequest",
  "admitTask",
  "admitBatch",
  "admitBatchRequest",
];
for (const name of nativeMethods) {
  if (!new RegExp(`\\b${name}\\s*\\(`).test(nativeContracts)) {
    fail(`NativeContracts method is absent from TypeScript source: ${name}`);
  }
  if (!declarationExportSet.has(name)) fail(`NativeContracts WASM dependency is absent from declaration: ${name}`);
}
if (!/validateModelInputManifest\s*\??:/.test(nativeContracts)) {
  fail("NativeContracts does not declare its model-input manifest dependency");
}
if (!declarationExportSet.has("validateModelInputManifest")) {
  fail("model-input manifest validator is absent from generated declaration");
}

const rustContracts = [
  ["budget admission", "rust/crates/harness/src/swarm_budget.rs", [
    /pub struct SwarmBudgetLimits\b/, /pub struct SwarmDispatchContext\b/, /pub struct SwarmRootDispatchContext\b/,
  ]],
  ["local swarm workspace", "rust/crates/harness/src/filesystem/swarm_local.rs", [
    /pub struct LocalSwarmConfig\b/, /pub struct LocalSwarmBindings\b/, /pub struct LocalSwarmSession\b/,
  ]],
  ["local workspace tools", "rust/crates/harness/src/filesystem/workspace_tools.rs", [
    /WORKSPACE_EDIT/, /WORKSPACE_READ/, /WORKSPACE_SEARCH/, /max_matches/,
  ]],
  ["host execution", "rust/crates/harness/src/host_execution.rs", [
    /pub trait ExecutionRunner\b/, /pub trait ExecutionApprovalVerifier\b/,
  ]],
  ["root writeback", "rust/crates/harness/src/filesystem/git_facade.rs", [
    /pub struct RootWritebackApproval\b/, /pub struct RootWritebackRequest\b/,
    /apply_root_writeback_plan_for_child_with_recovery_outcome/,
  ]],
  ["host operator constructor", "rust/crates/harness/src/host_execution.rs", [
    /pub struct ExecutionOperatorAuthorizer\b/, /pub(?:\(crate\))? fn new\(issuer: AuthorityIssuer\)/,
  ]],
  ["terminal and shell capability", "rust/crates/harness/src/bundle.rs", [
    /acyclic\.shell/, /interactive terminal session/,
  ]],
  ["execution provider", "rust/crates/harness/src/runtime.rs", [
    /pub trait ExecutionProvider\b/,
  ]],
];
for (const [label, path, patterns] of rustContracts) {
  const source = read(path);
  for (const pattern of patterns) {
    if (!pattern.test(source)) fail(`${label} contract is absent from Rust source: ${pattern}`);
  }
}

// These probes describe contracts currently being integrated by the runtime
// slices. They stay pending on older source revisions, but become enforcing as
// soon as a marker lands, preventing a partial public-contract rollout.
const deferredContracts = [
  {
    name: "provider transport context v6",
    files: [
      "rust/crates/harness/src/model.rs",
      "rust/crates/harness/src/executor.rs",
      "rust/crates/harness/src/swarm_budget.rs",
    ],
    marker: /ProviderDispatchContext|provider_dispatch_context|acyclic\.stock\.v6/,
    required: [
      ["rust/crates/harness/src/model.rs", /pub struct ProviderDispatchContext\b/],
      ["rust/crates/harness/src/model.rs", /generate_with_dispatch/],
      ["rust/crates/harness/src/executor.rs", /provider_dispatch_context/],
      ["rust/crates/harness/src/swarm_budget.rs", /provider_dispatch_context/],
      ["rust/crates/harness/src/executor.rs", /acyclic\.stock\.v6/],
    ],
  },
  {
    name: "sealed root writeback",
    files: [
      "rust/crates/harness/src/filesystem/swarm_local.rs",
      "rust/crates/harness/src/filesystem/git_facade.rs",
    ],
    marker: /LocalApprovedRootWriteback\b|issue_approved_root_writeback\b/,
    required: [
      ["rust/crates/harness/src/filesystem/swarm_local.rs", /pub struct LocalApprovedRootWriteback\b/],
      ["rust/crates/harness/src/filesystem/swarm_local.rs", /issue_approved_root_writeback/],
      ["rust/crates/harness/src/filesystem/swarm_local.rs", /apply_with_recovery/],
      ["rust/crates/harness/src/filesystem/swarm_local.rs", /continue_with_recovery/],
    ],
  },
  {
    name: "model dispatch context and usage",
    files: [
      "rust/crates/harness/src/model.rs",
      "rust/crates/harness/src/executor.rs",
      "rust/crates/harness/src/runtime.rs",
      "rust/crates/harness/src/swarm_budget.rs",
      "rust/crates/harness/src/swarm_budget_journal.rs",
    ],
    marker: /ModelDispatchContext\b|model_dispatch_context|model_usage_receipt/,
    required: [
      ["rust/crates/harness/src/model.rs", /pub struct ModelDispatchContext\b/],
      ["rust/crates/harness/src/model.rs", /usage/],
      ["rust/crates/harness/src/executor.rs", /ModelDispatchContext\b|model_dispatch_context/],
      ["rust/crates/harness/src/runtime.rs", /usage/],
      ["rust/crates/harness/src/swarm_budget.rs", /usage/],
    ],
  },
];
const deferredResults = deferredContracts.map(contract => {
  const contents = contract.files.map(path => [path, read(path)]);
  const active = contents.some(([, source]) => contract.marker.test(source));
  const missing = active
    ? contract.required.filter(([path, pattern]) => !pattern.test(read(path))).map(([path, pattern]) => `${path}: ${pattern}`)
    : [];
  for (const item of missing) fail(`${contract.name} contract is incomplete: ${item}`);
  return { name: contract.name, status: active ? (missing.length === 0 ? "active" : "failed") : "pending", files: contract.files.map(fileDigest), missing };
});

const packageManifest = JSON.parse(read("typescript/packages/harness/package.json"));
const packageExports = Object.entries(packageManifest.exports ?? {}).map(([name, value]) => ({
  name,
  types: value.types,
  default: value.default,
  typesPresent: existsSync(pathFor(join("typescript/packages/harness", value.types))),
  defaultPresent: existsSync(pathFor(join("typescript/packages/harness", value.default))),
}));
const missingRuntimeArtifacts = packageExports.flatMap(entry => [
  ...(entry.typesPresent ? [] : [`${entry.name}:types:${entry.types}`]),
  ...(entry.defaultPresent ? [] : [`${entry.name}:default:${entry.default}`]),
]);
const generatedWasmArtifacts = [
  "typescript/packages/harness/generated/wasm/acyclic_harness_wasm.js",
  "typescript/packages/harness/generated/wasm/acyclic_harness_wasm.d.ts",
  "typescript/packages/harness/generated/wasm/acyclic_harness_wasm_bg.wasm",
  "typescript/packages/harness/generated/wasm/acyclic_harness_wasm_bg.wasm.d.ts",
];
const generatedWasmManifestPath = "typescript/packages/harness/generated/wasm/acyclic_harness_wasm.manifest.json";
const generatedWasmChecks = generatedWasmArtifacts.map(path => ({
  path,
  present: existsSync(generatedArtifactPath(path)),
  digest: generatedArtifactDigest(path),
}));
const generatedWasmManifestCheck = generatedArtifactDigest(generatedWasmManifestPath);
const generationInputPaths = harnessWasmSourceClosure(root);
const generationInputDigests = generationInputPaths.map(fileDigest);
const generatedWasmValidationFailures = [];
let generatedWasmManifest;
if (generatedWasmManifestCheck.present) {
  try {
    generatedWasmManifest = JSON.parse(readGeneratedArtifact(generatedWasmManifestPath));
  } catch (error) {
    generatedWasmValidationFailures.push(`generated WASM manifest is not valid JSON: ${error.message}`);
  }
}
if (generatedWasmManifest !== undefined) {
  if (generatedWasmManifest.version !== 1) {
    generatedWasmValidationFailures.push("generated WASM manifest version is unsupported");
  }
  if (generatedWasmManifest.generator !== "scripts/build-harness-wasm.mjs") {
    generatedWasmValidationFailures.push("generated WASM manifest generator does not match the owning build script");
  }
  if (generatedWasmManifest.wasmBindgen !== "0.2.117") {
    generatedWasmValidationFailures.push("generated WASM manifest does not pin wasm-bindgen 0.2.117");
  }
  const expectedInputs = new Map(generationInputDigests.map(item => [item.path, item.sha256]));
  const actualInputs = new Map((generatedWasmManifest.sourceSnapshot ?? []).map(item => [item.path, item.sha256]));
  for (const [path, digest] of expectedInputs) {
    if (actualInputs.get(path) !== digest) generatedWasmValidationFailures.push(`generated WASM source input is stale: ${path}`);
  }
  for (const path of actualInputs.keys()) {
    if (!expectedInputs.has(path)) generatedWasmValidationFailures.push(`generated WASM manifest has an unexpected source input: ${path}`);
  }
  const expectedArtifacts = new Map((generatedWasmManifest.artifacts ?? []).map(item => [item.path, item.sha256]));
  for (const item of generatedWasmChecks) {
    if (!item.present) {
      generatedWasmValidationFailures.push(`generated WASM artifact is missing: ${item.path}`);
      continue;
    }
    if (expectedArtifacts.get(item.path.replace("typescript/packages/harness/generated/wasm/", "")) !== item.digest.sha256) {
      generatedWasmValidationFailures.push(`generated WASM artifact digest mismatch: ${item.path}`);
    }
  }
}
const generatedWasmContentChecks = [];
const generatedJavaScript = generatedWasmChecks.find(item => item.path.endsWith("acyclic_harness_wasm.js"));
if (generatedJavaScript?.present) {
  const source = readGeneratedArtifact(generatedJavaScript.path).toString("utf8");
  const missing = requiredExports.filter(name => !source.includes(name));
  generatedWasmContentChecks.push({ path: generatedJavaScript.path, valid: missing.length === 0, missing });
  if (missing.length > 0) generatedWasmValidationFailures.push(`generated WASM JavaScript is missing exports: ${missing.join(", ")}`);
}
const generatedWasmBinary = generatedWasmChecks.find(item => item.path.endsWith("acyclic_harness_wasm_bg.wasm"));
if (generatedWasmBinary?.present) {
  const bytes = readGeneratedArtifact(generatedWasmBinary.path);
  const valid = bytes.length >= 4 && bytes[0] === 0 && bytes[1] === 97 && bytes[2] === 115 && bytes[3] === 109;
  generatedWasmContentChecks.push({ path: generatedWasmBinary.path, valid });
  if (!valid) generatedWasmValidationFailures.push("generated WASM binary does not have the WebAssembly magic header");
}
const generatedTypeScript = generatedWasmChecks.find(item => item.path.endsWith("acyclic_harness_wasm.d.ts"));
if (generatedTypeScript?.present) {
  const source = readGeneratedArtifact(generatedTypeScript.path).toString("utf8");
  const missing = requiredExports.filter(name => !new RegExp(`export function ${name}\\b`).test(source));
  generatedWasmContentChecks.push({ path: generatedTypeScript.path, valid: missing.length === 0, missing });
  if (missing.length > 0) generatedWasmValidationFailures.push(`generated WASM declaration is missing exports: ${missing.join(", ")}`);
}
if (generatedWasmValidationFailures.length > 0) {
  for (const failure of generatedWasmValidationFailures) fail(failure);
}
const generatedWasmReady = generatedWasmManifestCheck.present
  && generatedWasmChecks.every(item => item.present)
  && generatedWasmValidationFailures.length === 0;
const generatedWasmStatus = generatedWasmValidationFailures.length > 0
  ? "failed"
  : generatedWasmReady
    ? "passed"
    : "unverified";

const contractSources = [
  wasmRustPath,
  wasmRuntimePath,
  wasmDeclarationPath,
  nativeContractsPath,
  "rust/crates/harness/src/swarm_budget.rs",
  "rust/crates/harness/src/filesystem/swarm_local.rs",
  "rust/crates/harness/src/filesystem/workspace_tools.rs",
  "rust/crates/harness/src/host_execution.rs",
  "rust/crates/harness/src/runtime.rs",
  "rust/crates/harness/src/model.rs",
  "rust/crates/harness/src/executor.rs",
  "rust/crates/harness/src/filesystem/git_facade.rs",
  "rust/crates/harness/src/bundle.rs",
  "typescript/packages/harness/package.json",
  "proto/harness/v2/harness.proto",
  "conformance/vectors/harness/model-input-v3.json",
  "conformance/vectors/harness/model-input-rejection-v1.json",
  "conformance/vectors/harness/provider-consumption-v1.json",
  "rust/crates/harness/src/provider_conformance.rs",
  "typescript/packages/harness/test/provider-consumption-v1.test.ts",
  "scripts/fixtures/installed-harness/test/model-input-rejection.test.ts",
  "scripts/fixtures/installed-harness/test/provider-consumption.test.ts",
  "scripts/fixtures/graphcoder-qualification/consumer.mjs",
  "scripts/check-harness-package.sh",
  "typescript/packages/harness/generated/proto/harness/v2/harness_pb.js",
  "typescript/packages/harness/generated/proto/harness/v2/harness_pb.d.ts",
  ...generatedWasmArtifacts,
  generatedWasmManifestPath,
];

const surfaceMappings = [
  {
    contract: "model and task admission",
    rust: [wasmRustPath, "rust/crates/harness/src/runtime.rs"],
    generated: [wasmRuntimePath, wasmDeclarationPath],
    typescript: [nativeContractsPath],
    runtimeTests: ["scripts/fixtures/installed-harness/test/provider-consumption.test.ts"],
    binding: "wasm-backed",
  },
  {
    contract: "swarm budget admission",
    rust: ["rust/crates/harness/src/swarm_budget.rs", "rust/crates/harness/src/runtime.rs"],
    generated: [],
    runtimeTests: [
      "rust/crates/harness/tests/swarm_budget_contracts_v2.rs",
      "rust/crates/harness/tests/swarm_budget_usage_contracts_v2.rs",
    ],
    binding: "native-rust-host-boundary",
  },
  {
    contract: "generation-pinned workspace tools",
    rust: ["rust/crates/harness/src/filesystem/workspace_tools.rs", "rust/crates/harness/src/filesystem/swarm_local.rs"],
    generated: [],
    runtimeTests: [],
    binding: "native-rust-host-boundary",
    qualification: "native-runtime-required",
  },
  {
    contract: "terminal and shell execution",
    rust: ["rust/crates/harness/src/bundle.rs", "rust/crates/harness/src/host_execution.rs"],
    generated: [],
    runtimeTests: ["rust/crates/harness/src/host_execution.rs"],
    binding: "native-host-boundary",
  },
  {
    contract: "provider transport context v6",
    rust: ["rust/crates/harness/src/model.rs", "rust/crates/harness/src/executor.rs", "rust/crates/harness/src/swarm_budget.rs"],
    generated: [],
    runtimeTests: ["rust/crates/harness/tests/swarm_provider_support.rs"],
    binding: "native-rust-host-boundary",
    qualification: "deferred-contract-probe",
  },
  {
    contract: "sealed root writeback",
    rust: ["rust/crates/harness/src/filesystem/swarm_local.rs", "rust/crates/harness/src/filesystem/git_facade.rs"],
    generated: [],
    runtimeTests: [
      "rust/crates/harness/tests/root_writeback_acceptance.rs",
      "rust/crates/harness/tests/git_facade.rs",
    ],
    binding: "native-host-boundary",
    qualification: "deferred-contract-probe",
  },
  {
    contract: "ModelDispatchContext and usage",
    rust: [
      "rust/crates/harness/src/model.rs",
      "rust/crates/harness/src/executor.rs",
      "rust/crates/harness/src/runtime.rs",
      "rust/crates/harness/src/swarm_budget.rs",
    ],
    generated: [],
    runtimeTests: [
      "rust/crates/harness/tests/swarm_provider_support.rs",
      "rust/crates/harness/tests/local_model_swarm.rs",
    ],
    binding: "native-rust-host-boundary",
    qualification: "deferred-contract-probe",
  },
  {
    contract: "host operator constructor",
    rust: ["rust/crates/harness/src/host_execution.rs"],
    generated: [],
    runtimeTests: ["rust/crates/harness/src/host_execution.rs"],
    binding: "native-host-boundary",
    qualification: "source-owned",
  },
];
const surfaceChecks = surfaceMappings.flatMap(mapping => [
  ...mapping.rust,
  ...mapping.generated,
  ...(mapping.typescript ?? []),
].map(path => ({
  contract: mapping.contract,
  path,
  present: existsSync(pathFor(path)),
  kind: "ownership",
})).concat((mapping.runtimeTests ?? []).map(path => ({
  contract: mapping.contract,
  path,
  present: existsSync(pathFor(path)),
  kind: "runtime-test",
}))));
for (const check of surfaceChecks) {
  if (!check.present) fail(`surface mapping input is missing for ${check.contract}: ${check.path}`);
}
const publicContractAudit = [
  {
    contract: "SwarmBudgetLimits / SwarmDispatchContext",
    owner: "rust/crates/harness/src/swarm_budget.rs",
    generatedExposure: "none",
    reason: "native host budget admission; transport provenance is out of band",
  },
  {
    contract: "WorkspaceToolsBinding (acyclic.edit/read/search)",
    owner: "rust/crates/harness/src/filesystem/workspace_tools.rs",
    generatedExposure: "none",
    reason: "model-facing workspace tools are admitted by the native Harness runtime and return generation-pinned refs; no standalone generated binding is exposed",
    qualification: "native-runtime-required",
  },
  {
    contract: "RootWritebackApproval / LocalApprovedRootWriteback",
    owner: "rust/crates/harness/src/filesystem/git_facade.rs",
    generatedExposure: "none",
    reason: "approval-bound native filesystem publication; never model-visible",
  },
  {
    contract: "ExecutionOperatorAuthorizer",
    owner: "rust/crates/harness/src/host_execution.rs",
    generatedExposure: "none",
    reason: "host-only signer; credentials and authority stay outside model input",
  },
  {
    contract: "ProviderDispatchContext v6",
    owner: "rust/crates/harness/src/model.rs",
    generatedExposure: "none",
    reason: "authenticated provider transport metadata; must remain outside serialized model requests",
    qualification: "deferred-contract-probe",
  },
  {
    contract: "ModelDispatchContext and usage",
    owner: "rust/crates/harness/src/model.rs",
    generatedExposure: "none",
    reason: "provider usage and dispatch provenance remain transport metadata outside model input",
    qualification: "deferred-contract-probe",
  },
].map(value => ({ ...value, ownerPresent: existsSync(pathFor(value.owner)) }));
for (const contract of publicContractAudit) {
  if (!contract.ownerPresent) fail(`public contract owner is missing: ${contract.owner}`);
}
const consumerChecks = [
  "scripts/fixtures/installed-harness/test/model-input-rejection.test.ts",
  "scripts/fixtures/installed-harness/test/provider-consumption.test.ts",
].map(path => ({ path, present: existsSync(pathFor(path)) }));
for (const check of consumerChecks) {
  if (!check.present) fail(`installed package consumer fixture is missing: ${check.path}`);
}
const qualificationCommands = [
  {
    id: "static-bindings",
    command: "node scripts/check-harness-bindings-static.mjs",
    evidence: "source, declaration, fixture, and ownership digest",
    status: failures.length === 0 ? "passed" : "failed",
  },
  {
    id: "generated-contracts",
    command: "bun run check:generated",
    evidence: "generator output and canonical source copies",
    status: "not-run",
  },
  {
    id: "harness-rust",
    command: "cargo test -p acyclic-harness --all-features --locked",
    evidence: "native Harness contract and conformance tests",
    status: "not-run",
  },
  {
    id: "harness-wasm-build",
    command: "bun scripts/build-harness-wasm.mjs",
    evidence: "acyclic_harness_wasm.js/.wasm/.d.ts generated by wasm-bindgen 0.2.117",
    status: "not-run",
  },
  {
    id: "harness-wasm-fresh-artifacts",
    command: "bun scripts/build-harness-wasm.mjs <ABSOLUTE_EMPTY_OUTPUT> && node scripts/check-harness-bindings-static.mjs --generated-wasm-dir <ABSOLUTE_EMPTY_OUTPUT>",
    evidence: "fresh output directory, pinned wasm-bindgen 0.2.117, source-closure snapshot, and artifact digests",
    status: "not-run",
  },
  {
    id: "harness-typescript",
    command: "bun x tsc -p typescript/packages/harness/tsconfig.json --noEmit",
    evidence: "TypeScript Harness package against generated WASM/proto declarations",
    status: "not-run",
  },
  {
    id: "installed-consumer",
    command: "bash scripts/check-harness-package.sh <ABSOLUTE_OUTPUT>",
    evidence: "packed @acyclic-labs/harness import and consumer fixtures",
    status: "not-run",
  },
];

const result = {
  contract: "harness-bindings-static-v1",
  qualification: "static-source-only",
  repository: {
    root,
    commit: git(["rev-parse", "HEAD"]) ?? null,
    dirty: (git(["status", "--porcelain"]) ?? "") !== "",
  },
  sourceDigests: contractSources.map(fileDigest),
  wasm: {
    rustExportCount: wasmExportSet.size,
    requiredExportCount: requiredExports.length,
    declarationExportCount: declarationExportSet.size,
    requiredExports,
    missingFromRust: requiredExports.filter(name => !wasmExportSet.has(name)),
    missingFromDeclaration: requiredExports.filter(name => !declarationExportSet.has(name)),
  },
  rustContracts: rustContracts.map(([label, path]) => ({ label, source: fileDigest(path) })),
  surfaceMappings,
  surfaceChecks,
  publicContractAudit,
  deferredContracts: deferredResults,
  consumers: consumerChecks,
  requestCapture: {
    boundary: "prepareModelRequest",
    replay: "serialized request bytes and manifest digest are compared by native, generated WASM, and installed package fixtures",
    providerAdapter: "not-exposed",
  },
  qualificationCommands,
  package: {
    name: packageManifest.name,
    exports: packageExports,
    missingRuntimeArtifacts,
    generatedWasm: {
      artifacts: generatedWasmChecks,
      content: generatedWasmContentChecks,
      manifest: generatedWasmManifestCheck,
      sourceInputs: generationInputDigests,
      validationFailures: generatedWasmValidationFailures,
      ready: generatedWasmReady,
      status: generatedWasmStatus,
      missing: [
        ...generatedWasmChecks.filter(item => !item.present).map(item => item.path),
        ...(generatedWasmManifestCheck.present ? [] : [generatedWasmManifestPath]),
      ],
    },
  },
  runtimePrerequisites: [
    { command: "bun --version", requiredFor: ["generated-contracts", "harness-wasm-build", "harness-typescript", "installed-consumer"] },
    { command: "cargo --version", requiredFor: ["harness-rust", "harness-wasm-build", "installed-consumer"] },
    { command: "rustup target list --installed", requiredFor: ["harness-wasm-build"], requires: "wasm32-unknown-unknown" },
    { command: "wasm-bindgen --version", requiredFor: ["harness-wasm-build"], requires: "wasm-bindgen 0.2.117" },
    { command: "bash --version", requiredFor: ["installed-consumer"], note: "Git Bash or WSL on Windows" },
    { command: "cygpath --version", requiredFor: ["installed-consumer"], note: "Git Bash Windows path conversion; WSL may provide wslpath instead" },
    { command: "tar --version", requiredFor: ["installed-consumer"] },
    { command: "sha256sum --version", requiredFor: ["installed-consumer"] },
  ],
  runtime: {
    native: "not-run",
    wasm: "not-run",
    installedPackage: "not-run",
    reason: "This entrypoint performs source and declaration drift checks only.",
  },
  status: failures.length > 0 ? "failed" : generatedWasmReady ? "passed" : "unverified",
  failures,
};

const serialized = `${JSON.stringify(result, null, 2)}\n`;
if (outputPath !== undefined) writeFileSync(resolve(outputPath), serialized);
if (args.has("--json") || outputPath === undefined) process.stdout.write(serialized);
process.exitCode = failures.length === 0 ? 0 : 1;
