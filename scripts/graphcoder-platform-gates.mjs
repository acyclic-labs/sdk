#!/usr/bin/env node

// Validate and optionally run the portable GraphCoder qualification gates.
// Commands are argv vectors and are never passed through a shell. The runner
// records source and command digests so a result cannot be detached from the
// checkout or silently reused for another gate definition.

import { createHash, randomUUID } from "node:crypto";
import { execFileSync, spawnSync } from "node:child_process";
import { existsSync, lstatSync, readFileSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { basename, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnOwnedProcess, terminateOwnedProcessTree, trackOwnedProcess } from "./graphcoder-process-ownership.mjs";
import { assertCanonicalParents, ensureOwnedDirectory, isWithin } from "./graphcoder-path-ownership.mjs";
import { gitEnvironment, workingPathDigests, workingTreeDigest } from "./graphcoder-source-fence.mjs";

const MANIFEST_PATH = "docs/graphcoder-swarm/platform-gates.json";
const ROOT = resolve(fileURLToPath(new URL("..", import.meta.url)));
const PROTOCOL = "acyclic.graphcoder.platform-gates.v1";
const PLATFORMS = new Set(["windows", "linux", "macos"]);
const KINDS = new Set(["native", "compile", "package"]);
const HEX40 = /^[0-9a-f]{40}$/u;
const HEX64 = /^[0-9a-f]{64}$/u;
const EXECUTION_COUNT_LINE = /^graphcoder-executed-count:\s*(\d+)\s*$/gmu;
const CASE_WITNESS_LINE = /^graphcoder-case:\s*([A-Z][A-Z0-9-]*-\d+)\s+([a-z][a-z0-9._-]*)\s+passed\s*$/gmu;
const REQUIREMENT_ID = /^[A-Z][A-Z0-9-]*-\d+$/u;
const ASSERTION_NAME = /^[a-z][a-z0-9._-]*$/u;
const WINDOWS_NATIVE_COMPANION_ARCHIVE = "target/graphcoder-package-qualification/acyclic-fs-native-package.tgz";
const SAFE_ENVIRONMENT_KEYS = new Set([
  "PATH", "PATHEXT", "SYSTEMROOT", "TEMP", "TMP",
  "CARGO_HOME", "CARGO_TARGET_DIR", "RUSTUP_HOME", "RUSTFLAGS", "RUSTDOCFLAGS",
  "CI", "NUMBER_OF_PROCESSORS", "PROCESSOR_ARCHITECTURE", "NPM_CONFIG_CACHE",
  "ACYCLIC_FS_NATIVE_BINDING", "ACYCLIC_FS_NATIVE_BINDING_RECEIPT",
]);
const SECRET_ENVIRONMENT = /(?:TOKEN|PASSWORD|SECRET|CREDENTIAL|AUTH|PRIVATE_KEY|ACCESS_KEY)/iu;
const RUNTIME_ENVIRONMENT = /^(?:PATH|PATHEXT|SystemRoot|TEMP|TMP|CI|NUMBER_OF_PROCESSORS|PROCESSOR_ARCHITECTURE)$/iu;
const QUALIFICATION_ENVIRONMENT_KEYS = new Set([
  "PATH", "PATHEXT", "SystemRoot", "WINDIR", "COMSPEC",
  "GRAPHCODER_PACKAGE_ROOT", "GRAPHCODER_PACKAGE_ARTIFACT", "GRAPHCODER_BRIDGE_EXECUTABLE",
  "GRAPHCODER_BRIDGE_ARGS_JSON", "GRAPHCODER_BRIDGE_ENV_JSON", "GRAPHCODER_BRIDGE_CWD",
  "GRAPHCODER_IDENTITY_PATH", "GRAPHCODER_REQUIRE_PACKAGE_IDENTITY", "GRAPHCODER_LAZY_OBSERVATION_PATH",
  "GRAPHCODER_REQUIRE_LAZY_COUNTERS", "GRAPHCODER_NODE", "GRAPHCODER_CONHOST",
  "GRAPHCODER_PTY_TRANSCRIPT_PATH", "GRAPHCODER_PTY_LIFECYCLE_PATH", "GRAPHCODER_PTY_COMMAND_EXPECTATIONS_JSON",
  "GRAPHCODER_SWARM_EXPECTED_FILES_JSON", "GRAPHCODER_SWARM_EXPECTED_APPROVAL_JSON", "GRAPHCODER_SWARM_EXPECTED_COMMAND_JSON",
  "GRAPHCODER_SWARM_CHECKOUT_ROOT",
  "GRAPHCODER_SWARM_CONCURRENT_EDIT_JSON",
]);

const hash = value => createHash("sha256").update(value).digest("hex");
const readJson = path => JSON.parse(readFileSync(resolve(ROOT, path), "utf8"));
const fail = message => { throw new Error(`graphcoder-platform-gates: ${message}`); };

function git(...args) {
  return execFileSync("git", ["-C", ROOT, ...args], {
    encoding: "utf8",
    windowsHide: true,
    env: { ...runtimeEnvironment(process.env), ...gitEnvironment() },
  }).trim();
}

export function platformName() {
  if (process.platform === "win32") return "windows";
  if (process.platform === "darwin") return "macos";
  if (process.platform === "linux") return "linux";
  fail(`unsupported host platform: ${process.platform}`);
}

function requestedPlatform(value) {
  const actual = platformName();
  const requested = value ?? actual;
  if (!PLATFORMS.has(requested)) fail(`unsupported requested platform: ${requested}`);
  if (requested !== actual) fail(`requested platform ${requested} does not match host platform ${actual}`);
  return requested;
}

function validateManifest(manifest) {
  if (!manifest || manifest.protocol !== PROTOCOL) fail("manifest protocol is invalid");
  if (manifest.base_commit !== "31b9ff52d63c91f2b9bf87e16b78ad682d26546f") fail("manifest base commit is invalid");
  if (!Array.isArray(manifest.gates) || manifest.gates.length === 0) fail("manifest has no gates");
  validateQualificationLanes(manifest);
  const ids = new Set();
  for (const [index, gate] of manifest.gates.entries()) {
    if (!gate || typeof gate !== "object") fail(`gate ${index} is not an object`);
    for (const field of ["id", "area", "execution_kind", "evidence"]) {
      if (typeof gate[field] !== "string" || gate[field].trim() === "") fail(`gate ${index} lacks ${field}`);
    }
    if (!/^[a-z][a-z0-9-]+$/u.test(gate.id)) fail(`gate ${index} id is invalid`);
    if (ids.has(gate.id)) fail(`duplicate gate id ${gate.id}`);
    ids.add(gate.id);
    if (!KINDS.has(gate.execution_kind)) fail(`${gate.id} execution_kind is invalid`);
    if (!Array.isArray(gate.platforms) || gate.platforms.length === 0 || gate.platforms.some(platform => !PLATFORMS.has(platform))) {
      fail(`${gate.id} platforms are invalid`);
    }
    if (!gate.command || typeof gate.command !== "object" || typeof gate.command.executable !== "string" || gate.command.executable.trim() === "") {
      fail(`${gate.id} command executable is invalid`);
    }
    if (!Array.isArray(gate.command.args) || gate.command.args.some(arg => typeof arg !== "string")) fail(`${gate.id} command args are invalid`);
    const validateProducedArtifactPaths = (paths, label) => {
      if (!Array.isArray(paths) || paths.some(path => {
        if (typeof path !== "string" || path.trim() === "") return true;
        const normalized = path.replaceAll("\\", "/");
        const segments = normalized.split("/");
        return normalized.includes("\0") || normalized.startsWith("/") || /^[A-Za-z]:\//u.test(normalized) || segments.some(segment => segment === "" || segment === "." || segment === "..");
      })) fail(`${gate.id} ${label} must be relative paths without parent traversal`);
    };
    validateProducedArtifactPaths(gate.produced_artifacts ?? [], "produced_artifacts");
    if (gate.produced_artifacts_by_platform !== undefined) {
      if (!gate.produced_artifacts_by_platform || typeof gate.produced_artifacts_by_platform !== "object" || Array.isArray(gate.produced_artifacts_by_platform)) fail(`${gate.id} produced_artifacts_by_platform is invalid`);
      for (const [platform, paths] of Object.entries(gate.produced_artifacts_by_platform)) {
        if (!PLATFORMS.has(platform) || !gate.platforms.includes(platform)) fail(`${gate.id} produced_artifacts_by_platform has unsupported platform ${platform}`);
        validateProducedArtifactPaths(paths, `produced_artifacts_by_platform.${platform}`);
      }
    }
    for (const platform of gate.platforms) {
      const paths = [...(gate.produced_artifacts ?? []), ...(gate.produced_artifacts_by_platform?.[platform] ?? [])];
      if (new Set(paths).size !== paths.length) fail(`${gate.id} produced artifacts repeat a path for ${platform}`);
    }
    if (gate.id === "graphcoder-package" && gate.platforms.includes("windows") && !gate.produced_artifacts_by_platform?.windows?.includes(WINDOWS_NATIVE_COMPANION_ARCHIVE)) {
      fail(`${gate.id} must declare the Windows native companion archive`);
    }
    if (!Number.isInteger(gate.timeout_ms) || gate.timeout_ms <= 0) fail(`${gate.id} timeout_ms is invalid`);
  }
  return manifest;
}

function producedArtifactPaths(gate, platform) {
  return [...(gate.produced_artifacts ?? []), ...(gate.produced_artifacts_by_platform?.[platform] ?? [])];
}

function validateQualificationLanes(manifest) {
  if (!Array.isArray(manifest.qualification_lanes) || manifest.qualification_lanes.length === 0) fail("manifest has no qualification lanes");
  const ids = new Set();
  const gateIds = new Set(manifest.gates.map(gate => gate.id));
  for (const [index, lane] of manifest.qualification_lanes.entries()) {
    if (!lane || typeof lane !== "object") fail(`qualification lane ${index} is not an object`);
    if (typeof lane.id !== "string" || !/^[a-z][a-z0-9-]+$/u.test(lane.id)) fail(`qualification lane ${index} id is invalid`);
    if (ids.has(lane.id)) fail(`duplicate qualification lane id ${lane.id}`);
    ids.add(lane.id);
    if (!Array.isArray(lane.execution_kinds) || lane.execution_kinds.length === 0 || lane.execution_kinds.some(kind => !KINDS.has(kind) && kind !== "pty")) fail(`${lane.id} execution_kinds are invalid`);
    if (!Array.isArray(lane.platforms) || lane.platforms.length === 0 || lane.platforms.some(platform => !PLATFORMS.has(platform))) fail(`${lane.id} platforms are invalid`);
    for (const field of ["driver", "scenario", "configurator", "capture", "native_pty_driver", "negative_driver"]) {
      if (lane[field] !== undefined && (typeof lane[field] !== "string" || lane[field].trim() === "")) fail(`${lane.id} ${field} is invalid`);
      if (lane[field] !== undefined && !existsSync(resolve(ROOT, lane[field]))) fail(`${lane.id} ${field} does not exist: ${lane[field]}`);
    }
    const receiptEnvironments = [lane.receipt_environment, ...Object.values(lane.receipt_environment_by_execution_kind ?? {})];
    for (const environmentName of receiptEnvironments) {
      if (environmentName !== undefined && (typeof environmentName !== "string" || !/^[A-Z][A-Z0-9_]+$/u.test(environmentName))) fail(`${lane.id} receipt environment is invalid`);
    }
    if (lane.runtime_artifact_by_platform !== undefined) {
      if (!lane.runtime_artifact_by_platform || typeof lane.runtime_artifact_by_platform !== "object") fail(`${lane.id} runtime_artifact_by_platform is invalid`);
      for (const platform of lane.platforms) {
        const artifact = lane.runtime_artifact_by_platform[platform];
        if (typeof artifact !== "string" || artifact.trim() === "" || artifact.includes("..") || /^[A-Za-z]:[\\/]/u.test(artifact) || artifact.startsWith("/")) fail(`${lane.id} runtime artifact for ${platform} is invalid`);
      }
    }
    if (lane.gate_ids !== undefined && (!Array.isArray(lane.gate_ids) || lane.gate_ids.length === 0 || lane.gate_ids.some(id => !gateIds.has(id)))) fail(`${lane.id} gate_ids reference an unknown gate`);
    if (lane.requires_installed_package === true && lane.allows_mock_fixture === true) fail(`${lane.id} cannot allow a mock fixture with an installed package requirement`);
    if (lane.requires_lazy_observation === true && lane.id !== "installed-pty" && !Array.isArray(lane.required_markers)) fail(`${lane.id} must declare required markers with lazy observation`);
  }
}

function readLaneReceipt(path, lane, executionKind, source, platform) {
  const receiptPath = resolve(path);
  if (!existsSync(receiptPath)) fail(`${lane.id} ${executionKind} receipt is missing: ${receiptPath}`);
  const qualifiedWorktree = source.canonical_worktree;
  if (typeof qualifiedWorktree !== "string" || qualifiedWorktree.trim() === "") fail(`${lane.id} ${executionKind} receipt source worktree is missing`);
  const receiptRoot = resolve(qualifiedWorktree);
  if (!isWithin(receiptRoot, receiptPath)) fail(`${lane.id} ${executionKind} receipt path escapes the qualified worktree`);
  assertCanonicalParents(receiptRoot, receiptPath, `${lane.id} ${executionKind} receipt`, false);
  const receiptMetadata = lstatSync(receiptPath);
  if (!receiptMetadata.isFile() || receiptMetadata.isSymbolicLink()) fail(`${lane.id} ${executionKind} receipt is not a regular file`);
  let record;
  try { record = JSON.parse(readFileSync(receiptPath, "utf8")); }
  catch (error) { fail(`${lane.id} ${executionKind} receipt is invalid JSON: ${error instanceof Error ? error.message : String(error)}`); }
  const suite = record?.suite;
  if (!suite || suite.status !== "passed") fail(`${lane.id} ${executionKind} receipt is not passed`);
  if (suite.execution_kind !== executionKind || suite.platform !== platform) fail(`${lane.id} ${executionKind} receipt execution identity is invalid`);
  if (!Array.isArray(record.artifacts) || record.artifacts.length === 0) fail(`${lane.id} ${executionKind} receipt has no artifact evidence`);
  if (typeof suite.id !== "string" || suite.id.trim() === "") fail(`${lane.id} ${executionKind} suite id is invalid`);
  if (!Array.isArray(suite.artifact_paths) || suite.artifact_paths.length === 0) fail(`${lane.id} ${executionKind} suite has no artifact paths`);
  const descriptorPath = suite.descriptor_path;
  if (typeof descriptorPath !== "string" || !existsSync(descriptorPath)) fail(`${lane.id} ${executionKind} descriptor is missing`);
  const ownedPath = (candidate, label) => {
    const resolvedPath = resolve(candidate);
    if (!isWithin(receiptRoot, resolvedPath)) fail(`${lane.id} ${executionKind} ${label} escapes the qualified worktree`);
    assertCanonicalParents(receiptRoot, resolvedPath, `${lane.id} ${executionKind} ${label}`, false);
    const metadata = lstatSync(resolvedPath);
    if (!metadata.isFile() || metadata.isSymbolicLink()) fail(`${lane.id} ${executionKind} ${label} is not a regular file`);
    return resolvedPath;
  };
  const ownedDescriptorPath = ownedPath(descriptorPath, "descriptor");
  const descriptorBytes = readFileSync(ownedDescriptorPath);
  if (suite.descriptor_sha256 !== hash(descriptorBytes)) fail(`${lane.id} ${executionKind} descriptor digest does not match its bytes`);
  let descriptor;
  try { descriptor = JSON.parse(descriptorBytes.toString("utf8")); }
  catch (error) { fail(`${lane.id} ${executionKind} descriptor is invalid JSON: ${error instanceof Error ? error.message : String(error)}`); }
  if (descriptor.protocol !== "acyclic.graphcoder.suite-descriptor.v1") fail(`${lane.id} ${executionKind} descriptor protocol is invalid`);
  if (descriptor.id !== suite.id || descriptor.descriptor !== suite.descriptor || descriptor.execution_kind !== executionKind || descriptor.platform !== platform) fail(`${lane.id} ${executionKind} descriptor identity is invalid`);
  if (descriptor.source_commit !== source.commit || descriptor.source_tree !== source.tree || descriptor.source_clean !== true) fail(`${lane.id} ${executionKind} receipt is bound to a different or dirty source`);
  if (!Array.isArray(descriptor.coverage) || descriptor.coverage.length === 0) fail(`${lane.id} ${executionKind} descriptor lacks named requirement coverage`);
  const coverageRequirements = new Set();
  for (const [index, item] of descriptor.coverage.entries()) {
    if (!item || typeof item.requirement_id !== "string" || !REQUIREMENT_ID.test(item.requirement_id) || typeof item.assertion !== "string" || !ASSERTION_NAME.test(item.assertion)) {
      fail(`${lane.id} ${executionKind} descriptor coverage ${index} is invalid`);
    }
    if (coverageRequirements.has(item.requirement_id)) fail(`${lane.id} ${executionKind} descriptor repeats requirement ${item.requirement_id}`);
    coverageRequirements.add(item.requirement_id);
  }
  if (source.canonical_worktree === undefined || descriptor.source_working_tree_sha256 !== workingTreeDigest(source.canonical_worktree)) fail(`${lane.id} ${executionKind} receipt working-tree digest is stale`);
  if (!descriptor.execution_assertion || descriptor.execution_assertion.marker !== "graphcoder-executed-count" || !Number.isInteger(descriptor.execution_assertion.minimum_executed) || descriptor.execution_assertion.minimum_executed <= 0) fail(`${lane.id} ${executionKind} descriptor lacks an executed-count assertion`);
  const command = descriptor.command;
  if (!command || typeof command !== "object" || !Array.isArray(command.args) || typeof command.executable !== "string") fail(`${lane.id} ${executionKind} descriptor command is invalid`);
  if (lane.driver && command.args[0] !== lane.driver) fail(`${lane.id} ${executionKind} descriptor command does not invoke its declared driver as argv[0]`);
  if (basename(command.executable).toLowerCase() !== "node" && basename(command.executable).toLowerCase() !== "node.exe") fail(`${lane.id} ${executionKind} descriptor executable is not Node`);
  if (source.canonical_worktree !== undefined && resolve(command.cwd) !== resolve(source.canonical_worktree)) fail(`${lane.id} ${executionKind} descriptor cwd is not the qualified worktree`);
  if (!Array.isArray(command.env) || command.env.some(key => typeof key !== "string" || !QUALIFICATION_ENVIRONMENT_KEYS.has(key) || /(?:TOKEN|PASSWORD|SECRET|CREDENTIAL|PRIVATE_KEY|ACCESS_KEY|API_KEY)/iu.test(key))) fail(`${lane.id} ${executionKind} descriptor environment is not filtered`);
  if (lane.allows_mock_fixture === false && (command.args.some(arg => /(?:^|=)--fixture(?:=|$)/u.test(arg)) || command.env.some(key => /(?:MOCK_FIXTURE|PTY_FIXTURE|ALLOW_FIXTURE)/u.test(key)))) {
    fail(`${lane.id} ${executionKind} descriptor invokes a mock or fixture launch recipe`);
  }
  if (!Array.isArray(descriptor.consumed_artifacts)) fail(`${lane.id} ${executionKind} descriptor lacks consumed artifacts`);
  const receiptArtifacts = new Map();
  for (const artifact of record.artifacts) {
    if (!artifact || typeof artifact !== "object") fail(`${lane.id} ${executionKind} artifact record is invalid`);
    if (typeof artifact.path !== "string" || !existsSync(artifact.path)) fail(`${lane.id} ${executionKind} artifact is missing`);
    const ownedArtifactPath = ownedPath(artifact.path, "artifact");
    const artifactKey = process.platform === "win32" ? ownedArtifactPath.toLowerCase() : ownedArtifactPath;
    if (receiptArtifacts.has(artifactKey)) fail(`${lane.id} ${executionKind} receipt repeats artifact ${artifact.path}`);
    receiptArtifacts.set(artifactKey, { ...artifact, path: ownedArtifactPath });
    if (typeof artifact.sha256 !== "string" || artifact.sha256 !== hash(readFileSync(ownedArtifactPath))) fail(`${lane.id} ${executionKind} artifact digest does not match its bytes`);
    if (artifact.source_commit !== source.commit || artifact.source_tree !== source.tree || artifact.fresh !== true) fail(`${lane.id} ${executionKind} artifact provenance is stale or not fresh`);
  }
  const descriptorArtifacts = new Map();
  for (const [index, artifact] of descriptor.consumed_artifacts.entries()) {
    if (!artifact || typeof artifact.path !== "string" || artifact.path.trim() === "") fail(`${lane.id} ${executionKind} descriptor artifact ${index} is invalid`);
    const descriptorArtifactPath = ownedPath(artifact.path, `descriptor artifact ${index}`);
    const artifactKey = process.platform === "win32" ? descriptorArtifactPath.toLowerCase() : descriptorArtifactPath;
    if (descriptorArtifacts.has(artifactKey)) fail(`${lane.id} ${executionKind} descriptor repeats artifact ${artifact.path}`);
    const receiptArtifact = receiptArtifacts.get(artifactKey);
    if (receiptArtifact === undefined) fail(`${lane.id} ${executionKind} descriptor references an unknown artifact`);
    for (const field of ["sha256", "source_commit", "source_tree", "build_id"]) {
      if (typeof artifact[field] !== "string" || artifact[field] !== receiptArtifact[field]) fail(`${lane.id} ${executionKind} descriptor artifact ${artifact.path} does not match the receipt artifact`);
    }
    descriptorArtifacts.set(artifactKey, artifact);
  }
  const suiteArtifactKeys = suite.artifact_paths.map(pathValue => {
    if (typeof pathValue !== "string" || pathValue.trim() === "") fail(`${lane.id} ${executionKind} suite artifact path is invalid`);
    const suiteArtifactPath = ownedPath(pathValue, "suite artifact");
    return process.platform === "win32" ? suiteArtifactPath.toLowerCase() : suiteArtifactPath;
  });
  if (new Set(suiteArtifactKeys).size !== suiteArtifactKeys.length || JSON.stringify([...descriptorArtifacts.keys()].sort()) !== JSON.stringify([...receiptArtifacts.keys()].sort()) || JSON.stringify([...new Set(suiteArtifactKeys)].sort()) !== JSON.stringify([...receiptArtifacts.keys()].sort())) {
    fail(`${lane.id} ${executionKind} descriptor, suite, and receipt artifact use do not match`);
  }
  if (typeof suite.transcript_path !== "string" || !existsSync(suite.transcript_path)) fail(`${lane.id} ${executionKind} transcript is missing`);
  const ownedTranscriptPath = ownedPath(suite.transcript_path, "transcript");
  const transcriptBytes = readFileSync(ownedTranscriptPath);
  if (typeof suite.transcript_sha256 !== "string" || suite.transcript_sha256 !== hash(transcriptBytes)) fail(`${lane.id} ${executionKind} transcript digest does not match its bytes`);
  const executionEvidence = suite.execution_evidence;
  if (!executionEvidence || executionEvidence.marker !== "graphcoder-executed-count" || !Number.isInteger(executionEvidence.executed_count) || !Number.isInteger(executionEvidence.minimum_executed) || !Number.isInteger(executionEvidence.raw_exit_code) || executionEvidence.signal !== null || !Array.isArray(executionEvidence.cases)) fail(`${lane.id} ${executionKind} receipt lacks a raw-exit and executed-count witness`);
  if (executionEvidence.minimum_executed !== descriptor.execution_assertion.minimum_executed || executionEvidence.raw_exit_code !== 0) fail(`${lane.id} ${executionKind} execution witness does not match the descriptor or successful raw exit`);
  const countMatches = [...transcriptBytes.toString("utf8").matchAll(EXECUTION_COUNT_LINE)];
  if (countMatches.length !== 1 || Number(countMatches[0][1]) !== executionEvidence.executed_count || executionEvidence.executed_count < executionEvidence.minimum_executed) fail(`${lane.id} ${executionKind} transcript does not prove the required executed count`);
  const evidenceRequirements = new Set();
  for (const [index, item] of executionEvidence.cases.entries()) {
    if (!item || typeof item.requirement_id !== "string" || !REQUIREMENT_ID.test(item.requirement_id) || typeof item.assertion !== "string" || !ASSERTION_NAME.test(item.assertion) || item.status !== "passed") {
      fail(`${lane.id} ${executionKind} execution case ${index} is invalid`);
    }
    if (evidenceRequirements.has(item.requirement_id)) fail(`${lane.id} ${executionKind} execution repeats requirement ${item.requirement_id}`);
    evidenceRequirements.add(item.requirement_id);
  }
  const descriptorCoverage = descriptor.coverage.map(item => `${item.requirement_id}\0${item.assertion}`);
  const evidenceCoverage = executionEvidence.cases.map(item => `${item?.requirement_id}\0${item?.assertion}`);
  const transcriptCoverage = [...transcriptBytes.toString("utf8").matchAll(CASE_WITNESS_LINE)].map(match => `${match[1]}\0${match[2]}`);
  if (JSON.stringify(descriptorCoverage) !== JSON.stringify(evidenceCoverage) || JSON.stringify(descriptorCoverage) !== JSON.stringify(transcriptCoverage)) fail(`${lane.id} ${executionKind} named case witnesses do not match descriptor coverage`);
  return { lane: lane.id, execution_kind: executionKind, receipt_path: receiptPath, suite_id: suite.id, descriptor_path: descriptorPath };
}

function requiredLaneReceipts(manifest, platform, source) {
  const receipts = [];
  for (const lane of manifest.qualification_lanes) {
    if (!lane.platforms.includes(platform)) continue;
    if (lane.receipt_environment_by_execution_kind) {
      for (const executionKind of lane.execution_kinds) {
        const environmentName = lane.receipt_environment_by_execution_kind[executionKind];
        if (environmentName === undefined) fail(`${lane.id} lacks a receipt environment for ${executionKind}`);
        const path = process.env[environmentName];
        if (typeof path !== "string" || path.trim() === "") fail(`${lane.id} requires ${environmentName}; platform run cannot complete without the installed lane receipt`);
        receipts.push(readLaneReceipt(path, lane, executionKind, source, platform));
      }
    } else if (lane.receipt_environment) {
      const path = process.env[lane.receipt_environment];
      if (typeof path !== "string" || path.trim() === "") fail(`${lane.id} requires ${lane.receipt_environment}; platform run cannot complete without the installed lane receipt`);
      receipts.push(readLaneReceipt(path, lane, lane.execution_kinds[0], source, platform));
    }
  }
  return receipts;
}

function loadManifest(path = MANIFEST_PATH) {
  return validateManifest(readJson(path));
}

function currentSource() {
  const commit = git("rev-parse", "HEAD");
  const tree = git("rev-parse", "HEAD^{tree}");
  if (!HEX40.test(commit) || !HEX40.test(tree)) fail("git returned an invalid source commit or tree");
  const worktree = realpathSync(git("rev-parse", "--show-toplevel"));
  return {
    commit,
    tree,
    branch: git("branch", "--show-current"),
    worktree,
    canonical_worktree: worktree,
    rust_tree_sha256: sourceTreeDigest(join(worktree, "rust")),
    qualification_tree_sha256: qualificationTreeDigest(worktree),
    clean: git("status", "--porcelain", "--untracked-files=all") === "",
  };
}

function sourceTreeDigest(root) {
  if (!existsSync(root)) fail(`Rust source tree is missing: ${root}`);
  const worktree = resolve(root, "..");
  const inventory = execFileSync("git", ["-C", worktree, "ls-files", "-z", "--", "rust"], { encoding: "buffer", env: { ...runtimeEnvironment(process.env), ...gitEnvironment() } }).toString("utf8");
  const paths = inventory.split("\0").filter(Boolean).sort();
  const hashes = workingPathDigests(worktree, paths);
  const entries = paths.map((path, index) => `${path}\0${hashes[index]}`);
  return hash(entries.join("\n"));
}

function qualificationTreeDigest(worktree) {
  try { return workingTreeDigest(worktree); }
  catch (error) { fail(error instanceof Error ? error.message : String(error)); }
}

function sameSourceIdentity(left, right) {
  return left.commit === right.commit
    && left.tree === right.tree
    && left.canonical_worktree === right.canonical_worktree
    && left.rust_tree_sha256 === right.rust_tree_sha256
    && left.qualification_tree_sha256 === right.qualification_tree_sha256;
}

function commandText(gate) {
  return [gate.command.executable, ...gate.command.args].map(arg => JSON.stringify(arg)).join(" ");
}

function safeEnvironment(environment = process.env) {
  const result = {};
  for (const [key, value] of Object.entries(environment)) {
    if (SECRET_ENVIRONMENT.test(key) || !SAFE_ENVIRONMENT_KEYS.has(key.toUpperCase()) || typeof value !== "string") continue;
    result[key] = value;
  }
  // Windows environment names are case-insensitive. Preserve the spelling
  // supplied by the host instead of replacing `Path` with an empty `PATH`.
  const pathEntry = Object.entries(environment).find(([key]) => key.toLowerCase() === "path");
  if (pathEntry !== undefined) {
    const [key, value] = pathEntry;
    result[key] = value;
    // Unix shells require the canonical spelling; Windows accepts either.
    if (key !== "PATH" && process.platform !== "win32") result.PATH = value;
  } else {
    result.PATH = "";
  }
  return result;
}

/**
 * Runtime gates receive only the host values needed to locate and execute the
 * declared program. Build and packaging gates may receive the separately
 * allowlisted toolchain variables above; neither policy inherits credentials.
 */
function runtimeEnvironment(environment = process.env) {
  const result = {};
  for (const [key, value] of Object.entries(environment)) {
    if (!RUNTIME_ENVIRONMENT.test(key) || SECRET_ENVIRONMENT.test(key) || typeof value !== "string") continue;
    result[key] = value;
  }
  const pathEntry = Object.entries(environment).find(([key]) => key.toLowerCase() === "path");
  if (pathEntry !== undefined) {
    const [key, value] = pathEntry;
    result[key] = value;
    if (key !== "PATH" && process.platform !== "win32") result.PATH = value;
  } else {
    result.PATH = "";
  }
  return result;
}

function list(manifest) {
  for (const gate of manifest.gates) {
    process.stdout.write(`${gate.id}\t${gate.platforms.join(",")}\t${gate.execution_kind}\t${commandText(gate)}\n`);
  }
}

export function check(manifest, platform = platformName()) {
  platform = requestedPlatform(platform);
  const source = currentSource();
  const selected = manifest.gates.filter(gate => gate.platforms.includes(platform));
  if (selected.length === 0) fail(`no gates are defined for ${platform}`);
  const result = { protocol: "acyclic.graphcoder.platform-gates-check.v1", platform, source, manifest_sha256: hash(readFileSync(resolve(ROOT, MANIFEST_PATH))), gates: [] };
  for (const gate of selected) {
    // `where` is used only for discovery. Execution remains shell-free below.
    const lookup = spawnSync(process.platform === "win32" ? "where.exe" : "which", [gate.command.executable], {
      encoding: "utf8", windowsHide: true, env: runtimeEnvironment(process.env),
    });
    const available = lookup.status === 0;
    result.gates.push({ id: gate.id, command: gate.command, available });
    if (!available) fail(`${gate.id} executable is unavailable: ${gate.command.executable}`);
  }
  return result;
}

export async function executeGate(gate, cwd, { environment = process.env, environmentPolicy = "runtime" } = {}) {
  const child = spawnOwnedProcess(gate.command.executable, gate.command.args, {
    cwd,
    env: environmentPolicy === "toolchain" ? safeEnvironment(environment) : runtimeEnvironment(environment),
    encoding: "utf8",
    shell: false,
    windowsHide: true,
    stdio: ["ignore", "pipe", "pipe"],
  });
  let untrackChild = trackOwnedProcess(child);
  let releaseOnClose = false;
  const releaseChild = () => {
    if (untrackChild === null) return;
    untrackChild();
    untrackChild = null;
  };
  const stdout = [];
  const stderr = [];
  child.stdout?.on("data", chunk => stdout.push(Buffer.isBuffer(chunk) ? chunk.toString("utf8") : String(chunk)));
  child.stderr?.on("data", chunk => stderr.push(Buffer.isBuffer(chunk) ? chunk.toString("utf8") : String(chunk)));
  let closeResolve;
  const closed = new Promise(resolvePromise => { closeResolve = resolvePromise; });
  let readersClosed = false;
  child.once("close", (code, signal) => {
    readersClosed = true;
    closeResolve({ code, signal });
    if (releaseOnClose) releaseChild();
  });
  let errorResolve;
  const errored = new Promise(resolvePromise => { errorResolve = resolvePromise; });
  child.once("error", error => errorResolve(error));
  let timer;
  let cleanup = null;
  try {
    const outcome = await Promise.race([
      closed,
      errored.then(error => ({ error })),
      new Promise(resolvePromise => { timer = setTimeout(() => resolvePromise({ timeout: true }), gate.timeout_ms); }),
    ]);
    if (outcome?.timeout) {
      const cleanupRequest = await terminateOwnedProcessTree(child);
      cleanup = cleanupRequest;
      let terminationTimer;
      const termination = await Promise.race([
        closed,
        new Promise(resolvePromise => { terminationTimer = setTimeout(() => resolvePromise({ timeout: true }), 5_000); }),
      ]);
      clearTimeout(terminationTimer);
      return {
        stdout: stdout.join(""),
        stderr: stderr.join(""),
        status: null,
        signal: termination?.signal ?? null,
        readers_closed: readersClosed,
        cleanup: cleanupRequest,
        error: cleanupRequest?.state === "uncertain" || termination?.timeout ? `process termination is uncertain after timeout` : `gate timed out after ${gate.timeout_ms}ms`,
      };
    }
    if (outcome?.error) {
      cleanup = await terminateOwnedProcessTree(child);
      await Promise.race([closed, new Promise(resolvePromise => setTimeout(resolvePromise, 5_000))]);
      return { stdout: stdout.join(""), stderr: stderr.join(""), status: null, signal: null, readers_closed: readersClosed, cleanup, error: outcome.error.message };
    }
    cleanup = { state: "exited", verified: readersClosed };
    return { stdout: stdout.join(""), stderr: stderr.join(""), status: outcome.code, signal: outcome.signal, readers_closed: readersClosed, cleanup, error: null };
  } finally {
    clearTimeout(timer);
    // Keep ownership until the close event when termination did not prove
    // that the child and its inherited readers are gone. This closes the
    // parent-exit race where an unresolved descendant would otherwise be
    // removed from the cleanup set as soon as executeGate returns.
    if (readersClosed) releaseChild();
    else releaseOnClose = true;
  }
}

function helperArtifacts(manifestBytes, gate) {
  const runnerPath = resolve(fileURLToPath(import.meta.url));
  const manifestPath = resolve(ROOT, MANIFEST_PATH);
  const artifacts = [
    { path: manifestPath, sha256: hash(manifestBytes) },
    { path: runnerPath, sha256: hash(readFileSync(runnerPath)) },
  ];
  if (gate.command.args.includes("scripts/graphcoder-package-gate.mjs")) {
    for (const packageHelperPath of [
      resolve(ROOT, "scripts", "graphcoder-package-gate.mjs"),
      resolve(ROOT, "scripts", "graphcoder-package-contract.test.mjs"),
      resolve(ROOT, "scripts", "fixtures", "graphcoder-qualification", "package-contract.mjs"),
      resolve(ROOT, "release", "npm-packages.json"),
      resolve(ROOT, "typescript", "packages", "graphcoder", "package.json"),
    ]) {
      artifacts.push({ path: packageHelperPath, sha256: hash(readFileSync(packageHelperPath)) });
    }
  }
  return artifacts;
}

function resolveRootPath(path) {
  return resolve(ROOT, path);
}

function artifactPath(worktree, path, gateId) {
  const candidate = resolve(worktree, path);
  if (!isWithin(worktree, candidate)) fail(`${gateId} produced artifact escapes the qualified worktree: ${path}`);
  assertCanonicalParents(worktree, candidate, gateId);
  const relativePath = relative(resolve(worktree), candidate).replaceAll("\\", "/");
  try {
    execFileSync("git", ["-C", worktree, "ls-files", "--error-unmatch", "--", relativePath], { stdio: "ignore", env: { ...runtimeEnvironment(process.env), ...gitEnvironment() } });
    fail(`${gateId} produced artifact must be an untracked generated path: ${path}`);
  } catch (error) {
    if (error instanceof Error && error.message.startsWith("graphcoder-platform-gates:")) throw error;
    if (error?.status !== 1) throw error;
    // `ls-files --error-unmatch` exits 1 for the intended untracked path.
  }
  return candidate;
}

function prepareProducedArtifacts(gate, worktree, platform) {
  const before = new Map();
  for (const path of producedArtifactPaths(gate, platform)) {
    const candidate = artifactPath(worktree, path, gate.id);
    if (existsSync(candidate)) {
      const metadata = lstatSync(candidate);
      if (metadata.isSymbolicLink() || !metadata.isFile()) fail(`${gate.id} produced artifact path is not a regular file: ${path}`);
      const bytes = readFileSync(candidate);
      before.set(path, { exists: true, sha256: hash(bytes), size_bytes: bytes.length });
      rmSync(candidate, { force: true });
    } else {
      before.set(path, { exists: false, sha256: null, size_bytes: null });
    }
  }
  return before;
}

function claimQualificationOutput(output, source) {
  const root = resolveRootPath(output);
  if (!isWithin(source.canonical_worktree, root)) fail(`qualification output must remain inside the source worktree: ${root}`);
  ensureOwnedDirectory(root, source.canonical_worktree, "qualification output");
  const markerPath = join(root, ".graphcoder-platform-source.json");
  const marker = {
    protocol: "acyclic.graphcoder.platform-output-owner.v1",
    canonical_worktree: source.canonical_worktree,
    source_commit: source.commit,
    source_tree: source.tree,
    rust_tree_sha256: source.rust_tree_sha256,
    qualification_tree_sha256: source.qualification_tree_sha256,
  };
  if (existsSync(markerPath)) {
    let existing;
    try {
      existing = JSON.parse(readFileSync(markerPath, "utf8"));
    } catch (error) {
      fail(`qualification output ownership marker is corrupt: ${error instanceof Error ? error.message : String(error)}`);
    }
    if (existing.canonical_worktree !== marker.canonical_worktree) {
      fail(`qualification output is already owned by another source root: ${existing.canonical_worktree}`);
    }
    if (existing.source_commit !== marker.source_commit || existing.source_tree !== marker.source_tree || existing.rust_tree_sha256 !== marker.rust_tree_sha256 || existing.qualification_tree_sha256 !== marker.qualification_tree_sha256) {
      fail("qualification output source identity changed; use a fresh output directory");
    }
  } else {
    writeFileSync(markerPath, `${JSON.stringify(marker, null, 2)}\n`, { flag: "wx" });
  }
  return root;
}

function cargoTargetDirectory(output, source) {
  const identity = hash(`${source.canonical_worktree}\0${source.commit}\0${source.tree}\0${source.rust_tree_sha256}\0${source.qualification_tree_sha256}`).slice(0, 32);
  const target = resolve(output, "cargo-targets", identity);
  ensureOwnedDirectory(target, source.canonical_worktree, "Cargo target");
  const markerPath = join(target, ".graphcoder-cargo-target-owner.json");
  const marker = {
    protocol: "acyclic.graphcoder.cargo-target-owner.v1",
    canonical_worktree: source.canonical_worktree,
    source_commit: source.commit,
    source_tree: source.tree,
    rust_tree_sha256: source.rust_tree_sha256,
    qualification_tree_sha256: source.qualification_tree_sha256,
  };
  if (existsSync(markerPath)) {
    const existing = JSON.parse(readFileSync(markerPath, "utf8"));
    if (existing.canonical_worktree !== marker.canonical_worktree || existing.source_commit !== marker.source_commit || existing.source_tree !== marker.source_tree || existing.rust_tree_sha256 !== marker.rust_tree_sha256 || existing.qualification_tree_sha256 !== marker.qualification_tree_sha256) {
      fail(`Cargo target directory is shared by a different source identity: ${target}`);
    }
  } else {
    writeFileSync(markerPath, `${JSON.stringify(marker, null, 2)}\n`, { flag: "wx" });
  }
  return target;
}

function producedArtifactDigests(gate, worktree, platform, source, commandDigest, attemptNonce, before, producerResult) {
  return producedArtifactPaths(gate, platform).map(path => {
    const artifactPath = artifactPathForRecord(worktree, path, gate.id);
    if (!existsSync(artifactPath)) fail(`${gate.id} declared produced artifact was not created by the producer: ${path}`);
    const metadata = lstatSync(artifactPath);
    if (!metadata.isFile() || metadata.isSymbolicLink()) fail(`${gate.id} produced artifact is not a regular file: ${path}`);
    const canonicalPath = realpathSync(artifactPath);
    if (!isWithin(worktree, canonicalPath)) fail(`${gate.id} produced artifact resolves outside the qualified worktree: ${path}`);
    const bytes = readFileSync(canonicalPath);
    const format = validateArtifactBytes(gate.id, path, bytes);
    const previous = before.get(path) ?? { exists: false, sha256: null, size_bytes: null };
    return {
      path: canonicalPath,
      sha256: hash(bytes),
      size_bytes: bytes.length,
      format,
      provenance: {
        producer_gate: gate.id,
        command_sha256: commandDigest,
        attempt_nonce: attemptNonce,
        invocation_sha256: commandDigest,
        source_working_tree_sha256: source.qualification_tree_sha256,
        artifact_sha256: hash(bytes),
        producer_observation: {
          exit_code: producerResult.status,
          signal: producerResult.signal,
          error: producerResult.error,
          observed_after_dispatch: true,
        },
        canonical_worktree: source.canonical_worktree,
        source_commit: source.commit,
        source_tree: source.tree,
        rust_tree_sha256: source.rust_tree_sha256,
        qualification_tree_sha256: source.qualification_tree_sha256,
        pre_execution: previous,
      },
    };
  });
}

function artifactPathForRecord(worktree, path, gateId) {
  const candidate = resolve(worktree, path);
  if (!isWithin(worktree, candidate)) fail(`${gateId} produced artifact escapes the qualified worktree: ${path}`);
  assertCanonicalParents(worktree, candidate, gateId);
  return candidate;
}

function validateArtifactBytes(gateId, path, bytes) {
  if (bytes.length === 0) fail(`${gateId} produced artifact is empty: ${path}`);
  let nonZero = false;
  for (const byte of bytes) {
    if (byte !== 0) { nonZero = true; break; }
  }
  if (!nonZero) fail(`${gateId} produced artifact is all zero bytes: ${path}`);
  if (/\.(?:exe|dll)$/iu.test(path)) {
    if (bytes.length < 64 || bytes[0] !== 0x4d || bytes[1] !== 0x5a) fail(`${gateId} produced PE artifact has no valid DOS header: ${path}`);
    const peOffset = bytes.readUInt32LE(0x3c);
    if (peOffset < 64 || peOffset + 4 > bytes.length || bytes.subarray(peOffset, peOffset + 4).toString("ascii") !== "PE\0\0") {
      fail(`${gateId} produced PE artifact has no valid NT header: ${path}`);
    }
    return "pe";
  }
  if (/\.wasm$/iu.test(path)) {
    if (bytes.length < 8 || bytes.subarray(0, 4).toString("hex") !== "0061736d" || bytes.readUInt32LE(4) !== 1) {
      fail(`${gateId} produced WASM artifact has an invalid header: ${path}`);
    }
    return "wasm";
  }
  return "file";
}

export async function run(manifest, { platform = platformName(), output = ".qualification/platform-gates" } = {}) {
  platform = requestedPlatform(platform);
  const sourceBefore = currentSource();
  const laneReceipts = requiredLaneReceipts(manifest, platform, sourceBefore);
  const selected = manifest.gates.filter(gate => gate.platforms.includes(platform));
  if (selected.length === 0) fail(`no gates are defined for ${platform}`);
  const root = claimQualificationOutput(output, sourceBefore);
  const cargoTarget = cargoTargetDirectory(root, sourceBefore);
  const manifestBytes = readFileSync(resolve(ROOT, MANIFEST_PATH));
  const records = [];
  for (const gate of selected) {
    const helperArtifactDigests = helperArtifacts(manifestBytes, gate);
    const startedAt = new Date().toISOString();
    const attemptNonce = randomUUID();
    const producedBefore = prepareProducedArtifacts(gate, sourceBefore.worktree, platform);
    const environment = /^cargo(?:\.exe)?$/iu.test(gate.command.executable)
      ? { ...process.env, CARGO_TARGET_DIR: cargoTarget }
      : process.env;
    const environmentPolicy = gate.execution_kind === "compile"
      || gate.execution_kind === "package"
      || /^cargo(?:\.exe)?$/iu.test(gate.command.executable)
      ? "toolchain"
      : "runtime";
    const invocationEnvironment = environmentPolicy === "toolchain" ? safeEnvironment(environment) : runtimeEnvironment(environment);
    // Bind provenance to the complete dispatched argv/cwd/environment policy,
    // rather than only the manifest command. Credentials are excluded from
    // the serialized record; the digest still covers every allowed value that
    // the child actually receives.
    const commandDigest = hash(Buffer.from(JSON.stringify({
      executable: gate.command.executable,
      args: gate.command.args,
      cwd: sourceBefore.worktree,
      environment_policy: environmentPolicy,
      environment: Object.fromEntries(Object.entries(invocationEnvironment).sort(([left], [right]) => left.localeCompare(right))),
    })));
    const result = await executeGate(gate, sourceBefore.worktree, { environment, environmentPolicy });
    // Fence the producer before reading any generated artifact. The source
    // identity attached to a record is only causal when this immediate
    // post-process snapshot still matches the pre-dispatch snapshot.
    const sourceAfterGate = currentSource();
    const producerSourceFence = sameSourceIdentity(sourceBefore, sourceAfterGate);
    const producerPassed = result.status === 0 && result.signal === null && result.error === null;
    const completedAt = new Date().toISOString();
    const transcript = `${result.stdout}${result.stderr ? `\n[stderr]\n${result.stderr}` : ""}`;
    const transcriptPath = resolve(root, `${gate.id}.transcript.log`);
    writeFileSync(transcriptPath, transcript, { flag: "wx" });
    const suite = {
      id: `platform-${gate.id}`,
      area: gate.area,
      execution_kind: gate.execution_kind,
      platform,
      execution_evidence: {
        mode: gate.execution_kind === "compile" ? "compile-only" : "host-executed",
        requested_platform: platform,
        actual_host_platform: platformName(),
        host_platform_verified: platform === platformName(),
        environment_policy: environmentPolicy,
        credentials_inherited: false,
        process_exit_observed: result.status !== null && result.error === null,
      },
      command: gate.command,
      timeout_ms: gate.timeout_ms,
    };
    const suiteBytes = Buffer.from(JSON.stringify(suite));
    const record = {
      protocol: "acyclic.graphcoder.platform-gate-record.v1",
      id: gate.id,
      area: gate.area,
      execution_kind: gate.execution_kind,
      platform,
      command: gate.command,
      command_sha256: commandDigest,
      manifest_sha256: hash(manifestBytes),
      suite_id: suite.id,
      suite_descriptor_sha256: hash(suiteBytes),
      execution_evidence: suite.execution_evidence,
      // Runner files and transcripts prove how the gate was executed. They
      // are helper artifacts, not distributable SDK artifacts.
      helper_artifacts: [...helperArtifactDigests, { path: transcriptPath, sha256: hash(Buffer.from(transcript)) }],
      qualification_artifacts: producerSourceFence && producerPassed
        ? producedArtifactDigests(gate, sourceBefore.worktree, platform, sourceBefore, commandDigest, attemptNonce, producedBefore, result)
        : [],
      source: sourceBefore,
      source_before: sourceBefore,
      source_after: sourceAfterGate,
      source_fence_after_producer: producerSourceFence,
      started_at: startedAt,
      completed_at: completedAt,
      transcript_path: transcriptPath,
      transcript_sha256: hash(Buffer.from(transcript)),
      result: { status: producerPassed && producerSourceFence && result.cleanup?.state !== "uncertain" ? "passed" : "failed", exit_code: result.status, signal: result.signal, cleanup: result.cleanup, error: producerSourceFence ? result.error : "source changed during gate execution" },
    };
    const sourcePostSuite = currentSource();
    const postSuiteFence = sameSourceIdentity(sourceAfterGate, sourcePostSuite);
    record.source_post_suite = sourcePostSuite;
    record.post_suite_fence = postSuiteFence;
    record.source_fence = producerSourceFence && postSuiteFence;
    if (!record.source_fence) {
      record.result = { ...record.result, status: "failed", error: producerSourceFence ? "source changed during qualification evidence collection" : "source changed during gate execution" };
      record.qualification_artifacts = [];
    }
    const recordPath = resolve(root, `${gate.id}.json`);
    writeFileSync(recordPath, `${JSON.stringify(record, null, 2)}\n`, { flag: "wx" });
    records.push({ ...record, record_path: recordPath });
    if (record.result.status !== "passed") break;
  }
  const sourceAfter = currentSource();
  const summary = {
    protocol: "acyclic.graphcoder.platform-gates-run.v1",
    platform,
    manifest_sha256: hash(manifestBytes),
    source_before: sourceBefore,
    source_after: sourceAfter,
    records,
    qualification_lane_receipts: laneReceipts,
    status: records.length === selected.length && records.every(record => record.result.status === "passed") && sameSourceIdentity(sourceBefore, sourceAfter) && sourceBefore.clean && sourceAfter.clean ? "passed" : "failed",
  };
  const summaryPath = resolve(root, "summary.json");
  writeFileSync(summaryPath, `${JSON.stringify(summary, null, 2)}\n`, { flag: "wx" });
  return { summary_path: summaryPath, ...summary };
}

export { loadManifest, validateManifest, commandText, safeEnvironment, runtimeEnvironment, readLaneReceipt };

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const [mode, value] = process.argv.slice(2);
    const manifest = loadManifest();
    if (mode === "list" && value === undefined) list(manifest);
    else if (mode === "check" && (value === undefined || PLATFORMS.has(value))) process.stdout.write(`${JSON.stringify(check(manifest, value), null, 2)}\n`);
    else if (mode === "run" && (value === undefined || PLATFORMS.has(value))) {
      const result = await run(manifest, { platform: value });
      process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
      if (result.status !== "passed") process.exitCode = 1;
    }
    else fail("usage: graphcoder-platform-gates.mjs list | check [windows|linux|macos] | run [windows|linux|macos]");
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}
