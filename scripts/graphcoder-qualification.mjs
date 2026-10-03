import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

const DEFAULT_MATRIX = "docs/graphcoder-swarm/requirements.json";
const RECEIPT_PROTOCOL = "acyclic.graphcoder.qualification-receipt.v1";
const MATRIX_PROTOCOL = "acyclic.graphcoder.requirements.v1";
const STATUS_VALUES = new Set(["passed", "pending", "failed", "skipped", "flaky", "not-run"]);
const EXECUTION_KINDS = new Set(["native", "compile", "mock", "pty", "package", "wasm"]);
const HEX64 = /^[0-9a-f]{64}$/;

const readJson = path => JSON.parse(readFileSync(resolve(path), "utf8"));
const sha256 = bytes => createHash("sha256").update(bytes).digest("hex");
const fileDigest = path => sha256(readFileSync(resolve(path)));
const currentCommit = () => execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim();
const currentBranch = () => execFileSync("git", ["branch", "--show-current"], { encoding: "utf8" }).trim();
const gitStatus = () => execFileSync("git", ["status", "--porcelain"], { encoding: "utf8" }).trim();
const gitRoot = () => execFileSync("git", ["rev-parse", "--show-toplevel"], { encoding: "utf8" }).trim();
const gitTree = commit => execFileSync("git", ["rev-parse", `${commit}^{tree}`], { encoding: "utf8" }).trim();
const gitIsAncestor = (base, commit) => {
  try {
    execFileSync("git", ["merge-base", "--is-ancestor", base, commit], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
};
const gitMergeCount = (base, commit) => Number(execFileSync("git", ["rev-list", "--count", "--merges", `${base}..${commit}`], { encoding: "utf8" }).trim());
const samePath = (left, right) => resolve(left).toLowerCase() === resolve(right).toLowerCase();

function failure(message) {
  throw new Error(`qualification: ${message}`);
}

export function loadMatrix(path = DEFAULT_MATRIX) {
  const matrix = readJson(path);
  validateMatrix(matrix);
  return matrix;
}

export function validateMatrix(matrix) {
  if (!matrix || matrix.protocol !== MATRIX_PROTOCOL) failure("requirements matrix protocol is invalid");
  if (!Array.isArray(matrix.entries) || matrix.entries.length === 0) failure("requirements matrix is empty");
  const ids = new Set();
  for (const [index, entry] of matrix.entries.entries()) {
    if (!entry || typeof entry !== "object") failure(`matrix entry ${index} is not an object`);
    for (const field of ["id", "area", "contract", "verification", "evidence"]) {
      if (typeof entry[field] !== "string" || entry[field].trim() === "") failure(`matrix entry ${index} lacks ${field}`);
    }
    if (!/^[A-Z][A-Z0-9-]+$/.test(entry.id)) failure(`invalid requirement id ${entry.id}`);
    if (ids.has(entry.id)) failure(`duplicate requirement id ${entry.id}`);
    ids.add(entry.id);
    if (!Array.isArray(entry.modes) || entry.modes.length === 0 || entry.modes.some(mode => !EXECUTION_KINDS.has(mode))) {
      failure(`${entry.id} has invalid execution modes`);
    }
  }
  if (!matrix.scope || typeof matrix.scope !== "object") failure("matrix scope is missing");
  if (!Array.isArray(matrix.status_values) || matrix.status_values.some(status => !STATUS_VALUES.has(status))) {
    failure("matrix status_values contains an unknown status");
  }
  return matrix;
}

function validateSuite(suite, index, final, artifactPaths) {
  if (!suite || typeof suite !== "object") failure(`suite ${index} is not an object`);
  for (const field of ["id", "descriptor", "descriptor_path", "descriptor_sha256", "platform", "execution_kind", "status", "started_at", "completed_at", "transcript_path", "transcript_sha256"]) {
    if (typeof suite[field] !== "string" || suite[field].trim() === "") failure(`suite ${index} lacks ${field}`);
  }
  if (!EXECUTION_KINDS.has(suite.execution_kind)) failure(`suite ${suite.id} has invalid execution_kind`);
  if (!new Set(["passed", "failed", "skipped", "flaky"]).has(suite.status)) failure(`suite ${suite.id} has invalid status`);
  if (Number.isNaN(Date.parse(suite.started_at)) || Number.isNaN(Date.parse(suite.completed_at))) failure(`suite ${suite.id} has invalid execution timestamps`);
  if (Date.parse(suite.completed_at) < Date.parse(suite.started_at)) failure(`suite ${suite.id} completed before it started`);
  if (!Array.isArray(suite.artifact_paths) || suite.artifact_paths.some(path => typeof path !== "string" || path.trim() === "")) failure(`suite ${suite.id} has invalid artifact paths`);
  if (suite.artifact_paths.some(path => !artifactPaths.has(path))) failure(`suite ${suite.id} references an unknown artifact`);
  if (final && suite.execution_kind === "package" && suite.artifact_paths.length === 0) failure(`final package suite ${suite.id} must reference its installed artifact`);
  if (!HEX64.test(suite.descriptor_sha256)) failure(`suite ${suite.id} has invalid descriptor digest`);
  if (!HEX64.test(suite.transcript_sha256)) failure(`suite ${suite.id} has invalid transcript digest`);
  for (const [kind, path, expected] of [["descriptor", suite.descriptor_path, suite.descriptor_sha256], ["transcript", suite.transcript_path, suite.transcript_sha256]]) {
    if (!existsSync(resolve(path))) failure(`suite ${suite.id} ${kind} file is missing: ${path}`);
    const actual = fileDigest(path);
    if (actual !== expected) failure(`suite ${suite.id} ${kind} digest mismatch: ${path}`);
  }
}

function validateArtifact(artifact, index, final, qualifiedCommit, qualifiedTree) {
  if (!artifact || typeof artifact !== "object") failure(`artifact ${index} is not an object`);
  for (const field of ["path", "sha256", "source_commit", "source_tree", "built_at", "build_id"]) {
    if (typeof artifact[field] !== "string" || artifact[field].trim() === "") failure(`artifact ${index} lacks ${field}`);
  }
  if (!HEX64.test(artifact.sha256)) failure(`artifact ${artifact.path} has invalid digest`);
  if (!/^[0-9a-f]{40}$/.test(artifact.source_tree)) failure(`artifact ${artifact.path} has invalid source tree`);
  if (artifact.source_commit !== qualifiedCommit) failure(`artifact ${artifact.path} was not built from the qualified source commit`);
  if (artifact.source_tree !== qualifiedTree) failure(`artifact ${artifact.path} was not built from the qualified source tree`);
  if (Number.isNaN(Date.parse(artifact.built_at))) failure(`artifact ${artifact.path} has invalid build time`);
  if (typeof artifact.fresh !== "boolean") failure(`artifact ${artifact.path} must declare fresh`);
  const path = resolve(artifact.path);
  if (!existsSync(path)) {
    if (final) failure(`artifact is missing: ${artifact.path}`);
    return;
  }
  const actual = fileDigest(path);
  if (actual !== artifact.sha256) failure(`artifact digest mismatch: ${artifact.path}`);
}

function validateCase(caseRecord, entry, suites, final) {
  if (!caseRecord || typeof caseRecord !== "object") failure(`${entry.id} case is not an object`);
  if (caseRecord.id !== entry.id) failure(`case id ${caseRecord.id} does not match matrix entry ${entry.id}`);
  if (!STATUS_VALUES.has(caseRecord.status)) failure(`${entry.id} has invalid status`);
  if (!Array.isArray(caseRecord.evidence)) failure(`${entry.id} evidence must be an array`);
  if (caseRecord.status === "passed" && caseRecord.evidence.length === 0) failure(`${entry.id} passed without evidence`);
  const suiteById = new Map(suites.map(suite => [suite.id, suite]));
  const modes = new Set();
  for (const [index, evidence] of caseRecord.evidence.entries()) {
    if (!evidence || typeof evidence !== "object") failure(`${entry.id} evidence ${index} is not an object`);
    if (typeof evidence.suite !== "string" || !suiteById.has(evidence.suite)) failure(`${entry.id} references unknown suite ${evidence.suite}`);
    if (!HEX64.test(evidence.descriptor_sha256)) failure(`${entry.id} evidence ${index} has invalid descriptor digest`);
    if (!EXECUTION_KINDS.has(evidence.execution_kind)) failure(`${entry.id} evidence ${index} has invalid execution kind`);
    const suite = suiteById.get(evidence.suite);
    if (evidence.execution_kind !== suite.execution_kind) failure(`${entry.id} evidence ${index} execution kind does not match suite ${suite.id}`);
    if (evidence.descriptor_sha256 !== suite.descriptor_sha256) failure(`${entry.id} evidence ${index} descriptor is not the referenced suite descriptor`);
    if (evidence.artifact_paths !== undefined && (!Array.isArray(evidence.artifact_paths) || evidence.artifact_paths.some(path => typeof path !== "string"))) failure(`${entry.id} evidence ${index} has invalid artifact paths`);
    const evidenceArtifacts = [...new Set(evidence.artifact_paths ?? [])].sort();
    const suiteArtifacts = [...new Set(suite.artifact_paths)].sort();
    if (JSON.stringify(evidenceArtifacts) !== JSON.stringify(suiteArtifacts)) failure(`${entry.id} evidence ${index} artifact use does not match suite ${suite.id}`);
    modes.add(evidence.execution_kind);
  }
  if (final) {
    if (caseRecord.status !== "passed") failure(`${entry.id} is ${caseRecord.status}, required cases must be passed`);
    for (const mode of entry.modes) {
      if (!modes.has(mode)) failure(`${entry.id} lacks required ${mode} evidence`);
    }
  }
}

export function validateReceipt(matrix, receipt, { final = false, matrixPath = DEFAULT_MATRIX } = {}) {
  validateMatrix(matrix);
  if (!receipt || typeof receipt !== "object" || receipt.protocol !== RECEIPT_PROTOCOL) failure("receipt protocol is invalid");
  const expectedMatrixPath = matrixPath.replaceAll("\\", "/");
  if (!receipt.matrix || receipt.matrix.path !== expectedMatrixPath) failure("receipt does not identify the locked matrix path");
  const matrixBytes = readFileSync(resolve(matrixPath));
  if (receipt.matrix.sha256 !== sha256(matrixBytes)) failure("receipt matrix digest does not match the locked matrix");
  if (!receipt.source || typeof receipt.source !== "object") failure("receipt source is missing");
  for (const field of ["commit", "worktree", "branch", "base_commit"]) {
    if (typeof receipt.source[field] !== "string" || receipt.source[field].trim() === "") failure(`receipt source lacks ${field}`);
  }
  if (typeof receipt.source.clean !== "boolean" || typeof receipt.source.merged !== "boolean") failure("receipt source must declare clean and merged");
  const qualifiedCommit = currentCommit();
  if (receipt.source.commit !== qualifiedCommit) failure("receipt source commit does not match the current checkout");
  if (receipt.source.branch !== currentBranch()) failure("receipt source branch does not match the current checkout");
  if (!samePath(receipt.source.worktree, gitRoot())) failure("receipt source worktree does not match the current checkout");
  const actualClean = gitStatus() === "";
  if (receipt.source.clean !== actualClean) failure("receipt source clean claim does not match the current checkout");
  if (receipt.source.merged) failure("receipt source claims a merge, which is forbidden");
  if (receipt.source.base_commit !== matrix.scope.base_commit) failure("receipt source base commit does not match the locked scope");
  if (!gitIsAncestor(receipt.source.base_commit, qualifiedCommit)) failure("locked base commit is not an ancestor of the qualified source");
  if (gitMergeCount(receipt.source.base_commit, qualifiedCommit) !== 0) failure("qualified source contains a merge commit");
  if (!Array.isArray(receipt.suites)) failure("receipt suites must be an array");
  if (!Array.isArray(receipt.cases)) failure("receipt cases must be an array");
  if (!Array.isArray(receipt.artifacts)) failure("receipt artifacts must be an array");
  if (!receipt.gate || typeof receipt.gate !== "object") failure("receipt gate is missing");
  const effectiveFinal = final || receipt.gate.final === true;
  const suiteIds = new Set();
  const artifactPaths = new Set(receipt.artifacts.map(artifact => artifact?.path));
  for (const [index, suite] of receipt.suites.entries()) {
    validateSuite(suite, index, effectiveFinal, artifactPaths);
    if (suiteIds.has(suite.id)) failure(`duplicate suite id ${suite.id}`);
    suiteIds.add(suite.id);
  }
  const qualifiedTree = gitTree(qualifiedCommit);
  for (const [index, artifact] of receipt.artifacts.entries()) validateArtifact(artifact, index, effectiveFinal, qualifiedCommit, qualifiedTree);
  const artifactByPath = new Map(receipt.artifacts.map(artifact => [artifact.path, artifact]));
  for (const suite of receipt.suites) {
    for (const path of suite.artifact_paths) {
      const artifact = artifactByPath.get(path);
      if (artifact && Date.parse(artifact.built_at) > Date.parse(suite.started_at)) {
        failure(`suite ${suite.id} started before artifact ${path} was built`);
      }
    }
  }
  const casesById = new Map();
  for (const record of receipt.cases) {
    if (casesById.has(record?.id)) failure(`duplicate case id ${record?.id}`);
    casesById.set(record?.id, record);
  }
  for (const entry of matrix.entries) {
    const record = casesById.get(entry.id);
    if (!record) failure(`receipt is missing matrix case ${entry.id}`);
    validateCase(record, entry, receipt.suites, effectiveFinal);
  }
  if (casesById.size !== matrix.entries.length) failure("receipt contains a case not present in the locked matrix");
  const counts = { failed: 0, skipped: 0, flaky: 0, missing: 0 };
  for (const record of receipt.cases) {
    if (record.status === "failed") counts.failed++;
    if (record.status === "skipped") counts.skipped++;
    if (record.status === "flaky") counts.flaky++;
    if (["pending", "not-run"].includes(record.status)) counts.missing++;
  }
  for (const key of Object.keys(counts)) {
    if (receipt.gate[key] !== counts[key]) failure(`gate.${key} is ${receipt.gate[key]}, expected ${counts[key]}`);
  }
  if (effectiveFinal) {
    if (!receipt.gate.final) failure("receipt was requested as final but gate.final is false");
    if (!receipt.source.clean || receipt.source.merged) failure("final receipt requires a clean, unmerged source worktree");
    if (receipt.source.branch !== matrix.scope.branch) failure(`final receipt branch must be ${matrix.scope.branch}`);
    if (counts.failed || counts.skipped || counts.flaky || counts.missing) failure("final receipt contains failed, skipped, flaky, or missing cases");
    if (!receipt.artifacts.length || receipt.artifacts.some(artifact => !artifact.fresh)) failure("final receipt requires fresh distributable artifacts");
    if (receipt.suites.some(suite => suite.status !== "passed")) failure("final receipt contains a non-passing suite");
  }
  return { counts, requirements: matrix.entries.length, suites: receipt.suites.length, artifacts: receipt.artifacts.length };
}

export function makePendingReceipt(matrixPath = DEFAULT_MATRIX) {
  const matrix = loadMatrix(matrixPath);
  return {
    protocol: RECEIPT_PROTOCOL,
    matrix: { path: matrixPath.replaceAll("\\", "/"), sha256: fileDigest(matrixPath) },
    source: { commit: currentCommit(), worktree: process.cwd(), branch: currentBranch(), base_commit: matrix.scope.base_commit, clean: gitStatus() === "", merged: false },
    suites: [],
    cases: matrix.entries.map(entry => ({ id: entry.id, status: "pending", evidence: [] })),
    artifacts: [],
    gate: { final: false, failed: 0, skipped: 0, flaky: 0, missing: matrix.entries.length },
  };
}

function renderMarkdown(matrix) {
  const lines = [
    "# Locked acceptance matrix: Harness-first coding swarm",
    "",
    `Machine-readable source: [requirements.json](./requirements.json) (${matrix.entries.length} locked entries).`,
    "Every entry is required. The receipt validator rejects missing, failed, skipped, flaky, or not-run entries for final qualification.",
    "",
    "| ID | Area | Contract | Verification | Required evidence |",
    "|---|---|---|---|---|",
    ...matrix.entries.map(entry => `| ${entry.id} | ${entry.area} | ${entry.contract.replaceAll("|", "\\|")} | ${entry.verification.replaceAll("|", "\\|")} | ${entry.modes.join(", ")} |`),
    "",
    "The model is mocked. Storage, filesystem, recursive agents, the git facade, approved subprocesses, and terminal interaction must be exercised as real effects. Compilation is recorded separately from native execution and cannot satisfy a native or PTY requirement.",
    "",
  ];
  return lines.join("\n");
}

function usage() {
  console.error("usage: graphcoder-qualification.mjs matrix-check [matrix] | render [matrix] [output] | receipt-template [matrix] [output] | receipt-check [matrix] receipt [--final]");
  process.exitCode = 2;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const command = process.argv[2];
  try {
    if (command === "matrix-check") {
      const matrix = loadMatrix(process.argv[3] ?? DEFAULT_MATRIX);
      console.log(JSON.stringify({ protocol: matrix.protocol, requirements: matrix.entries.length, ids: matrix.entries.map(entry => entry.id) }, null, 2));
    } else if (command === "render") {
      const matrix = loadMatrix(process.argv[3] ?? DEFAULT_MATRIX);
      const output = process.argv[4];
      if (!output) usage();
      else writeFileSync(resolve(output), renderMarkdown(matrix));
    } else if (command === "receipt-template") {
      const matrixPath = process.argv[3] ?? DEFAULT_MATRIX;
      const output = process.argv[4];
      if (!output) usage();
      else writeFileSync(resolve(output), `${JSON.stringify(makePendingReceipt(matrixPath), null, 2)}\n`);
    } else if (command === "receipt-check") {
      const matrixPath = process.argv[3] ?? DEFAULT_MATRIX;
      const receiptPath = process.argv[4];
      if (!receiptPath) usage();
      else {
        const result = validateReceipt(loadMatrix(matrixPath), readJson(receiptPath), { final: process.argv.includes("--final"), matrixPath });
        console.log(JSON.stringify(result, null, 2));
      }
    } else usage();
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
