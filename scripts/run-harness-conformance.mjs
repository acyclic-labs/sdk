import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { basename, resolve } from "node:path";

if (process.argv.length !== 5) {
  throw new Error("usage: run-harness-conformance.mjs ARTIFACT_DIR REPORT.json RECEIPT.json");
}
const [artifactArgument, reportArgument, receiptArgument] = process.argv.slice(2);
const artifactDirectory = resolve(artifactArgument);
const reportPath = resolve(reportArgument);
const receiptPath = resolve(receiptArgument);
const evidenceBytes = readFileSync(resolve(artifactDirectory, "CONFORMANCE-EVIDENCE.json"));
const evidence = JSON.parse(evidenceBytes.toString("utf8"));
if (evidence.protocol !== "acyclic.package-evidence.v1") throw new Error("unsupported package evidence protocol");
const evidenceList = (name, value) => {
  if (!Array.isArray(value) || value.some(item => typeof item !== "string" || item.length === 0)) {
    throw new Error(`invalid ${name} package evidence`);
  }
  const normalized = [...new Set(value)].sort();
  if (JSON.stringify(value) !== JSON.stringify(normalized)) {
    throw new Error(`${name} package evidence must be sorted and unique`);
  }
  return normalized;
};
const digestPattern = /^[0-9a-f]{64}$/;
if (
  evidence.test_transcript_sha256 === null
  || typeof evidence.test_transcript_sha256 !== "object"
  || !digestPattern.test(evidence.test_transcript_sha256.rust)
  || !digestPattern.test(evidence.test_transcript_sha256.typescript)
) {
  throw new Error("invalid normalized package test transcript digests");
}
const executed = {
  rust: new Set(evidenceList("Rust", evidence.rust_tests)),
  typescript: new Set(evidenceList("TypeScript", evidence.typescript_tests)),
};
const normalizedTranscriptDigest = tests => createHash("sha256")
  .update(Buffer.from(`${tests.join("\n")}\n`))
  .digest("hex");
if (
  evidence.test_transcript_sha256.rust !== normalizedTranscriptDigest(evidence.rust_tests)
  || evidence.test_transcript_sha256.typescript !== normalizedTranscriptDigest(evidence.typescript_tests)
) {
  throw new Error("package test transcript digests do not match the executed cases");
}
const suiteBytes = readFileSync("conformance/vectors/core.json");
const suite = JSON.parse(suiteBytes.toString("utf8"));

const artifacts = readdirSync(artifactDirectory)
  .filter(name => name.endsWith(".crate") || name.endsWith(".tgz"))
  .sort();
if (
  artifacts.length !== 3
  || !artifacts.includes("acyclic-harness.tgz")
  || artifacts.filter(name => /^acyclic-harness-[^-].*\.crate$/.test(name)).length !== 1
  || artifacts.filter(name => /^acyclic-stream-[^-].*\.crate$/.test(name)).length !== 1
) {
  throw new Error("expected exact Stream, Harness, and npm archives");
}
const artifactEvidence = artifacts.map(name => ({
  name,
  sha256: createHash("sha256").update(readFileSync(resolve(artifactDirectory, name))).digest("hex"),
}));
if (JSON.stringify(evidence.artifacts) !== JSON.stringify(artifactEvidence)) {
  throw new Error("package evidence does not match the exact release archives");
}

const markers = new Map([
  ["operation-identities-are-stable", [["rust", "executor::tests::operation_identity_rejects_changed_input"]]],
  ["native-wasm-replay-is-byte-equivalent", [["rust", "wire_codec::tests::native_event_bytes_match_cross_language_fixture"], ["typescript", "WASM event bytes match the native cross-language fixture"]]],
  ["authority-scopes-cannot-cross-aggregate-audiences", [["rust", "core::tests::mutated_or_foreign_scopes_are_rejected"]]],
  ["stream-append-uncertainty-is-queryable", [["rust", "store::tests::reconciliation_observes_a_commit_without_redispatch"]]],
  ["trimmed-history-restores-from-checked-snapshot", [["rust", "store::tests::snapshot_reopens_a_trimmed_stream_suffix"]]],
  ["fork-publication-is-atomic", [["rust", "core::tests::fork_is_invisible_until_one_manifest_event_commits"]]],
  ["effect-attempts-respect-provider-guarantees", [["rust", "core::tests::at_most_once_effect_is_never_redispatched_after_uncertainty"]]],
  ["typed-approvals-bind-the-exact-action", [["rust", "core::tests::typed_approval_is_bound_and_resolved_once"]]],
  ["structured-parent-waits-release-capacity", [["rust", "scheduler::tests::waiting_parent_releases_execution_capacity"]]],
  ["join-preserves-child-slot-order", [["rust", "scheduler::tests::reduction_is_bound_to_the_exact_contract_and_inputs"]]],
  ["race-uses-first-authoritative-success", [["rust", "scheduler::tests::race_uses_first_observed_success_and_cancels_losers"]]],
  ["quorum-fails-when-threshold-is-unreachable", [["rust", "scheduler::tests::failed_dependencies_are_explicitly_rejectable"]]],
  ["stock-executor-replay-does-not-repeat-tools", [["rust", "executor::tests::stock_loop_replays_without_reinvoking_models_or_tools"]]],
  ["custom-executor-owns-the-whole-turn-loop", [["rust", "executor::tests::interrupted_model_stream_reconciles_without_redispatch"]]],
  ["client-replay-is-generation-fenced", [["typescript", "a replay generation cannot change without an explicit rebase"], ["typescript", "client hydrates a durable cursor before its first reconnect"], ["typescript", "rebase fences a delivery buffered by the previous replay connection"]]],
  ["client-outbox-clears-only-after-authority", [["typescript", "reconnect delivery is contiguous and clears authoritative outbox entries"], ["typescript", "IndexedDB atomically persists outbox acknowledgements and replay cursors across restart"], ["typescript", "IndexedDB preserves enqueue order across restart and isolates database namespaces"], ["typescript", "IndexedDB migration keeps version-one records ahead of newly enqueued commands"]]],
  ["pagination-is-bounded-and-rebase-safe", [["typescript", "page reset fences an older in-flight response"]]],
  ["operation-control-is-protocol-scope-and-owner-bound", [["rust", "wire_api::tests::operation_control_is_protocol_scope_and_response_identity_bound"], ["typescript", "gRPC control validates echoed operation, owner, protocol, error, and retry identity"]]],
  ["recursive-cancellation-is-atomic-and-exactly-replayable", [["rust", "distributed::tests::authenticated_recursive_cancel_is_atomic_durable_and_exactly_replayable"]]],
  ["recursive-cancellation-stops-at-owner-boundaries", [["rust", "scheduler::tests::recursive_cancellation_stops_at_owner_boundaries"]]],
  ["transport-control-errors-remain-request-correlated", [["typescript", "framed control serializes observe and cancel for the same operation"], ["typescript", "correlated framed errors do not abort another operation"]]],
  ["durable-context-providers-reopen-compaction-exactly", [["rust", "context::tests::durable_sources_and_compaction_reopen_exactly"]]],
  ["coding-bundle-host-adapter-is-complete-and-executable", [["rust", "bundle::tests::coding_factory_builds_an_executable_complete_registry"]]],
]);

const harnessCases = suite.cases.filter(item => item.family === "harness");
if (harnessCases.length !== markers.size) throw new Error("executable marker map does not exactly cover the Harness suite");
const command = (executable, args, input) => {
  const result = spawnSync(executable, args, { encoding: "utf8", input });
  if (result.status !== 0) throw new Error(`${executable} failed: ${result.stderr || result.stdout}`);
  return result.stdout.trim();
};
command("cargo", ["build", "--quiet", "--locked", "-p", "acyclic-conformance", "--bin", "harness-conformance"]);
const metadata = JSON.parse(command("cargo", ["metadata", "--locked", "--no-deps", "--format-version", "1"]));
const runnerBinary = resolve(metadata.target_directory, `debug/harness-conformance${process.platform === "win32" ? ".exe" : ""}`);
const hash = bytes => command(runnerBinary, ["digest"], bytes);
const cases = harnessCases.map(item => {
  const required = markers.get(item.name);
  if (required === undefined || required.some(([runtime, name]) => !executed[runtime]?.has(name))) {
    throw new Error(`executed package evidence is missing for ${item.name}`);
  }
  return {
    name: item.name,
    status: "passed",
    evidence_digest: hash(new TextEncoder().encode(JSON.stringify({
      case: item.name,
      evidence_digest: hash(evidenceBytes),
      required,
    }))),
  };
});

const artifactDigest = command(
  runnerBinary,
  ["bundle-digest", ...artifacts.map(name => resolve(artifactDirectory, name))],
);
const sourceRevision = command("git", ["rev-parse", "HEAD"]);
const status = command("git", ["status", "--porcelain"]);
if (status.length > 0 && process.env.ACYCLIC_ALLOW_DIRTY_CONFORMANCE !== "1") {
  throw new Error("conformance subject must be an exact committed source tree");
}
const protocolIdentity = JSON.parse(command(runnerBinary, ["identity"]));
const compatibility = JSON.parse(readFileSync("compatibility/manifest.json", "utf8"));
const report = {
  protocol: "acyclic.conformance.runner.v1",
  family: "harness",
  suite_version: suite.version,
  suite_digest: hash(suiteBytes),
  subject: {
    name: "acyclic-agent-runtime",
    version: compatibility.families.harness.version,
    source_revision: sourceRevision,
    artifact_digest: artifactDigest,
  },
  runner: {
    language: "rust+typescript",
    name: "acyclic-conformance/package-runner",
    version: compatibility.families.harness.version,
  },
  protocol_identity: protocolIdentity,
  capability_profile: ["embedded", "grpc", "host", "http", "jsonl", "typescript", "wasm", "websocket"],
  cases,
};
writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`);
const receipt = command(runnerBinary, ["validate", reportPath]);
writeFileSync(receiptPath, `${receipt}\n`);
const parsedReceipt = JSON.parse(receipt);
if (!parsedReceipt.qualified || parsedReceipt.passed !== harnessCases.length) {
  throw new Error("executed report did not qualify");
}
const checksumPath = resolve(artifactDirectory, "SHA256SUMS");
const checksumLines = [reportPath, receiptPath].map(path =>
  `${createHash("sha256").update(readFileSync(path)).digest("hex")}  ${basename(path)}`,
);
writeFileSync(checksumPath, `${readFileSync(checksumPath, "utf8").trimEnd()}\n${checksumLines.join("\n")}\n`);
console.log(`qualified ${parsedReceipt.passed}/${parsedReceipt.total} executed Harness cases for ${basename(artifactDirectory)}`);
