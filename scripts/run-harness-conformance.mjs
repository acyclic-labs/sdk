import { harnessCaseMarkers, registerCases, requireExecutedCase } from "./harness-conformance-cases.mjs";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { basename, resolve } from "node:path";
import { compatibilityArtifacts } from "./generated-bindings.mjs";
import { harnessPackageClosure } from "./harness-package-closure.mjs";
import { verifyProviderTests, recursiveProviderTest } from "./harness-provider-evidence.mjs";

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
const suiteBytes = readFileSync(compatibilityArtifacts.harness.conformanceDigest);
const suite = JSON.parse(suiteBytes.toString("utf8"));

const artifacts = readdirSync(artifactDirectory)
  .filter(name => name.endsWith(".crate") || name.endsWith(".tgz"))
  .sort();
// The same closure the packaging step stages, so the two cannot drift.
const expectedArtifacts = [
  "acyclic-harness.tgz",
  ...harnessPackageClosure().map(({ name, version }) => `${name}-${version}.crate`),
].sort();
if (JSON.stringify(artifacts) !== JSON.stringify(expectedArtifacts)) {
  throw new Error(`expected exactly the Harness npm archive and crate closure: ${expectedArtifacts.join(", ")}`);
}
const artifactEvidence = artifacts.map(name => ({
  name,
  sha256: createHash("sha256").update(readFileSync(resolve(artifactDirectory, name))).digest("hex"),
}));
if (JSON.stringify(evidence.artifacts) !== JSON.stringify(artifactEvidence)) {
  throw new Error("package evidence does not match the exact release archives");
}

const { cases: harnessCases, markers } = registerCases(suite, "harness", harnessCaseMarkers);
const command = (executable, args, input) => {
  const result = spawnSync(executable, args, { encoding: "utf8", input });
  if (result.status !== 0) throw new Error(`${executable} failed: ${result.stderr || result.stdout}`);
  return result.stdout.trim();
};
// The exact archived crate already ran the full provider-backed 1024-fork
// integration target. Verify that target's transcript, not a matching unit name.
// Exercise the smaller sibling scenario under the reduced feature set below.
const e2e = [
  ["durable-local-conversation-fork-and-merge-reopens", "local_conversation_fork",
    "local_reopen_preserves_ref_only_history_fork_and_parent_merge", ["--features", "filesystem-local"]],
];
const e2eTranscripts = new Map();
executed.e2e = new Set();
const providerDigest = verifyProviderTests(evidence);
const recursiveName = recursiveProviderTest.slice(recursiveProviderTest.indexOf("::") + 2);
executed.e2e.add(recursiveName);
const narrowName = "thirty_two_sibling_forks_reject_stale_and_conflicting_merges";
const narrowTranscript = command("cargo", [
  "test", "--locked", "-p", "acyclic-harness", "--features", "filesystem",
  "--test", "recursive_fork", "--", "--exact", narrowName,
]);
if (!narrowTranscript.split(/\r?\n/).includes(`test ${narrowName} ... ok`)) {
  throw new Error("reduced-feature sibling provider scenario was not executed");
}
e2eTranscripts.set("recursive-fork-isolation-attachments-and-project-only-merge",
  createHash("sha256").update(`${providerDigest}\n${narrowTranscript}`).digest("hex"));
for (const [marker, target, name, features] of e2e) {
  const transcript = command("cargo", [
    "test", "--locked", "-p", "acyclic-harness", "--features", "filesystem", ...features,
    "--test", target, "--", "--exact", name,
  ]);
  if (!transcript.split(/\r?\n/).some(line => line === `test ${name} ... ok`)) {
    throw new Error(`${name} E2E was not actually executed`);
  }
  executed.e2e.add(name);
  e2eTranscripts.set(marker, createHash("sha256").update(transcript).digest("hex"));
}
command("cargo", ["build", "--quiet", "--locked", "-p", "acyclic-conformance", "--bin", "harness-conformance"]);
const metadata = JSON.parse(command("cargo", ["metadata", "--locked", "--no-deps", "--format-version", "1"]));
const runnerBinary = resolve(metadata.target_directory, `debug/harness-conformance${process.platform === "win32" ? ".exe" : ""}`);
const hash = bytes => command(runnerBinary, ["digest"], bytes);
const cases = harnessCases.map(item => {
  const required = markers.get(item.name);
  requireExecutedCase(item.name, required, executed);
  return {
    name: item.name,
    status: "passed",
    evidence_digest: hash(new TextEncoder().encode(JSON.stringify({
      case: item.name,
      evidence_digest: hash(evidenceBytes),
      required,
      ...(e2eTranscripts.has(item.name)
        ? { e2e_transcript_sha256: e2eTranscripts.get(item.name) }
        : {}),
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
