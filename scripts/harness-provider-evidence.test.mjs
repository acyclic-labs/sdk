import { test } from "node:test";
import assert from "node:assert/strict";
import { collectProviderTests, verifyProviderTests, recursiveProviderTest } from "./harness-provider-evidence.mjs";

const name = recursiveProviderTest.slice(recursiveProviderTest.indexOf("::") + 2);
const transcript = `     Running tests/recursive_fork.rs (target/debug/deps/recursive_fork)\n\ntest ${name} ... ok\n`;
function evidence(source = transcript) {
  const parsed = collectProviderTests(source);
  return { rust_tests: [name], rust_provider_tests: parsed.tests, test_transcript_sha256: { provider: parsed.sha256 } };
}
test("provider evidence accepts the actual integration target on Unix and Windows", () => {
  for (const source of [transcript, transcript.replace("tests/", "tests\\").replace("Running", "\x1b[1mRunning\x1b[0m")]) {
    assert.equal(verifyProviderTests(evidence(source)), evidence().test_transcript_sha256.provider);
  }
});
test("a same-named unit test is not provider integration evidence", () => {
  assert.throws(() => verifyProviderTests(evidence(transcript.replace("tests/recursive_fork.rs", "unittests src/lib.rs"))), /not executed/);
});
test("a different integration target cannot stand in for recursive stress", () => {
  assert.throws(() => verifyProviderTests(evidence(transcript.replace("recursive_fork.rs", "other.rs"))), /not executed/);
});
test("later unit and doc-test names cannot inherit a previous target", () => {
  const source = `Running tests/recursive_fork.rs (binary)\nRunning unittests src/lib.rs (binary)\ntest ${name} ... ok\n`;
  assert.deepEqual(collectProviderTests(source).tests, []);
});
test("an altered provider transcript digest fails closed", () => {
  const value = evidence();
  value.test_transcript_sha256.provider = "0".repeat(64);
  assert.throws(() => verifyProviderTests(value), /invalid/);
});
test("the provider pass must also occur in the complete Rust package transcript", () => {
  const value = evidence();
  value.rust_tests = [];
  assert.throws(() => verifyProviderTests(value), /absent/);
});

// Registration is checked before Map construction so neither side can hide duplicates.
import { readFileSync } from "node:fs";
import { harnessCaseMarkers, registerCases, requireExecutedCase } from "./harness-conformance-cases.mjs";
const suite = { cases: [{ name: "a", family: "harness" }, { name: "b", family: "harness" }] };
const registrations = [["harness", "a", [["rust", "test_a"]]], ["harness", "b", [["e2e", "test_b"]]]];

test("locked Harness inventory fails closed until exact semantic assertions exist", () => {
  const locked = JSON.parse(readFileSync(new URL("../conformance/vectors/core.json", import.meta.url), "utf8"));
  const cases = locked.cases.filter(item => item.family === "harness");
  assert.deepEqual(harnessCaseMarkers.map(([, name]) => name), cases.map(item => item.name));
  const pending = harnessCaseMarkers.filter(([, , required]) => !required.length);
  assert.deepEqual(pending.map(([, name]) => name), [
    "join-preserves-child-slot-order", "quorum-fails-when-threshold-is-unreachable", "custom-executor-owns-the-whole-turn-loop",
  ]);
  assert.throws(() => registerCases(locked, "harness", harnessCaseMarkers), /no executable evidence/);
  for (const marker of pending) {
    assert.throws(() => registerCases({ cases: [{ name: marker[1], family: "harness" }] }, "harness", [marker]), /no executable evidence/);
  }
});

test("equal counts cannot conceal duplicate, missing or foreign ownership", () => {
  assert.throws(() => registerCases({ cases: [suite.cases[0], suite.cases[0]] }, "harness", registrations), /duplicate suite/);
  assert.throws(() => registerCases(suite, "harness", [registrations[0], registrations[0]]), /duplicate executable/);
  assert.throws(() => registerCases(suite, "harness", [registrations[0]]), /missing executable/);
  assert.throws(() => registerCases(suite, "harness", [registrations[0], ["harness", "c", [["rust", "test_b"]]]]), /unowned executable/);
  assert.throws(() => registerCases(suite, "harness", [registrations[0], ["filesystem", "b", [["rust", "test_b"]]]]), /unowned executable/);
});

test("documentation and empty or unsupported evidence cannot register executable cases", () => {
  for (const required of [[], [["documentation", "test_b"]], [["rust", ""]], [["rust", "test_b"], ["rust", "test_b"]]]) {
    assert.throws(() => registerCases(suite, "harness", [registrations[0], ["harness", "b", required]]), /evidence/);
  }
  assert.throws(() => registerCases({ cases: [] }, "harness", []), /no harness cases/);
  assert.throws(() => registerCases({ cases: [{ name: "a" }] }, "harness", registrations), /name and family/);
});

test("skipped, failed and partial execution cannot satisfy a case", () => {
  const required = [["rust", "test_a"], ["e2e", "test_b"]];
  for (const executed of [{}, { rust: new Set(["test_a"]) }, { rust: new Set(["test_a"]), e2e: new Set(["test_b ... FAILED"]) }]) {
    assert.throws(() => requireExecutedCase("a", required, executed), /missing/);
  }
  requireExecutedCase("a", required, { rust: new Set(["test_a"]), e2e: new Set(["test_b"]) });
});

test("one executable scenario may assert multiple independently named contracts", () => {
  const shared = [["rust", "multi_assertion_test"]];
  const registered = registerCases(suite, "harness", [["harness", "a", shared], ["harness", "b", shared]]);
  for (const [name, required] of registered.markers) {
    requireExecutedCase(name, required, { rust: new Set(["multi_assertion_test"]) });
  }
});

import { createHash } from "node:crypto";
import { verifyPackageProvenance, verifyQualificationReceipt } from "./harness-conformance-evidence.mjs";
const sourceRevision = "a".repeat(40);
const packageDigests = new Map(["acyclic-harness.tgz", "CONFORMANCE-EVIDENCE.json"].map(name => [name, createHash("sha256").update(name).digest("hex")]));
const packageChecksums = [...packageDigests].map(([name, digest]) => `${digest}  ${name}`).join("\n") + "\n";

test("package provenance binds source and every consumed archive/evidence digest", () => {
  verifyPackageProvenance(sourceRevision + "\n", packageChecksums, sourceRevision, packageDigests);
  assert.throws(() => verifyPackageProvenance("b".repeat(40), packageChecksums, sourceRevision, packageDigests), /source/);
  for (const name of packageDigests.keys()) {
    const substituted = new Map(packageDigests);
    substituted.set(name, "0".repeat(64));
    assert.throws(() => verifyPackageProvenance(sourceRevision, packageChecksums, sourceRevision, substituted), /checksum/);
  }
  assert.throws(() => verifyPackageProvenance(sourceRevision, packageChecksums.split("\n").slice(1).join("\n"), sourceRevision, packageDigests), /checksum/);
  assert.throws(() => verifyPackageProvenance(sourceRevision, packageChecksums + `${"0".repeat(64)}  extra.tgz\n`, sourceRevision, packageDigests), /exactly/);
  assert.throws(() => verifyPackageProvenance(sourceRevision, packageChecksums + packageChecksums, sourceRevision, packageDigests), /duplicate/);
  assert.throws(() => verifyPackageProvenance(sourceRevision, packageChecksums.replace("acyclic-harness.tgz", "../acyclic-harness.tgz"), sourceRevision, packageDigests), /invalid/);
});

const boundReport = {
  family: "harness", suite_version: 2, suite_digest: "blake3:" + "1".repeat(64),
  subject: { source_revision: sourceRevision, artifact_digest: "blake3:" + "2".repeat(64) },
  runner: { name: "package-runner" }, protocol_identity: { version: "2" }, capability_profile: ["embedded"],
  cases: [{ name: "a", status: "passed", evidence_digest: "blake3:" + "3".repeat(64) }],
};
const reportDigest = "blake3:" + "4".repeat(64);
const boundReceipt = { ...boundReport, protocol: "acyclic.conformance.receipt.v1", qualified: true, passed: 1, total: 1, report_digest: reportDigest };

test("a qualified aggregate from another report, source or artifact is rejected", () => {
  verifyQualificationReceipt(boundReport, boundReceipt, reportDigest);
  for (const mutation of [
    { report_digest: "blake3:" + "5".repeat(64) }, { protocol: "unsupported" }, { qualified: false }, { total: 2 },
    { subject: { ...boundReport.subject, source_revision: "b".repeat(40) } },
    { subject: { ...boundReport.subject, artifact_digest: "blake3:" + "5".repeat(64) } },
    { cases: [{ ...boundReport.cases[0], status: "skipped" }] },
    { cases: [{ ...boundReport.cases[0], evidence_digest: "blake3:" + "5".repeat(64) }] },
    { capability_profile: ["unsupported-backend"] }, { suite_digest: "blake3:" + "5".repeat(64) },
  ]) assert.throws(() => verifyQualificationReceipt(boundReport, { ...boundReceipt, ...mutation }, reportDigest), /qualif|bind/);
  assert.throws(() => verifyQualificationReceipt({ ...boundReport, cases: [{ ...boundReport.cases[0], status: "failed" }] }, boundReceipt, reportDigest), /qualify/);
});

// Exercise the actual CLI preflight with real Git and files, before any build.
import { spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync, existsSync, copyFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

test("runner rejects stale source and substituted package inputs before qualification", () => {
  const root = mkdtempSync(join(tmpdir(), "sdk-conformance-provenance-"));
  try {
    const git = (args, cwd = root) => {
      const result = spawnSync("git", args, { cwd, encoding: "utf8" });
      assert.equal(result.status, 0, result.stderr);
      return result.stdout.trim();
    };
    mkdirSync(join(root, "scripts"));
    for (const name of ["run-harness-conformance.mjs", "harness-conformance-cases.mjs", "harness-conformance-evidence.mjs", "generated-bindings.mjs", "harness-package-closure.mjs", "harness-provider-evidence.mjs"]) {
      copyFileSync(new URL(`./${name}`, import.meta.url), join(root, "scripts", name));
    }
    mkdirSync(join(root, "conformance/vectors"), { recursive: true });
    copyFileSync(new URL("../conformance/vectors/core.json", import.meta.url), join(root, "conformance/vectors/core.json"));
    writeFileSync(join(root, ".gitignore"), "/artifacts/\n/launcher/\n");
    git(["init", "--quiet"]);
    git(["add", "."]);
    git(["-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--quiet", "-m", "fixture"]);
    const revision = git(["rev-parse", "HEAD"]);
    const launcher = join(root, "launcher");
    mkdirSync(launcher);
    git(["init", "--quiet"], launcher);
    git(["-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--allow-empty", "--quiet", "-m", "unrelated launcher"], launcher);
    const directory = join(root, "artifacts");
    mkdirSync(directory);
    const archive = Buffer.from("synthetic archive; preflight must reject before Cargo");
    const archiveName = "acyclic-harness.tgz";
    const digest = bytes => createHash("sha256").update(bytes).digest("hex");
    const tests = { rust: ["test_a"], typescript: ["test_b"] };
    const evidenceBytes = JSON.stringify({
      protocol: "acyclic.package-evidence.v1", rust_tests: tests.rust, typescript_tests: tests.typescript,
      artifacts: [{ name: archiveName, sha256: digest(archive) }],
      test_transcript_sha256: { rust: digest("test_a\n"), typescript: digest("test_b\n") },
    });
    writeFileSync(join(directory, archiveName), archive);
    writeFileSync(join(directory, "CONFORMANCE-EVIDENCE.json"), evidenceBytes);
    writeFileSync(join(directory, "SHA256SUMS"), `${digest(archive)}  ${archiveName}\n${digest(evidenceBytes)}  CONFORMANCE-EVIDENCE.json\n`);
    const report = join(directory, "runner-report.json");
    const receipt = join(directory, "qualification-receipt.json");
    const reject = pattern => {
      const result = spawnSync(process.execPath, [join(root, "scripts/run-harness-conformance.mjs"), directory, report, receipt], { cwd: launcher, encoding: "utf8" });
      assert.notEqual(result.status, 0);
      assert.match(result.stderr, pattern);
      assert.equal(existsSync(report), false);
      assert.equal(existsSync(receipt), false);
    };
    writeFileSync(join(directory, "SOURCE_COMMIT"), "b".repeat(40));
    reject(/package source does not match/);
    writeFileSync(join(directory, "SOURCE_COMMIT"), revision);
    writeFileSync(join(directory, archiveName), "substituted archive");
    reject(/package checksum does not match acyclic-harness.tgz/);
    writeFileSync(join(directory, archiveName), archive);
    writeFileSync(join(directory, "CONFORMANCE-EVIDENCE.json"), evidenceBytes + "\n");
    reject(/package checksum does not match CONFORMANCE-EVIDENCE.json/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
