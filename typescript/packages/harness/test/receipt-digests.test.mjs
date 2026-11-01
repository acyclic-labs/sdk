import { test } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { compatibilityArtifacts } from "../../../../scripts/generated-bindings.mjs";
import { receiptDigester, verifyQualificationReceipt } from "../../../../scripts/harness-conformance-evidence.mjs";
import { buildContractTargets } from "../../../../scripts/contract-targets.mjs";

test("receipt digests preserve exact bytes against the original native validator", async () => {
  const hash = await receiptDigester();
  const root = fileURLToPath(new URL("../../../../", import.meta.url));
  const [binary] = buildContractTargets(root, [["acyclic-conformance", "bin", "harness-conformance"]]);
  const native = (args, input) => {
    const result = spawnSync(binary, args, { input, encoding: "utf8" });
    assert.equal(result.status, 0, result.error?.message ?? result.stderr);
    return result.stdout.trim();
  };
  const suiteBytes = readFileSync(new URL(compatibilityArtifacts.harness.conformanceDigest, new URL("../../../../", import.meta.url)));
  // These are input bytes; expected digests come from the actual native binary.
  for (const bytes of [Buffer.alloc(0), Buffer.from("\0é\n雪"), Buffer.alloc(2 * 1024 * 1024, 0xa5), suiteBytes]) {
    assert.equal(hash(bytes), native(["digest"], bytes));
  }
  const suite = JSON.parse(suiteBytes.toString("utf8"));
  const report = {
    protocol: "acyclic.conformance.runner.v1", family: "harness",
    suite_version: suite.version, suite_digest: hash(suiteBytes),
    subject: { name: "acyclic-harness", version: "0.2.0",
      source_revision: "0123456789abcdef0123456789abcdef01234567", artifact_digest: hash("artifact") },
    runner: { language: "typescript", name: "acyclic-conformance/package-runner", version: "0.2.0" },
    protocol_identity: JSON.parse(native(["identity"])), capability_profile: ["host"],
    cases: suite.cases.filter(item => item.family === "harness").map(item => ({
      name: item.name, status: "passed", evidence_digest: hash(JSON.stringify({ case: item.name })),
    })),
  };
  const compact = JSON.stringify(report), pretty = `${JSON.stringify(report, null, 2)}\n`;
  const receipt = JSON.parse(native([], pretty));
  verifyQualificationReceipt(report, receipt, hash(pretty));
  assert.notEqual(hash(compact), hash(pretty));
  assert.throws(() => verifyQualificationReceipt(report, receipt, hash(compact)), /did not qualify/);
  const changed = structuredClone(report);
  changed.cases[0].evidence_digest = hash("different original evidence");
  assert.throws(() => verifyQualificationReceipt(changed, receipt, hash(pretty)), /does not bind report cases/);
});
