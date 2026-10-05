import assert from "node:assert/strict";
import { verifyQualificationSummary } from "./verify-guide-projection-receipts.mjs";

const command = { command: "package-manager", exitCode: 0 };
const receipt = (overrides = {}) => ({
  scenario_id: "scenario-0",
  language: "rust",
  source_revision: "source-sha256:sha256:" + "a".repeat(64),
  install_status: "installed",
  install: command,
  compile: command,
  execution: command,
  status: "executed",
  source_sha256: "sha256:" + "a".repeat(64),
  package_artifact: "package.whl",
  package_sha256: "b".repeat(64),
  qualification: { install: "install", compile: "compile", execute: "execute" },
  ...overrides,
});
const valid = {
  schema: "acyclic.sdk.guide-projection-qualification.v1",
  projection_count: 54,
  source_revision: "source-sha256:sha256:" + "a".repeat(64),
  source_sha256: "sha256:" + "a".repeat(64),
  receipts: Array.from({ length: 54 }, (_, index) => receipt({ scenario_id: `scenario-${index}` })),
};

assert.equal(verifyQualificationSummary(valid).valid, true);
assert.equal(verifyQualificationSummary({ ...valid, receipts: valid.receipts.map((value, index) => index === 3 ? receipt({ scenario_id: value.scenario_id, install: { command: "artifact present", exitCode: 0 } }) : value) }).valid, false);
assert.equal(verifyQualificationSummary({ ...valid, receipts: valid.receipts.map((value, index) => index === 3 ? receipt({ scenario_id: value.scenario_id, install: { command: "artifact present", exitCode: 0 }, compile: null }) : value) }).valid, false);
assert.equal(verifyQualificationSummary({ ...valid, receipts: valid.receipts.map((value, index) => index === 3 ? receipt({ scenario_id: value.scenario_id, status: "compiled", execution: null }) : value) }).valid, false);

assert.equal(verifyQualificationSummary({ ...valid, receipts: Array.from({ length: 54 }, () => receipt()) }).valid, false);
for (const bad of [{ scenario_id: "" }, { language: "" }, { package_sha256: "sha256:package" }, { package_artifact: "" }, { source_revision: "another-source" }]) {
  const receipts = valid.receipts.map((value, index) => index === 3 ? { ...value, ...bad } : value);
  assert.equal(verifyQualificationSummary({ ...valid, receipts }).valid, false);
}
assert.equal(verifyQualificationSummary({ ...valid, source_sha256: "sha256:source" }).valid, false);
assert.equal(verifyQualificationSummary({ ...valid, source_revision: "source-sha256:another-source" }).valid, false);
console.log("guide receipt identity, provenance and execution regressions passed");