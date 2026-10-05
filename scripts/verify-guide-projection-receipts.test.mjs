import assert from "node:assert/strict";
import { verifyQualificationSummary } from "./verify-guide-projection-receipts.mjs";

const command = { command: "package-manager", exitCode: 0 };
const receipt = (overrides = {}) => ({
  install_status: "installed",
  install: command,
  compile: command,
  execution: command,
  status: "executed",
  source_sha256: "sha256:source",
  package_artifact: "package.whl",
  package_sha256: "sha256:package",
  qualification: { install: "install", compile: "compile", execute: "execute" },
  ...overrides,
});
const valid = {
  schema: "acyclic.sdk.guide-projection-qualification.v1",
  projection_count: 54,
  source_revision: "source-sha256:sha256:source",
  source_sha256: "sha256:source",
  receipts: Array.from({ length: 54 }, () => receipt()),
};

assert.equal(verifyQualificationSummary(valid).valid, true);
assert.equal(verifyQualificationSummary({ ...valid, receipts: valid.receipts.map((value, index) => index === 3 ? receipt({ install: { command: "artifact present", exitCode: 0 } }) : value) }).valid, false);
assert.equal(verifyQualificationSummary({ ...valid, receipts: valid.receipts.map((value, index) => index === 3 ? receipt({ install: { command: "artifact present", exitCode: 0 }, compile: null }) : value) }).valid, false);
assert.equal(verifyQualificationSummary({ ...valid, receipts: valid.receipts.map((value, index) => index === 3 ? receipt({ status: "compiled", execution: null }) : value) }).valid, false);
