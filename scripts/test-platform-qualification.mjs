import assert from "node:assert/strict";
import test from "node:test";
import { validatePlatformQualification } from "./verify-platform-qualification.mjs";

const revision = "a".repeat(40);
const receipt = { schema: "acyclic.sdk.platform-qualification.v1", package: "cpp", source_revision: revision, status: "qualified" };

test("matching qualified platform receipt passes", () => {
  assert.doesNotThrow(() => validatePlatformQualification(receipt, "cpp", revision));
  assert.doesNotThrow(() => validatePlatformQualification({ ...receipt, package: "swift" }, "swift", revision));
});

test("pending host and conformance receipts fail", () => {
  for (const status of ["pending-host-toolchain", "pending-conformance", "pending-swiftpm-host", "failed", undefined]) {
    assert.throws(() => validatePlatformQualification({ ...receipt, status }, "cpp", revision), /qualification is/);
  }
});

test("stale or missing source revision fails", () => {
  assert.throws(() => validatePlatformQualification(receipt, "cpp", "b".repeat(40)), /revision mismatch/);
  assert.throws(() => validatePlatformQualification({ ...receipt, source_revision: undefined }, "cpp", revision), /revision mismatch/);
  assert.throws(() => validatePlatformQualification(receipt, "cpp", ""), /revision mismatch/);
});

test("wrong package and malformed receipt fail", () => {
  assert.throws(() => validatePlatformQualification(receipt, "swift", revision), /package mismatch/);
  assert.throws(() => validatePlatformQualification(null, "cpp", revision), /schema/);
});
