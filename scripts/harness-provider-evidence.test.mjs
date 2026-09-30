import { test } from "node:test";
import assert from "node:assert/strict";
import { collectProviderTests, verifyProviderTests, recursiveProviderTest } from "./harness-provider-evidence.mjs";

const name = recursiveProviderTest.slice(recursiveProviderTest.indexOf("::") + 2);
const transcript = `     Running tests/recursive_fork.rs (target/debug/deps/recursive_fork)\n\ntest ${name} ... ok\n`;
function evidence(source = transcript) {
  const parsed = collectProviderTests(source);
  return { rust_tests: [name], rust_provider_tests: parsed.tests, test_transcript_sha256: { provider: parsed.sha256 } };
}
test("provider proof accepts the actual integration target on Unix and Windows", () => {
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
