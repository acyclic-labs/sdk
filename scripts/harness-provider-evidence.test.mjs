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

// Registration is checked before Map construction so neither side can hide duplicates.
import { readFileSync } from "node:fs";
import { harnessCaseMarkers, registerCases, requireExecutedCase } from "./harness-conformance-cases.mjs";
const suite = { cases: [{ name: "a", family: "harness" }, { name: "b", family: "harness" }] };
const registrations = [["harness", "a", [["rust", "test_a"]]], ["harness", "b", [["e2e", "test_b"]]]];

test("locked Harness names have exactly one executable owner", () => {
  const locked = JSON.parse(readFileSync(new URL("../conformance/vectors/core.json", import.meta.url), "utf8"));
  const registered = registerCases(locked, "harness", harnessCaseMarkers);
  assert.equal(registered.cases.length, 25);
  assert.deepEqual([...registered.markers.keys()], registered.cases.map(item => item.name));
  assert.ok(!registered.markers.has("filesystem-conflicts-are-explicit"));
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
