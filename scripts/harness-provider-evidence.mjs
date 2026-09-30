import { createHash } from "node:crypto";

export const recursiveProviderTest = "recursive_fork::thousand_twenty_four_recursive_forks_keep_files_private_and_merge_only_project";
const digest = tests => createHash("sha256").update(`${tests.join("\n")}\n`).digest("hex");

// Integration target headers distinguish provider-backed executions from unit
// tests with the same name. Cargo's merged stdout/stderr is retained by packaging.
export function collectProviderTests(transcript) {
  let target;
  const tests = new Set();
  for (const line of transcript.replace(/\x1b\[[0-9;]*m/g, "").split(/\r?\n/)) {
    if (/\bRunning\s/.test(line) || /\bDoc-tests\s/.test(line)) {
      target = line.match(/\bRunning tests[\\/]([\w-]+)\.rs\b/)?.[1];
    }
    const passed = line.match(/^test (\S+) \.\.\. ok$/);
    if (target && passed) tests.add(`${target}::${passed[1]}`);
  }
  const sorted = [...tests].sort();
  return { tests: sorted, sha256: digest(sorted) };
}

export function verifyProviderTests(evidence) {
  const tests = evidence.rust_provider_tests;
  if (!Array.isArray(tests) || tests.some(test => typeof test !== "string") ||
      JSON.stringify(tests) !== JSON.stringify([...new Set(tests)].sort()) ||
      evidence.test_transcript_sha256?.provider !== digest(tests)) {
    throw new Error("provider integration transcript evidence is invalid");
  }
  for (const test of tests) {
    const separator = test.indexOf("::");
    if (separator < 1 || !evidence.rust_tests.includes(test.slice(separator + 2))) {
      throw new Error("provider integration evidence is absent from Rust package tests");
    }
  }
  if (!tests.includes(recursiveProviderTest)) {
    throw new Error("packaged 1024-fork provider scenario was not executed");
  }
  return evidence.test_transcript_sha256.provider;
}
