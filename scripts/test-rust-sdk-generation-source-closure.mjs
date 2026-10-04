import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const read = (path) => readFileSync(join(root, path), "utf8");

function binEntries(cargo) {
  return [...cargo.matchAll(/\[\[bin\]\]\s*\nname\s*=\s*"([^"]+)"\s*\npath\s*=\s*"([^"]+)"/g)]
    .map((match) => ({ name: match[1], path: match[2] }));
}

test("every generation binary declared by Cargo has a source file", () => {
  const cargo = read("rust/crates/sdk-generation/Cargo.toml");
  const entries = binEntries(cargo);
  assert.deepEqual(entries.map(({ name }) => name), [
    "sdk-generation",
    "sdk-platform-receipt",
    "sdk-stream-native-receipt",
  ]);
  for (const entry of entries) {
    assert.equal(existsSync(join(root, "rust/crates/sdk-generation", entry.path)), true, entry.path);
  }
  assert.match(cargo, /edition\s*=\s*"2024"/);
});

test("source closure keeps Rust provenance and Seal/exclusion guards", () => {
  const main = read("rust/crates/sdk-generation/src/main.rs");
  for (const marker of [
    "Operation::Seal",
    "fn seal(",
    "source_revision_kind",
    "source_git_sha",
    "source-authority.json",
    "sdk-typescript-rpc-contracts",
    "fn catalog_exclusion_allowed(",
    "fn exclusion_test_receipt(",
    "seal_is_a_distinct_manifest_operation",
    "exclusion_requires_a_run_bound_receipt",
    "exclusion_receipt_binds_scope_and_digest",
  ]) {
    assert.match(main, new RegExp(marker.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")), marker);
  }
});

test("all generated producers carry source revision metadata", () => {
  const main = read("rust/crates/sdk-generation/src/main.rs");
  const python = read("rust/crates/sdk-python/src/main.rs");
  const typescript = read("rust/crates/sdk-typescript/src/main.rs");
  for (const marker of ["--source-revision", "source-authority", "source.revision"]) {
    assert.match(main, new RegExp(marker.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")), marker);
  }
  for (const marker of ["source_revision", "source_revision_kind"]) {
    assert.match(python, new RegExp(marker), `Python ${marker}`);
  }
  assert.match(typescript, /source_revision/);
  assert.match(typescript, /sourceGitSha/);
  assert.match(typescript, /sourceGitShaKind/);
});

test("workflow pins actions, aligns Rust edition, and keeps downstream work out of PR fast policy", () => {
  const workflow = read(".github/workflows/rust-source-qualification.yml");
  for (const line of workflow.split(/\r?\n/)) {
    const match = line.match(/^\s*uses:\s+([^\s#]+)/);
    if (match && !match[1].startsWith("./")) assert.match(match[1], /@[0-9a-f]{40}$/i, line);
  }
  assert.match(workflow, /rustfmt --edition 2024 --check/);
  assert.match(workflow, /fast-policy:[\s\S]*?timeout-minutes:\s*2/);
  const fastPolicy = workflow.split(/\n\s{2}qualify:/, 1)[0];
  assert.doesNotMatch(fastPolicy, /cargo\s+(test|run|build)/);
  assert.match(workflow, /if: github\.event_name == 'release' \|\| github\.event_name == 'workflow_dispatch' \|\| github\.event_name == 'workflow_call'/);
});
