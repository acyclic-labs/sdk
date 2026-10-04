import assert from "node:assert/strict";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { basename, dirname, join, resolve } from "node:path";
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
  const sourceBinDir = join(root, "rust/crates/sdk-generation/src/bin");
  const sourceBins = readdirSync(sourceBinDir)
    .filter((name) => name.endsWith(".rs"))
    .map((name) => basename(name, ".rs"))
    .sort();
  assert.ok(sourceBins.includes("sdk-qualification-receipt"), "qualification receipt source must remain in the crate");
  assert.deepEqual(entries.map(({ name }) => name), [
    "sdk-generation",
    "sdk-platform-receipt",
    "sdk-stream-native-receipt",
  ]);
  for (const entry of entries) {
    assert.equal(existsSync(join(root, "rust/crates/sdk-generation", entry.path)), true, entry.path);
  }
  for (const sourceBin of sourceBins) {
    assert.equal(
      sourceBin === "sdk-qualification-receipt" || entries.some(({ name }) => name === sourceBin),
      true,
      `untracked generation binary source: ${sourceBin}`,
    );
  }
  assert.match(cargo, /edition\s*=\s*"2024"/);
});

test("README and package hooks document one Rust-owned generation path", () => {
  const readme = read("rust/crates/sdk-generation/README.md");
  assert.match(readme, /cargo run --manifest-path rust\/crates\/sdk-generation\/Cargo\.toml -- generate --source-root/);
  assert.match(readme, /cargo run --manifest-path rust\/crates\/sdk-generation\/Cargo\.toml -- drift --source-root/);

  const main = read("rust/crates/sdk-generation/src/main.rs");
  for (const marker of [
    'id: "sdk-language-producers"',
    'id: "sdk-python"',
    'id: "sdk-typescript"',
    'id: "sdk-typescript-rpc-contracts"',
    '"packages-write"',
    '"packages-check"',
    '"--schema-root"',
    '"--source-authority"',
    'id: "sdk-examples"',
    'id: "sdk-docs"',
  ]) {
    assert.match(main, new RegExp(marker.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")), marker);
  }
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
    "ensure_source_identity_unchanged(",
    "compare_fresh_artifacts(",
    "artifact_digest(",
    "fn catalog_exclusion_allowed(",
    "fn exclusion_test_receipt(",
    "seal_is_a_distinct_manifest_operation",
    "exclusion_requires_a_run_bound_receipt",
    "exclusion_receipt_binds_scope_and_digest",
    "source_digest_changes_for_untracked_author_input",
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
  assert.match(workflow, /generate --source-root "\$GITHUB_WORKSPACE" --output "\$out"/);
  assert.match(workflow, /drift --source-root "\$GITHUB_WORKSPACE" --output "\$out"/);
});
