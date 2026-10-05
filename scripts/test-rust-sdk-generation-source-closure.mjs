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

function packageName(cargo) {
  const match = cargo.match(/^\s*name\s*=\s*"([^"]+)"\s*$/m);
  assert.ok(match, "Cargo manifest must declare a package name");
  return match[1];
}

function workspaceMembers(cargo) {
  const match = cargo.match(/members\s*=\s*\[([\s\S]*?)\]/m);
  assert.ok(match, "root Cargo.toml must declare workspace members");
  return [...match[1].matchAll(/"([^"]+)"/g)].map((entry) => entry[1]);
}

function directManifestDependencies(cargo) {
  const dependencies = new Map();
  let section = "";
  for (const line of cargo.split(/\r?\n/)) {
    if (/^\s*\[\[.*\]\]\s*$/.test(line)) {
      section = "";
      continue;
    }
    const header = line.match(/^\s*\[([^\]]+)\]\s*$/);
    if (header) {
      section = header[1];
      continue;
    }
    if (!/(^|\.)dependencies$/.test(section)) continue;
    const entry = line.match(/^\s*([A-Za-z0-9_-]+)\s*=\s*(.*)$/);
    if (!entry || /optional\s*=\s*true/.test(entry[2])) continue;
    const packageOverride = entry[2].match(/package\s*=\s*"([^"]+)"/);
    dependencies.set(entry[1], packageOverride?.[1] ?? entry[1]);
  }
  return dependencies;
}

function lockPackages(lock) {
  const packages = new Map();
  const blocks = lock.split(/\n(?=\[\[package\]\])/g).filter((block) => block.startsWith("[[package]]"));
  for (const block of blocks) {
    const name = block.match(/^name\s*=\s*"([^"]+)"/m)?.[1];
    if (!name) continue;
    const dependencySection = block.match(/\ndependencies\s*=\s*\[([\s\S]*?)\n\]/m)?.[1] ?? "";
    const dependencies = new Set(
      [...dependencySection.matchAll(/^\s*"([^"]+)/gm)].map((entry) => entry[1].split(/\s+/)[0]),
    );
    packages.set(name, dependencies);
  }
  return packages;
}

function assertWorkspaceLockClosure(rootCargo, lock) {
  const lockedPackages = lockPackages(lock);
  for (const member of workspaceMembers(rootCargo)) {
    const manifestPath = join(member, "Cargo.toml");
    assert.equal(existsSync(join(root, manifestPath)), true, manifestPath);
    const manifest = read(manifestPath);
    const name = packageName(manifest);
    const lockedDependencies = lockedPackages.get(name);
    assert.ok(lockedDependencies, `${name} is missing from the root Cargo.lock`);
    for (const [declaredName, packageNameOverride] of directManifestDependencies(manifest)) {
      assert.ok(
        lockedDependencies.has(packageNameOverride),
        `${name} declares ${declaredName} but Cargo.lock omits ${packageNameOverride}`,
      );
    }
  }
}

function assertFixtureSourceClosure(library, fixtures, hasSource) {
  assert.match(library, /^pub mod fixtures;\s*$/m,
    "SDK examples must export the fixture module used by qualification binaries");
  const modules = [...fixtures.matchAll(/^pub mod ([A-Za-z_][A-Za-z0-9_]*);\s*$/gm)]
    .map((entry) => entry[1]);
  assert.ok(modules.length > 0, "fixture module must expose its Rust sources");
  for (const name of modules) {
    assert.ok(hasSource(`${name}.rs`) || hasSource(`${name}/mod.rs`),
      `fixture module ${name} has no source file`);
  }
}

test("qualification fixtures have public module wiring and a closed source set", () => {
  const fixtureRoot = "rust/crates/sdk-examples/src/fixtures";
  assertFixtureSourceClosure(
    read("rust/crates/sdk-examples/src/lib.rs"),
    read(`${fixtureRoot}/mod.rs`),
    (path) => existsSync(join(root, fixtureRoot, path)),
  );
});

test("fixture source closure rejects missing library wiring and source modules", () => {
  assert.throws(
    () => assertFixtureSourceClosure("", "pub mod fixture_clock;", () => true),
    /SDK examples must export the fixture module/,
  );
  assert.throws(
    () => assertFixtureSourceClosure("pub mod fixtures;", "pub mod fixture_clock;", () => false),
    /fixture module fixture_clock has no source file/,
  );
});

test("every generation binary declared by Cargo has a source file", () => {
  const cargo = read("rust/crates/sdk-generation/Cargo.toml");
  const entries = binEntries(cargo);
  const sourceBinDir = join(root, "rust/crates/sdk-generation/src/bin");
  const sourceBins = readdirSync(sourceBinDir)
    .filter((name) => name.endsWith(".rs"))
    .map((name) => basename(name, ".rs"))
    .sort();
  assert.ok(sourceBins.includes("sdk-qualification-receipt"), "qualification receipt source must remain in the crate");
  assert.deepEqual(entries.map(({ name }) => name).sort(), [
    "sdk-generation",
    "sdk-platform-receipt",
    "sdk-stream-native-receipt",
    "sdk-runtime-consumer",
    "verify-observations",
    "verify-rpc-observations",
  ].sort());
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

test("workspace manifests have a closed root lockfile dependency graph", () => {
  assertWorkspaceLockClosure(read("Cargo.toml"), read("Cargo.lock"));
});

test("workspace lock closure rejects a missing declared dependency", () => {
  const rootCargo = read("Cargo.toml");
  const lock = read("Cargo.lock");
  const dependency = /(?<=name = "acyclic-sdk-luajit-remote"[\s\S]*?dependencies = \[[\s\S]*?)\n "acyclic-objects",/;
  assert.match(lock, dependency, "the mutation must target a real locked dependency");
  const brokenLock = lock.replace(dependency, "");
  assert.throws(
    () => assertWorkspaceLockClosure(rootCargo, brokenLock),
    /acyclic-sdk-luajit-remote declares acyclic-objects but Cargo\.lock omits acyclic-objects/,
  );
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

test("artifacts bind to the exact Git revision and declared Rust source scope", () => {
  const main = read("rust/crates/sdk-generation/src/main.rs");
  for (const marker of [
    'command_stdout(source_root, "git", &["rev-parse", "HEAD"])',
    '"source_git_sha_kind"',
    '"source_file_hashes"',
    '"source_files"',
    'fn source_identity_with_filter',
    '"ls-files", "-co", "--exclude-standard"',
    'ensure_source_identity_unchanged(&source, &source_after_tools, "generation")',
    'ensure_source_identity_unchanged(&source, &source_after_tools, "drift check")',
    'fn compare_fresh_artifacts(',
    'fn artifact_digest(',
  ]) {
    assert.match(main, new RegExp(marker.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")), marker);
  }
});

test("every required producer family has an orchestrator entry and invocation path", () => {
  const main = read("rust/crates/sdk-generation/src/main.rs");
  for (const id of [
    "sdk-product-artifacts",
    "sdk-contract-wire",
    "sdk-contract-validation",
    "sdk-openapi-prototype",
    "sdk-examples",
    "sdk-docs-rustdoc",
    "sdk-docs",
    "sdk-language-producers",
    "sdk-python",
    "sdk-typescript",
    "sdk-typescript-rpc-contracts",
  ]) {
    assert.match(main, new RegExp(`(?:id:|\\\")${id.replace(/[.*+?^${}()|[\\]\\]/g, "\\\\$&")}`), id);
  }
  for (const marker of [
    "fn run_tools(",
    "fn run_language_producers(",
    "fn run_openapi_projections(",
    "fn run_docs_rustdoc(",
    "fn tool_command(",
    "fn generated_package_roots(",
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
  const metadataStep = workflow.indexOf("cargo metadata --locked --format-version 1");
  const expensiveStep = workflow.indexOf("cargo test --manifest-path");
  assert.ok(metadataStep >= 0, "release qualification must preflight full locked Cargo metadata");
  assert.ok(expensiveStep > metadataStep, "locked metadata must run before qualification tests");
});

function assertIndependentCanonicalProducer(workflow) {
  const begin = workflow.indexOf('- name: Generate Rust-owned executable typed request manifest');
  const end = workflow.indexOf('- name: Build and consume generated package', begin);
  assert.ok(begin >= 0 && end > begin, 'manifest production must precede installed consumption');
  const producer = workflow.slice(begin, end);
  assert.match(producer, /export ACYCLIC_RUST_SOURCE_REVISION="\$\(git rev-parse HEAD\)"/);
  const invocations = [...producer.matchAll(/--bin typed-request-manifest > "([^"\n]+)"/g)].map(match => match[1]);
  assert.equal(invocations.length, 2, 'independent canonical evidence requires two Rust producer executions');
  assert.equal(new Set(invocations).size, 2, 'consumer input and canonical evidence must use distinct paths');
  assert.ok(invocations.includes('$RUNNER_TEMP/rust-canonical-typed-request-manifest.json'));
  const consumer = workflow.slice(end);
  assert.match(consumer, /ACYCLIC_RUST_CANONICAL_TYPED_REQUEST_MANIFEST:.*rust-canonical-typed-request-manifest\.json/);
}

test('additional language qualification independently regenerates canonical Rust evidence', () => {
  const workflow = read('.github/workflows/additional-language-qualification.yml');
  assertIndependentCanonicalProducer(workflow);
  const copied = workflow.replace(
    /cargo run --locked --manifest-path rust\/crates\/sdk-examples\/Cargo\.toml \\\r?\n\s*--bin typed-request-manifest > "\$RUNNER_TEMP\/rust-canonical-typed-request-manifest\.json"/,
    'cp "$RUNNER_TEMP/rust-typed-request-manifest.json" "$RUNNER_TEMP/rust-canonical-typed-request-manifest.json"',
  );
  assert.notEqual(copied, workflow, 'negative control must replace the actual independent invocation');
  assert.throws(() => assertIndependentCanonicalProducer(copied), /two Rust producer executions/);
  assert.throws(() => assertIndependentCanonicalProducer(workflow.replace(/export ACYCLIC_RUST_SOURCE_REVISION=.*\n/, '')), /ACYCLIC_RUST_SOURCE_REVISION/);
});

function assertGenerationModuleClosure(source, hasSource) {
  const declarations = [...source.matchAll(/((?:#\[[^\n]*\]\s*\n)*)mod ([A-Za-z_][A-Za-z0-9_]*);/g)];
  assert.ok(declarations.length > 0, 'generation entrypoint must declare its implementation modules');
  for (const [, attributes, name] of declarations) {
    const explicit = attributes.match(/#\[path = "([^"\n]+)"\]/)?.[1];
    const candidates = explicit ? [explicit] : [`${name}.rs`, `${name}/mod.rs`];
    assert.ok(candidates.some(hasSource), `generation module ${name} has no source: ${candidates.join(' or ')}`);
  }
}

test('generation entrypoint implementation modules have a closed source set', () => {
  const source = read('rust/crates/sdk-generation/src/main.rs');
  assertGenerationModuleClosure(source, path => existsSync(join(root, 'rust/crates/sdk-generation/src', path)));
});

test('generation module closure rejects omitted explicit and conventional modules', () => {
  const source = 'mod target_packaging;\n#[path = "observation_verifier.rs"]\n#[allow(dead_code)]\nmod observation_verifier;\n';
  const paths = new Set(['target_packaging.rs', 'observation_verifier.rs']);
  assertGenerationModuleClosure(source, path => paths.has(path));
  for (const missing of paths) {
    assert.throws(() => assertGenerationModuleClosure(source, path => paths.has(path) && path !== missing), /generation module .* has no source/);
  }
  assertGenerationModuleClosure('mod nested;\n', path => path === 'nested/mod.rs');
});
