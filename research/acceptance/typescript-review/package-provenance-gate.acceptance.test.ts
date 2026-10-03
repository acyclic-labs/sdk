import { expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { join } from "node:path";

const root = fileURLToPath(new URL("../../../", import.meta.url));

async function source(relative: string): Promise<string> {
  return readFile(join(root, relative), "utf8");
}

function functionBody(main: string, name: string, nextNames: string[]): string {
  const start = main.indexOf(`fn ${name}`);
  expect(start, `${name} must exist`).toBeGreaterThanOrEqual(0);
  const ends = nextNames
    .map(next => main.indexOf(`\nfn ${next}`, start + 1))
    .filter(index => index >= 0);
  const end = ends.length === 0 ? main.length : Math.min(...ends);
  return main.slice(start, end);
}

test("installed package lanes have no ambient cache or newest artifact fallback", async () => {
  const main = await source("rust/crates/sdk-examples/src/main.rs");
  const ruby = functionBody(main, "run_ruby", ["run_php", "run_dart"]);
  const php = functionBody(main, "run_php", ["run_dart", "run_typescript"]);

  expect(ruby).not.toContain("newest_artifact");
  expect(ruby).not.toMatch(/Q:\\sdk\\ruby-cache/i);
  expect(php).not.toMatch(/Q:\\sdk\\php-cache/i);
  expect(main).not.toMatch(/Q:\\sdk\\(?:ruby|php)-cache/i);

  expect(ruby).toContain('env::var_os("RUBY_ARTIFACT")');
  expect(php).toContain('env::var_os("PHP_PACKAGE_ROOT")');
  expect(ruby).toContain('attach_artifact(&mut receipt, "ruby-gem"');
  expect(php).toContain('attach_artifact(&mut receipt, "php-package-tree"');
});

test("Rust installed package receipts bind exact installed bytes to locked consumer resolution", async () => {
  const main = await source("rust/crates/sdk-examples/src/main.rs");
  const rust = functionBody(main, "run_rust", ["add_snippet_binding", "portable_output_path"]);

  // The package receipt must contain bytes copied from the installed output.
  // A hand-written text record that merely mentions the package tree is not
  // evidence for the artifact consumed by the qualification.
  expect(rust).toContain("fs::copy(&installed, &package_path)");
  expect(rust).not.toContain("let package_archive = format!(");
  expect(rust).toContain("let package_digest = hash(&fs::read(&package_path");
  expect(rust).toContain('"package_artifact_sha256": package_digest');
  expect(rust).toContain('"package_tree_sha256": package_tree_digest');
  expect(rust).toContain('"source_revision": source_revision');
  expect(rust).toContain('"compiled_snippet_sha256": snippet_digest');
  expect(rust).toMatch(/cargo test --manifest-path[\s\S]{0,120}--locked[\s\S]{0,80}--offline/);
  expect(rust).toContain('"consumer_manifest_sha256"');
  expect(rust).toContain('"consumer_lock_sha256"');
});

test("source closure review keeps the resolved local package graph and build recipe in identity", async () => {
  const [closure, build] = await Promise.all([
    source("rust/crates/sdk-examples/src/source_closure.rs"),
    source("rust/crates/sdk-examples/build.rs"),
  ]);

  expect(closure).toContain("cargo_metadata(&manifest)");
  expect(closure).toContain("collect_package_files(&source_root, package_dir, &mut files)");
  expect(closure).toContain("normalized_build_recipe");
  expect(closure).toContain("digest.update(recipe)");
  expect(build).toContain("source_closure::digest_files");
  expect(build).toContain("cargo:rustc-env=SDK_EXAMPLES_SOURCE_SHA256");
});

test("producer and importer use one canonical Cargo recipe helper", async () => {
  const [examples, examplesCargo, docs] = await Promise.all([
    source("rust/crates/sdk-examples/src/source_closure.rs"),
    source("rust/crates/sdk-examples/Cargo.toml"),
    source("rust/crates/sdk-docs/src/lib.rs"),
  ]);

  // A second recipe implementation can silently diverge on dependency kind,
  // feature, target, or path normalization while retaining the same framing.
  expect(examplesCargo).toContain('sdk-source-identity = { path = "../sdk-source-identity" }');
  expect(examples).toContain("sdk_source_identity::normalized_build_recipe");
  expect(examples).not.toMatch(/fn normalized_build_recipe\s*\(/);
  expect(docs).toContain("sdk_source_identity::normalized_build_recipe");
});
