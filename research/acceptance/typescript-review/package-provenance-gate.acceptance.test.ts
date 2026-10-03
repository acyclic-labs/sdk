import { expect, test } from "bun:test";
import { cp, mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { spawnSync } from "node:child_process";

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

test("a real SDK archive is extracted and resolved by a locked Cargo consumer", { timeout: 120_000 }, async () => {
  const fixture = await mkdtemp(join(tmpdir(), "sdk-package-provenance-"));
  try {
    const sourceRoot = resolve(root);
    const packageRoot = join(fixture, "acyclic-sdk-bundle");
    const packageCrates = join(packageRoot, "crates");
    await mkdir(packageCrates, { recursive: true });

    // Build the fixture from the repository's actual workspace and crate
    // sources.  This exercises the archive and Cargo resolver, rather than
    // accepting a text file or a generic disposable consumer.
    const workspace = (await readFile(join(sourceRoot, "Cargo.toml"), "utf8"))
      .replaceAll('"rust/crates/', '"crates/')
      .replace('  "plugin",\n', "");
    await writeFile(join(packageRoot, "Cargo.toml"), workspace);
    await cp(join(sourceRoot, "rust", "crates"), packageCrates, {
      recursive: true,
      filter: sourcePath => !/[\\/]target(?:[\\/]|$)/.test(sourcePath) && !/[\\/]\.git(?:[\\/]|$)/.test(sourcePath),
    });

    const archive = join(fixture, "acyclic-sdk-bundle.tgz");
    const packed = spawnSync("tar", ["-czf", archive, "-C", fixture, "acyclic-sdk-bundle"], {
      encoding: "utf8",
    });
    expect(packed.status, packed.stderr || packed.stdout).toBe(0);

    const extracted = join(fixture, "extracted");
    await mkdir(extracted);
    const unpacked = spawnSync("tar", ["-xzf", archive, "-C", extracted], {
      encoding: "utf8",
    });
    expect(unpacked.status, unpacked.stderr || unpacked.stdout).toBe(0);
    await expect(readFile(join(extracted, "acyclic-sdk-bundle", "crates", "actors", "Cargo.toml"), "utf8"))
      .resolves.toContain('name = "acyclic-actors"');
    await expect(readFile(join(extracted, "acyclic-sdk-bundle", "crates", "stream", "Cargo.toml"), "utf8"))
      .resolves.toContain('name = "acyclic-stream"');

    const consumer = join(fixture, "consumer");
    await mkdir(join(consumer, "src"), { recursive: true });
    await writeFile(
      join(consumer, "Cargo.toml"),
      `[package]\nname = "rendered-sdk-consumer"\nversion = "0.0.0"\nedition = "2024"\n\n[workspace]\n\n[dependencies]\nacyclic-actors = { path = "../extracted/acyclic-sdk-bundle/crates/actors" }\nacyclic-stream = { path = "../extracted/acyclic-sdk-bundle/crates/stream", features = ["grpc"] }\n`,
    );
    await writeFile(join(consumer, "src", "main.rs"), "fn main() {}\n");
    const lock = spawnSync("cargo", ["generate-lockfile", "--offline"], {
      cwd: consumer,
      encoding: "utf8",
      timeout: 120_000,
      env: { ...process.env, CARGO_NET_OFFLINE: "true" },
    });
    expect(lock.status, lock.stderr || lock.stdout).toBe(0);
    const lockText = await readFile(join(consumer, "Cargo.lock"), "utf8");
    expect(lockText).toContain('name = "acyclic-actors"');
    expect(lockText).toContain('name = "acyclic-stream"');
    expect(lockText).not.toContain("sdk-example-consumer");
  } finally {
    await rm(fixture, { recursive: true, force: true });
  }
});

test("source closure review keeps the resolved local package graph and build recipe in identity", { timeout: 30_000 }, async () => {
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

test("producer and importer use one canonical Cargo recipe helper", { timeout: 30_000 }, async () => {
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
