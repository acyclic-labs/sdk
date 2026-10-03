import { expect, test } from "bun:test";
import { cp, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { join } from "node:path";

const root = fileURLToPath(new URL("../../../", import.meta.url));

async function source(relative: string): Promise<string> {
  return readFile(join(root, relative), "utf8");
}

test("the examples receipt closure resolves transitive Rust model dependencies", async () => {
  const [closure, build, cargo, wire, tsEmitter] = await Promise.all([
    source("rust/crates/sdk-examples/src/source_closure.rs"),
    source("rust/crates/sdk-examples/build.rs"),
    source("rust/crates/sdk-examples/Cargo.toml"),
    source("rust/crates/sdk-contract-wire/src/bin/sdk-contract-wire.rs"),
    source("rust/crates/sdk-typescript/src/main.rs"),
  ]);
  expect(closure).toContain("cargo_metadata(&manifest)");
  expect(closure).toContain("collect_package_files(&source_root, package_dir, &mut files)");
  expect(closure).toContain("let model_files = files");
  expect(build).toContain("source_closure::closure_files");
  expect(build).toContain("source_closure::digest_files");
  expect(cargo).toContain('acyclic-objects = { path = "../objects" }');
  expect(cargo).toContain('acyclic-actors = { path = "../actors" }');
  expect(cargo).toContain('acyclic-stream = { path = "../stream", features = ["grpc"] }');
  expect(wire).toContain('"rust/crates/sdk-contract-wire/src/family_registry.rs"');
  expect(wire).toContain('"rust/crates/sdk-contract-wire/src/credential.rs"');
  expect(tsEmitter).toContain('/../sdk-contract-wire/src/credential.rs');
  expect(tsEmitter).toContain('model_source_content(&[lib, registry, credential])');
});

test("a compiled producer rejects a source snapshot with a mutated core dependency", async () => {
  const binary = join(root, "rust/crates/sdk-examples/target/debug/sdk-examples.exe");
  const scratch = await mkdtemp(join(tmpdir(), "acyclic-producer-dependency-"));
  try {
    const sourceRoot = join(scratch, "repo");
    await cp(join(root, "Cargo.toml"), join(sourceRoot, "Cargo.toml"));
    await cp(join(root, "Cargo.lock"), join(sourceRoot, "Cargo.lock"));
    await cp(join(root, "rust-toolchain.toml"), join(sourceRoot, "rust-toolchain.toml"));
    await cp(join(root, ".cargo"), join(sourceRoot, ".cargo"), { recursive: true });
    await cp(join(root, "rust/crates"), join(sourceRoot, "rust/crates"), {
      recursive: true,
      filter: sourcePath => !/[\\/]target(?:[\\/]|$)/.test(sourcePath),
    });
    await cp(join(root, "plugin"), join(sourceRoot, "plugin"), {
      recursive: true,
      filter: sourcePath => !/[\\/]target(?:[\\/]|$)/.test(sourcePath),
    });
    const dependency = join(sourceRoot, "rust/crates/objects/src/lib.rs");

    let requestNumber = 0;
    const run = (output: string) => {
      const request = join(scratch, `request-${++requestNumber}.json`);
      return writeFile(request, JSON.stringify({ source_root: sourceRoot, output }) + "\n").then(() =>
        spawnSync(binary, ["generate", "--request", request], { cwd: root, encoding: "utf8" }),
      );
    };
    const baseline = join(scratch, "baseline");
    const baselineRun = await run(baseline);
    expect(baselineRun.error).toBeUndefined();
    expect(baselineRun.status, `${baselineRun.stderr}\n${baselineRun.stdout}`).toBe(0);
    const baselineManifest = JSON.parse(
      await readFile(join(baseline, "sdk-examples-manifest.json"), "utf8"),
    ) as { source: { files: string[] } };
    for (const packageSource of [
      "sdk-examples/src/main.rs",
      "actors/src/lib.rs",
      "filesystem/src/lib.rs",
      "harness/src/lib.rs",
      "inference/src/lib.rs",
      "machines/src/lib.rs",
      "objects/src/lib.rs",
      "stream/src/lib.rs",
      "workers/src/lib.rs",
      "sdk-contract-wire/src/lib.rs",
      "sdk-contract-options/src/lib.rs",
      "sdk-contract-validation/src/lib.rs",
      "native-runtime/src/lib.rs",
    ]) {
      expect(baselineManifest.source.files).toContain(`rust/crates/${packageSource}`);
    }

    await writeFile(dependency, `${await readFile(dependency, "utf8")}\n// live core dependency edit\n`);
    expect(await readFile(dependency, "utf8")).toContain("live core dependency edit");
    const mutated = join(scratch, "mutated");
    const mutatedRun = await run(mutated);
    expect(mutatedRun.error).toBeUndefined();
    expect(mutatedRun.status).not.toBe(0);
    expect(`${mutatedRun.stderr}\n${mutatedRun.stdout}`).toContain("live example source closure does not match compiled producer");
    await expect(readFile(join(mutated, "sdk-examples-manifest.json"), "utf8")).rejects.toThrow();
  } finally {
    await rm(scratch, { recursive: true, force: true });
  }
}, 120_000);

test("source authority requires a configured digest and immutable origin revision", async () => {
  const docs = await source("rust/crates/sdk-docs/src/lib.rs");
  const start = docs.indexOf("fn verify_source_authority(");
  const end = docs.indexOf("fn is_portable_relative_path", start);
  expect(start).toBeGreaterThanOrEqual(0);
  const verifier = docs.slice(start, end);
  expect(verifier).toContain("authority_revision != source_revision");
  expect(verifier).toContain("authority_hash != actual_source_sha256");
  expect(verifier).toContain("configured_authority_sha256");
  expect(verifier).toContain("authority digest mismatch");
  expect(verifier).toContain("git_revision(repository_root)");
  expect(verifier).toContain("immutable origin revision");
});
