import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { promisify } from "node:util";
import test from "node:test";
import { fileURLToPath } from "node:url";

const run = promisify(execFile);
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const script = resolve(root, "scripts/build-actors-native.mjs");
const packagePath = resolve(root, "typescript/packages/actors/package.json");
const nativeTargetsRelative = "generated/native-targets.json";
const sourceRevision = "0123456789abcdef0123456789abcdef01234567";
const sourceDigest = "0".repeat(64);

function digest(bytes) {
  return `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
}

async function withBundle({ artifact: artifactOverrides = {}, manifest: manifestOverrides = {} } = {}, callback) {
  const directory = await mkdtemp(join(tmpdir(), "acyclic-actors-generation-bundle-"));
  const generated = join(directory, "generated");
  const artifactPath = join(generated, "native-targets.json");
  const packageJson = JSON.parse(await readFile(packagePath, "utf8"));
  const artifact = {
    schema: "acyclic.actors.native-targets.v1",
    package: "acyclic-actors-napi",
    version: packageJson.version,
    source_path: "rust/crates/actors-napi/Cargo.toml",
    source_revision: sourceRevision,
    source_sha256: sourceDigest,
    targets: ["x86_64-pc-windows-msvc"],
    ...artifactOverrides,
  };
  const artifactBytes = Buffer.from(`${JSON.stringify(artifact)}\n`);
  await mkdir(generated, { recursive: true });
  await writeFile(artifactPath, artifactBytes);
  const manifest = {
    schema: "acyclic.sdk.generation.v1",
    generator_version: "test",
    family: "acyclic_actors",
    version: packageJson.version,
    revision: sourceRevision,
    source_sha256: sourceDigest,
    artifacts: [{ path: nativeTargetsRelative, sha256: digest(artifactBytes), bytes: artifactBytes.length }],
    ...manifestOverrides,
  };
  await writeFile(join(directory, "generation-manifest.json"), `${JSON.stringify(manifest)}\n`);
  try {
    return await callback(directory, artifact);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

async function invoke(args) {
  try {
    const result = await run(process.execPath, [script, ...args], { cwd: root, windowsHide: true });
    return { status: 0, stdout: result.stdout, stderr: result.stderr };
  } catch (error) {
    return {
      status: error.status ?? 1,
      stdout: error.stdout ?? "",
      stderr: error.stderr ?? String(error),
    };
  }
}

test("native builder uses the attested Rust target metadata through a dry-run NAPI operation", async () => {
  const packageJson = JSON.parse(await readFile(packagePath, "utf8"));
  assert.equal(packageJson.napi.targets, undefined);
  await withBundle({}, async bundle => {
    const result = await invoke(["prepare", "--dry-run", "--bundle", bundle]);
    assert.equal(result.status, 0, result.stderr || result.stdout);
  });
});

test("native builder rejects stale or malformed bundle identity before invoking Cargo", async () => {
  const invalidCases = [
    [{ artifact: { version: "9.9.9" } }, /version|manifest/i],
    [{ artifact: { source_revision: "not-a-revision" } }, /revision/i],
    [{ artifact: { source_sha256: "bad" } }, /digest/i],
    [{ artifact: { targets: ["x86_64-pc-windows-msvc", "x86_64-pc-windows-msvc"] } }, /unique/i],
    [{ manifest: { revision: "fedcba9876543210fedcba9876543210fedcba98" } }, /revision/i],
    [{ manifest: { source_sha256: "1".repeat(64) } }, /digest/i],
  ];
  for (const [fixture, message] of invalidCases) {
    await withBundle(fixture, async bundle => {
      const result = await invoke(["build", "--target", "x86_64-pc-windows-msvc", "--bundle", bundle]);
      assert.notEqual(result.status, 0);
      assert.match(`${result.stdout}\n${result.stderr}`, message);
    });
  }
});

test("native builder rejects an artifact absent from the bundle attestation", async () => {
  await withBundle({ manifest: { artifacts: [] } }, async bundle => {
    const result = await invoke(["build", "--target", "x86_64-pc-windows-msvc", "--bundle", bundle]);
    assert.notEqual(result.status, 0);
    assert.match(`${result.stdout}\n${result.stderr}`, /does not attest/i);
  });
});

test("native builder rejects a target absent from the Rust artifact", async () => {
  await withBundle({}, async bundle => {
    const result = await invoke(["build", "--target", "aarch64-unknown-linux-gnu", "--bundle", bundle]);
    assert.notEqual(result.status, 0);
    assert.match(`${result.stdout}\n${result.stderr}`, /unsupported Actors N-API target/i);
  });
});

test("native builder requires an explicit generation bundle", async () => {
  const result = await invoke(["prepare", "--dry-run"]);
  assert.notEqual(result.status, 0);
  assert.match(`${result.stdout}\n${result.stderr}`, /require --bundle/i);
});

test("native builder exposes the generation bundle input in help", async () => {
  const result = await invoke(["--help"]);
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /--bundle <generation-bundle>/);
  assert.doesNotMatch(result.stdout, /--native-targets/);
});
