import { expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { cp, mkdir, mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { tmpdir } from "node:os";

const root = fileURLToPath(new URL("../../../", import.meta.url));
const authorityFixture = join(root, "staged-jvm-dotnet-evidence/authority");
const requiredTools = [
  "sdk-product-artifacts",
  "sdk-contract-wire",
  "sdk-contract-validation",
  "sdk-openapi-prototype",
  "sdk-examples",
  "sdk-docs-rustdoc",
  "sdk-docs",
  "sdk-typescript",
];
const emptyDigest = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

function descriptorFields(bytes: Uint8Array): Array<[number, number | Uint8Array]> {
  const fields: Array<[number, number | Uint8Array]> = [];
  let cursor = 0;
  while (cursor < bytes.length) {
    let key = 0;
    let shift = 0;
    while (true) {
      const byte = bytes[cursor++];
      key |= (byte & 0x7f) << shift;
      if ((byte & 0x80) === 0) break;
      shift += 7;
    }
    const field = key >>> 3;
    const wire = key & 7;
    if (wire === 0) {
      let value = 0;
      shift = 0;
      while (true) {
        const byte = bytes[cursor++];
        value |= (byte & 0x7f) << shift;
        if ((byte & 0x80) === 0) break;
        shift += 7;
      }
      fields.push([field, value]);
    } else if (wire === 2) {
      let length = 0;
      shift = 0;
      while (true) {
        const byte = bytes[cursor++];
        length |= (byte & 0x7f) << shift;
        if ((byte & 0x80) === 0) break;
        shift += 7;
      }
      fields.push([field, bytes.slice(cursor, cursor + length)]);
      cursor += length;
    } else if (wire === 1) {
      cursor += 8;
    } else if (wire === 5) {
      cursor += 4;
    } else {
      throw new Error(`unsupported descriptor wire type ${wire}`);
    }
  }
  return fields;
}

function descriptorShapes(bytes: Uint8Array, source: string): string[] {
  const shapes = new Set<string>();
  for (const [field, value] of descriptorFields(bytes)) {
    if (field !== 1 || !(value instanceof Uint8Array)) continue;
    const fileFields = descriptorFields(value);
    const fileName = fileFields.find(([number]) => number === 1)?.[1];
    if (!(fileName instanceof Uint8Array) || Buffer.from(fileName).toString() !== source) continue;
    for (const [serviceField, service] of fileFields) {
      if (serviceField !== 6 || !(service instanceof Uint8Array)) continue;
      for (const [methodField, method] of descriptorFields(service)) {
        if (methodField !== 2 || !(method instanceof Uint8Array)) continue;
        let client = false;
        let server = false;
        for (const [streamField, stream] of descriptorFields(method)) {
          if (streamField === 5) client = stream !== 0;
          if (streamField === 6) server = stream !== 0;
        }
        shapes.add(client && server ? "bidi" : client ? "client" : server ? "server" : "unary");
      }
    }
  }
  return [...shapes].sort();
}

function digest(bytes: Uint8Array): string {
  return `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
}

function runGit(cwd: string, args: string[]): void {
  const result = spawnSync("git", args, { cwd, encoding: "utf8" });
  if (result.status !== 0) throw new Error(`git ${args.join(" ")} failed: ${result.stderr}`);
}

function runDrift(sourceRoot: string, output: string) {
  return spawnSync("cargo", [
    "run", "--manifest-path", join(root, "rust/crates/sdk-generation/Cargo.toml"),
    "--locked", "--offline", "--", "drift", "--source-root", sourceRoot, "--output", output,
  ], { cwd: root, encoding: "utf8", env: { ...process.env, CARGO_NET_OFFLINE: "true" } });
}

async function makeFixture() {
  const scratch = await mkdtemp(join(tmpdir(), "sdk-generation-authority-cli-"));
  const sourceRoot = join(scratch, "source");
  const output = join(scratch, "output");
  await mkdir(sourceRoot, { recursive: true });
  await mkdir(output, { recursive: true });

  const original = JSON.parse(await readFile(join(authorityFixture, "rust-authority.json"), "utf8")) as {
    source_files: string[];
    source_file_hashes: Record<string, string>;
    source_revision: string;
    families: Array<Record<string, any>>;
  };
  const canonical = createHash("sha256");
  const sourceHashes: Record<string, string> = {};
  for (const relative of original.source_files) {
    const sourcePath = join(root, relative);
    const destination = join(sourceRoot, relative);
    await mkdir(join(destination, ".."), { recursive: true });
    const bytes = await readFile(sourcePath);
    await writeFile(destination, bytes);
    sourceHashes[relative] = digest(bytes).slice("sha256:".length);
  }
  for (const relative of original.source_files) {
    const bytes = await readFile(join(sourceRoot, relative));
    canonical.update(relative);
    canonical.update(Buffer.from([0]));
    canonical.update(bytes);
    canonical.update(Buffer.from([0]));
  }
  original.source_file_hashes = sourceHashes;
  original.source_revision = canonical.digest("hex");

  runGit(sourceRoot, ["init", "--quiet"]);
  runGit(sourceRoot, ["config", "user.email", "authority@example.invalid"]);
  runGit(sourceRoot, ["config", "user.name", "Authority Fixture"]);
  runGit(sourceRoot, ["add", "."]);
  runGit(sourceRoot, ["commit", "--quiet", "-m", "authority fixture"]);
  const revision = spawnSync("git", ["rev-parse", "HEAD"], { cwd: sourceRoot, encoding: "utf8" }).stdout.trim();
  const sourceDigestHash = createHash("sha256");
  for (const relative of [...original.source_files].sort()) {
    sourceDigestHash.update(relative);
    sourceDigestHash.update(Buffer.from([0]));
    sourceDigestHash.update(await readFile(join(sourceRoot, relative)));
    sourceDigestHash.update(Buffer.from([0]));
  }
  const sourceIdentity = { revision, digest: `sha256:${sourceDigestHash.digest("hex")}`, dirty: false };

  await cp(authorityFixture, join(output, "wire"), { recursive: true });
  for (const family of original.families) {
    const descriptor = await readFile(join(output, "wire", family.descriptor));
    family.rpc_shapes = descriptorShapes(descriptor, family.source);
  }
  await writeFile(join(output, "wire/rust-authority.json"), JSON.stringify(original, null, 2) + "\n");
  await mkdir(join(output, "logs"), { recursive: true });
  const tools = requiredTools.map(id => {
    const command = ["authority-fixture", sourceRoot, output];
    return {
      id, status: "passed", required: true, command, request: "authority-fixture-request.json",
      stdout_sha256: emptyDigest, stderr_sha256: emptyDigest, exit_code: 0, message: null,
    };
  });
  for (const tool of tools) {
    await writeFile(join(output, "logs", `${tool.id}.stdout`), "");
    await writeFile(join(output, "logs", `${tool.id}.stderr`), "");
  }
  const artifacts: Array<{ path: string; sha256: string; bytes: number }> = [];
  async function collect(directory: string) {
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      const full = join(directory, entry.name);
      if (entry.isDirectory()) {
        await collect(full);
      } else if (entry.isFile()) {
        const relative = full.slice(output.length + 1).replaceAll("\\", "/");
        if (!relative.startsWith("logs/") && relative !== "sdk-generation-manifest.json") {
          const bytes = await readFile(full);
          artifacts.push({ path: relative, sha256: digest(bytes), bytes: bytes.length });
        }
      }
    }
  }
  await collect(output);
  artifacts.sort((left, right) => left.path.localeCompare(right.path));
  const artifactDigest = createHash("sha256");
  for (const artifact of artifacts) {
    artifactDigest.update(artifact.path);
    artifactDigest.update(Buffer.from([0]));
    artifactDigest.update(artifact.sha256);
    artifactDigest.update(Buffer.from([0]));
  }
  const manifest = {
    schema: "acyclic.sdk.generation.manifest.v1", operation: "generate", status: "generated",
    source: sourceIdentity, authoritative_source: sourceIdentity,
    generator: { name: "acyclic-sdk-generation", version: "0.1.0" }, tools,
    artifacts, artifact_digest: `sha256:${artifactDigest.digest("hex")}`, languages: [],
  };
  await writeFile(join(output, "sdk-generation-manifest.json"), JSON.stringify(manifest, null, 2) + "\n");
  return { scratch, sourceRoot, output };
}

test("real drift accepts a coherent authority fixture before mutation", { timeout: 120_000 }, async () => {
  const fixture = await makeFixture();
  try {
    const result = runDrift(fixture.sourceRoot, fixture.output);
    expect(result.status).toBe(0);
    expect(JSON.parse(await readFile(join(fixture.output, "sdk-generation-drift.json"), "utf8"))).toMatchObject({ status: "passed" });
  } finally {
    await rm(fixture.scratch, { recursive: true, force: true });
  }
});

test("real drift rejects a source mutation after the coherent authority baseline", { timeout: 120_000 }, async () => {
  const fixture = await makeFixture();
  try {
    const baseline = runDrift(fixture.sourceRoot, fixture.output);
    expect(baseline.status).toBe(0);
    const sourceFile = join(fixture.sourceRoot, "rust/crates/sdk-contract-wire/src/lib.rs");
    await writeFile(sourceFile, `${await readFile(sourceFile, "utf8")}\n// mutation after authority freeze\n`);
    const mutated = runDrift(fixture.sourceRoot, fixture.output);
    expect(mutated.status).not.toBe(0);
    expect(`${mutated.stdout}\n${mutated.stderr}`).toContain("source identity differs");
  } finally {
    await rm(fixture.scratch, { recursive: true, force: true });
  }
});

test("real drift rejects a removed authority family", { timeout: 120_000 }, async () => {
  const fixture = await makeFixture();
  try {
    const authorityPath = join(fixture.output, "wire/rust-authority.json");
    const authority = JSON.parse(await readFile(authorityPath, "utf8")) as { families: unknown[] };
    authority.families = authority.families.slice(1);
    await writeFile(authorityPath, JSON.stringify(authority, null, 2) + "\n");
    const result = runDrift(fixture.sourceRoot, fixture.output);
    expect(result.status).not.toBe(0);
    expect(`${result.stdout}\n${result.stderr}`).toMatch(/family|authority/i);
  } finally {
    await rm(fixture.scratch, { recursive: true, force: true });
  }
});

test("real drift rejects a forged generator identity", { timeout: 120_000 }, async () => {
  const fixture = await makeFixture();
  try {
    const manifestPath = join(fixture.output, "sdk-generation-manifest.json");
    const manifest = JSON.parse(await readFile(manifestPath, "utf8")) as { generator: { version: string } };
    manifest.generator.version = "999.0.0-forged";
    await writeFile(manifestPath, JSON.stringify(manifest, null, 2) + "\n");
    const result = runDrift(fixture.sourceRoot, fixture.output);
    expect(result.status).not.toBe(0);
    expect(`${result.stdout}\n${result.stderr}`).toMatch(/generator|version/i);
  } finally {
    await rm(fixture.scratch, { recursive: true, force: true });
  }
});
