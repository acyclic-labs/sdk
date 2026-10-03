import { expect, test } from "bun:test";
import { access, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { tmpdir } from "node:os";

const root = fileURLToPath(new URL("../../../", import.meta.url));

async function source(relative: string): Promise<string> {
  return readFile(join(root, relative), "utf8");
}

const EMPTY_ARTIFACT_DIGEST =
  "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const VALID_RECEIPT_DIGEST = "sha256:" + "0".repeat(64);

function forgedManifest(tool: Record<string, unknown> | null): string {
  return JSON.stringify({
    schema: "acyclic.sdk.generation.manifest.v1",
    operation: "generate",
    status: "generated",
    source: {
      revision: "forged-revision",
      digest: "sha256:" + "0".repeat(64),
      dirty: false,
    },
    authoritative_source: null,
    generator: { name: "forged-acceptance-fixture", version: "0" },
    tools: tool === null ? [] : [tool],
    artifacts: [],
    artifact_digest: EMPTY_ARTIFACT_DIGEST,
    languages: [],
  });
}

function runDrift(sourceRoot: string, output: string) {
  const manifest = join(root, "rust", "crates", "sdk-generation", "Cargo.toml");
  return spawnSync(
    "cargo",
    [
      "run",
      "--manifest-path",
      manifest,
      "--locked",
      "--offline",
      "--",
      "drift",
      "--source-root",
      sourceRoot,
      "--output",
      output,
    ],
    {
      cwd: root,
      encoding: "utf8",
      timeout: 120_000,
      env: { ...process.env, CARGO_NET_OFFLINE: "true" },
    },
  );
}

async function initializeGitSource(sourceRoot: string): Promise<void> {
  await writeFile(join(sourceRoot, "input.txt"), "acceptance source\n");
  for (const args of [
    ["init", "--quiet"],
    ["config", "user.email", "acceptance@example.invalid"],
    ["config", "user.name", "Acceptance Fixture"],
    ["add", "input.txt"],
    ["commit", "--quiet", "-m", "fixture"],
  ]) {
    const result = spawnSync("git", args, { cwd: sourceRoot, encoding: "utf8" });
    if (result.status !== 0) {
      throw new Error(`git fixture setup failed: ${args.join(" ")}\n${result.stderr}`);
    }
  }
}

async function forgedCliFixture(tool: Record<string, unknown>) {
  const fixture = await mkdtemp(join(tmpdir(), "sdk-generation-drift-"));
  const sourceRoot = join(fixture, "source");
  const output = join(fixture, "output");
  await Promise.all([
    mkdir(sourceRoot, { recursive: true }),
    mkdir(output, { recursive: true }),
  ]);
  await initializeGitSource(sourceRoot);
  await writeFile(join(output, "sdk-generation-manifest.json"), forgedManifest(tool));
  return { fixture, sourceRoot, output };
}

function functionBody(main: string, name: string, next: string): string {
  const start = main.indexOf(`fn ${name}`);
  const end = main.indexOf(`\nfn ${next}`, start + 1);
  expect(start, `${name} must exist`).toBeGreaterThanOrEqual(0);
  expect(end, `${next} must follow ${name}`).toBeGreaterThan(start);
  return main.slice(start, end);
}

test("forged generated status with a failed required tool is a drift rejection fixture", async () => {
  const [manifestText, main] = await Promise.all([
    source("research/acceptance/typescript-review/fixtures/generation-manifest-forged-required-tool.json"),
    source("rust/crates/sdk-generation/src/main.rs"),
  ]);
  const manifest = JSON.parse(manifestText) as {
    status: string;
    tools: Array<{
      status: string;
      required: boolean;
      stdout_sha256: string | null;
      stderr_sha256: string | null;
      exit_code: number | null;
    }>;
  };
  expect(manifest.status).toBe("generated");
  expect(manifest.tools.some(tool => tool.status === "failed" && tool.required)).toBe(true);
  expect(
    manifest.tools.some(
      tool => tool.status === "passed" && tool.required && tool.exit_code === 0
        && tool.stdout_sha256 === null && tool.stderr_sha256 === null,
    ),
  ).toBe(true);

  const drift = functionBody(main, "drift", "qualify");
  expect(drift).toContain("verify_required_tools(&manifest.tools, source_root, output)?");
});

test("check validates required tool receipts before trusting generated status", async () => {
  const main = await source("rust/crates/sdk-generation/src/main.rs");
  const check = functionBody(main, "check", "drift");
  const verifier = functionBody(main, "verify_required_tools", "is_commit_revision");

  expect(check).toContain("verify_required_tools(&manifest.tools, source_root, output)?");
  expect(verifier).toMatch(/tool\.required/);
  expect(verifier).toMatch(/tool\.status/);
  expect(verifier).toMatch(/tool\.exit_code/);
  expect(verifier).toContain("stdout_sha256");
  expect(verifier).toContain("stderr_sha256");
  expect(verifier).toContain("is_sha256");
  expect(verifier).toContain("source_binding");
  expect(verifier).toContain("output_binding");
});

test("real drift CLI rejects a forged generated manifest with a failed required tool", { timeout: 120_000 }, async () => {
  const fixture = await forgedCliFixture({
    id: "sdk-docs",
    status: "failed",
    required: true,
    command: ["forged-tool"],
    request: "forged-request.json",
    stdout_sha256: null,
    stderr_sha256: null,
    exit_code: 1,
    message: "forged failure",
  });
  try {
    const result = runDrift(fixture.sourceRoot, fixture.output);
    const combined = `${result.stdout ?? ""}\n${result.stderr ?? ""}`;
    expect(result.status).not.toBe(0);
    expect(combined).toMatch(/required|failed/i);
    await expect(access(join(fixture.output, "sdk-generation-drift.json"))).rejects.toThrow();
  } finally {
    await rm(fixture.fixture, { recursive: true, force: true });
  }
});

test("real drift CLI rejects a required passed tool without hashed output receipts", { timeout: 120_000 }, async () => {
  const fixture = await forgedCliFixture({
    id: "sdk-docs",
    status: "passed",
    required: true,
    command: ["forged-tool"],
    request: "forged-request.json",
    stdout_sha256: null,
    stderr_sha256: null,
    exit_code: 0,
    message: null,
  });
  try {
    const result = runDrift(fixture.sourceRoot, fixture.output);
    const combined = `${result.stdout ?? ""}\n${result.stderr ?? ""}`;
    expect(result.status).not.toBe(0);
    expect(combined).toMatch(/required|receipt|stdout|stderr/i);
    await expect(access(join(fixture.output, "sdk-generation-drift.json"))).rejects.toThrow();
  } finally {
    await rm(fixture.fixture, { recursive: true, force: true });
  }
});

test("real drift CLI rejects valid-looking receipts that are not bound to the source and output", { timeout: 120_000 }, async () => {
  const fixture = await forgedCliFixture({
    id: "sdk-docs",
    status: "passed",
    required: true,
    command: ["forged-tool", "wrong-source", "wrong-output"],
    request: "forged-request.json",
    stdout_sha256: VALID_RECEIPT_DIGEST,
    stderr_sha256: VALID_RECEIPT_DIGEST,
    exit_code: 0,
    message: null,
  });
  try {
    const result = runDrift(fixture.sourceRoot, fixture.output);
    const combined = `${result.stdout ?? ""}\n${result.stderr ?? ""}`;
    expect(result.status).not.toBe(0);
    expect(combined).toMatch(/source-bound|source.bound|bound/i);
    await expect(access(join(fixture.output, "sdk-generation-drift.json"))).rejects.toThrow();
  } finally {
    await rm(fixture.fixture, { recursive: true, force: true });
  }
});

test("real drift CLI rejects retained logs whose bytes do not match valid-looking receipt hashes", { timeout: 120_000 }, async () => {
  const fixture = await forgedCliFixture({
    id: "sdk-docs",
    status: "passed",
    required: true,
    command: [],
    request: "forged-request.json",
    stdout_sha256: VALID_RECEIPT_DIGEST,
    stderr_sha256: VALID_RECEIPT_DIGEST,
    exit_code: 0,
    message: null,
  });
  try {
    await mkdir(join(fixture.output, "logs"), { recursive: true });
    await writeFile(join(fixture.output, "logs", "sdk-docs.stdout"), "tampered stdout");
    await writeFile(join(fixture.output, "logs", "sdk-docs.stderr"), "tampered stderr");
    await writeFile(
      join(fixture.output, "sdk-generation-manifest.json"),
      forgedManifest({
        id: "sdk-docs",
        status: "passed",
        required: true,
        command: ["forged-tool", fixture.sourceRoot, fixture.output],
        request: "forged-request.json",
        stdout_sha256: VALID_RECEIPT_DIGEST,
        stderr_sha256: VALID_RECEIPT_DIGEST,
        exit_code: 0,
        message: null,
      }),
    );
    const result = runDrift(fixture.sourceRoot, fixture.output);
    const combined = `${result.stdout ?? ""}\n${result.stderr ?? ""}`;
    expect(result.status).not.toBe(0);
    expect(combined).toMatch(/valid passed|hashed|source-bound/i);
    await expect(access(join(fixture.output, "sdk-generation-drift.json"))).rejects.toThrow();
  } finally {
    await rm(fixture.fixture, { recursive: true, force: true });
  }
});

test("real drift CLI rejects manifests that omit or demote the required stage inventory", { timeout: 120_000 }, async () => {
  for (const tools of [
    [],
    [{
      id: "sdk-docs",
      status: "passed",
      required: false,
      command: ["forged-tool"],
      request: "forged-request.json",
      stdout_sha256: VALID_RECEIPT_DIGEST,
      stderr_sha256: VALID_RECEIPT_DIGEST,
      exit_code: 0,
      message: null,
    }],
  ]) {
    const fixture = await mkdtemp(join(tmpdir(), "sdk-generation-inventory-"));
    const sourceRoot = join(fixture, "source");
    const output = join(fixture, "output");
    await mkdir(sourceRoot, { recursive: true });
    await mkdir(output, { recursive: true });
    await initializeGitSource(sourceRoot);
    await writeFile(
      join(output, "sdk-generation-manifest.json"),
      JSON.stringify({
        schema: "acyclic.sdk.generation.manifest.v1",
        operation: "generate",
        status: "generated",
        source: { revision: "forged-revision", digest: "sha256:" + "0".repeat(64), dirty: false },
        authoritative_source: null,
        generator: { name: "forged-acceptance-fixture", version: "0" },
        tools,
        artifacts: [],
        artifact_digest: EMPTY_ARTIFACT_DIGEST,
        languages: [],
      }),
    );
    try {
      const result = runDrift(sourceRoot, output);
      const combined = `${result.stdout ?? ""}\n${result.stderr ?? ""}`;
      expect(result.status).not.toBe(0);
      expect(combined).toMatch(/required stages without valid|required generation stage|missing required stage|unknown required stage/i);
      await expect(access(join(output, "sdk-generation-drift.json"))).rejects.toThrow();
    } finally {
      await rm(fixture, { recursive: true, force: true });
    }
  }
});

test("required stage inventory names are realizable by the producer tool specs", async () => {
  const main = await source("rust/crates/sdk-generation/src/main.rs");
  for (const id of [
    "sdk-product-artifacts",
    "sdk-contract-wire",
    "sdk-contract-validation",
    "sdk-openapi-prototype",
    "sdk-examples",
    "sdk-docs-rustdoc",
    "sdk-docs",
    "sdk-typescript",
  ]) {
    expect(main.match(new RegExp(`\\\"${id}\\\"`, "g"))?.length ?? 0).toBeGreaterThan(1);
  }
});

test("check and drift bind the manifest generator identity to this executable", async () => {
  const main = await source("rust/crates/sdk-generation/src/main.rs");
  const check = functionBody(main, "check", "drift");
  const drift = functionBody(main, "drift", "qualify");
  expect(main).toContain('name: "acyclic-sdk-generation"');
  for (const operation of [check, drift]) {
    expect(operation).toContain("manifest.generator");
    expect(operation).toContain("CARGO_PKG_VERSION");
  }
});

test("Rust authority validation requires the complete registered eight-family matrix", async () => {
  const main = await source("rust/crates/sdk-generation/src/main.rs");
  const authority = functionBody(main, "verify_authority_manifest", "artifact_digest");
  expect(authority).toMatch(/openapi_family_names|explicit_http_family_views/);
  expect(authority).toMatch(/families.*len|families.*all|missing.*family/i);
});
