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

function forgedManifest(tool: Record<string, unknown>): string {
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
    tools: [tool],
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

async function forgedCliFixture(tool: Record<string, unknown>) {
  const fixture = await mkdtemp(join(tmpdir(), "sdk-generation-drift-"));
  const sourceRoot = join(fixture, "source");
  const output = join(fixture, "output");
  await Promise.all([
    mkdir(sourceRoot, { recursive: true }),
    mkdir(output, { recursive: true }),
  ]);
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
