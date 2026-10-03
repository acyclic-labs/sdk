import { expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { join } from "node:path";

const root = fileURLToPath(new URL("../../../", import.meta.url));

async function source(relative: string): Promise<string> {
  return readFile(join(root, relative), "utf8");
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
  expect(drift).toContain("verify_required_tools(&manifest.tools)?");
});

test("check validates required tool receipts before trusting generated status", async () => {
  const main = await source("rust/crates/sdk-generation/src/main.rs");
  const check = functionBody(main, "check", "drift");
  const verifier = functionBody(main, "verify_required_tools", "is_commit_revision");

  expect(check).toContain("verify_required_tools(&manifest.tools)?");
  expect(verifier).toMatch(/tool\.required/);
  expect(verifier).toMatch(/tool\.status/);
  expect(verifier).toMatch(/tool\.exit_code/);
  expect(verifier).toMatch(/stdout_sha256|stderr_sha256/);
});
