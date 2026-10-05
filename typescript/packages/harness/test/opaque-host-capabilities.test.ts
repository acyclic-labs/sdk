import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { describe, expect, test } from "bun:test";

const packageRoot = join(import.meta.dir, "..");
const publicSurfaces = [
  "generated/wasm/acyclic_harness_wasm.d.ts",
  "generated/wasm/acyclic_harness_wasm_bg.wasm.d.ts",
  "generated/proto/harness/v2/harness_pb.d.ts",
  "src/index.ts",
] as const;

const hostOnlyCapabilityNames = [
  "ExecutionResolutionCapability",
  "SwarmUsageLimiter",
  "SwarmDispatchToken",
] as const;

const hostOnlyCredentialFields = [
  "operator_principal",
  "completed_boundary_digest",
  "workspace_generation_digest",
] as const;

async function publicSurfaceText(): Promise<string> {
  return (await Promise.all(publicSurfaces.map(async relativePath =>
    readFile(join(packageRoot, relativePath), "utf8")
  ))).join("\n");
}

describe("opaque native capability boundary", () => {
  test("keeps host-only capability types out of public WASM and JSON declarations", async () => {
    const source = await publicSurfaceText();
    for (const name of hostOnlyCapabilityNames) {
      expect(source).not.toMatch(new RegExp(`\\b${name}\\b`));
    }
  });

  test("keeps host credentials out of public JSON field names", async () => {
    const source = await publicSurfaceText();
    for (const field of hostOnlyCredentialFields) {
      expect(source).not.toMatch(new RegExp(`(?:^|[\\s\\\"'])${field}(?:$|[\\s\\\"'])`));
    }
  });
});
