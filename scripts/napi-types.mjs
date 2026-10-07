import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { generateTypeDef } from "@napi-rs/cli";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");

/**
 * Generate or check N-API declarations from one Rust crate. The native build
 * artifact itself is deliberately not copied or published by this helper;
 * qualification owns that separate step.
 */
export async function generateNapiTypes({
  mode,
  family,
  manifest,
  target,
  forceBuildEnvironment,
}) {
  if (mode !== "write" && mode !== "check") {
    throw new Error(`usage: ${family}-napi-types.mjs write|check`);
  }
  const manifestPath = join(root, manifest);
  const targetPath = join(root, target);
  const temporary = mkdtempSync(join(tmpdir(), `acyclic-${family}-napi-types-`));
  try {
    const generated = spawnSync(process.env.ACYCLIC_CARGO_BIN || "cargo", [
      "build", "--locked", "--manifest-path", manifestPath,
    ], {
      cwd: root,
      stdio: "inherit",
      env: {
        ...process.env,
        NAPI_TYPE_DEF_TMP_FOLDER: temporary,
        [forceBuildEnvironment]: temporary,
      },
    });
    if (generated.status !== 0) {
      throw new Error(`${family} N-API declaration generation failed: ${generated.status ?? "unknown"}`);
    }
    const { dts, exports } = await generateTypeDef({ typeDefDir: temporary, cwd: root });
    if (exports.length === 0 || dts.length === 0) {
      throw new Error(`${family} Rust macros emitted no N-API declarations`);
    }
    const fresh = Buffer.from(dts);
    mkdirSync(dirname(targetPath), { recursive: true });
    if (mode === "write") {
      writeFileSync(targetPath, fresh);
    } else if (!fresh.equals(readFileSync(targetPath))) {
      throw new Error(`${family} N-API declarations are stale; run its generation entrypoint`);
    }
  } finally {
    rmSync(temporary, { recursive: true, force: true });
  }
}
