import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { generateTypeDef } from "@napi-rs/cli";

export async function render(_, root) {
  const temporary = mkdtempSync(join(tmpdir(), "acyclic-fs-napi-types-"));
  try {
    // Generate only declarations from fresh Rust macro output. The CLI's build
    // command also publishes native binaries/loaders through filesystem locks;
    // those artifacts are qualified separately and are not inputs to this check.
    const generated = spawnSync(process.env.ACYCLIC_CARGO_BIN || "cargo", [
      "build", "--locked", "--manifest-path", "rust/crates/filesystem-napi/Cargo.toml",
    ], { cwd: root, stdio: "inherit", env: {
      ...process.env,
      NAPI_TYPE_DEF_TMP_FOLDER: temporary,
      NAPI_FORCE_BUILD_ACYCLIC_FS_NAPI: temporary,
    } });
    if (generated.status !== 0) {
      throw new Error(`filesystem N-API declaration generation failed: ${generated.status ?? "unknown"}`);
    }
    const { dts, exports } = await generateTypeDef({ typeDefDir: temporary, cwd: root });
    if (exports.length === 0 || dts.length === 0) {
      throw new Error("filesystem Rust macros emitted no N-API declarations");
    }
    return { "typescript/packages/filesystem/generated/native/binding.d.ts": dts };
  } finally {
    rmSync(temporary, { recursive: true, force: true });
  }
}
