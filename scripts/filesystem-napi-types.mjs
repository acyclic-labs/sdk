import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { generateTypeDef } from "@napi-rs/cli";

const mode = process.argv[2];
if (mode !== "write" && mode !== "check") {
  throw new Error("usage: filesystem-napi-types.mjs write|check");
}

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const target = join(root, "typescript/packages/filesystem/generated/native/binding.d.ts");
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
  const fresh = Buffer.from(dts);
  if (mode === "write") {
    writeFileSync(target, fresh);
  } else if (!fresh.equals(readFileSync(target))) {
    throw new Error("filesystem N-API declarations are stale; run bun run generate:napi");
  }
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
