import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const mode = process.argv[2];
if (mode !== "write" && mode !== "check") {
  throw new Error("usage: filesystem-napi-types.mjs write|check");
}

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const target = join(root, "typescript/packages/filesystem/generated/native/binding.d.ts");
const temporary = mkdtempSync(join(tmpdir(), "acyclic-fs-napi-types-"));
try {
  const generated = spawnSync(join(root, "node_modules", ".bin", process.platform === "win32" ? "napi.exe" : "napi"), [
    "build",
    "--manifest-path", "rust/crates/filesystem-napi/Cargo.toml",
    "--output-dir", temporary,
    "--dts", "binding.d.ts",
  ], { cwd: root, stdio: "inherit" });
  if (generated.status !== 0) {
    throw new Error(`filesystem N-API declaration generation failed: ${generated.status ?? "unknown"}`);
  }
  const fresh = readFileSync(join(temporary, "binding.d.ts"));
  if (mode === "write") {
    writeFileSync(target, fresh);
  } else if (!fresh.equals(readFileSync(target))) {
    throw new Error("filesystem N-API declarations are stale; run bun run generate:napi");
  }
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
