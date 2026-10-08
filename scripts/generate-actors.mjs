import { spawnSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

export function generationArgs(mode = "check") {
  if (!["check", "write", "rust"].includes(mode)) {
    throw new Error("usage: node scripts/generate-actors.mjs [check|write|rust]");
  }
  return ["run", "--offline", "--locked", "-p", "acyclic-actors", "--example", "actors-http-routes", "--", "--generate", mode, root];
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const result = spawnSync(process.env.ACYCLIC_CARGO_BIN || "cargo", generationArgs(process.argv[2]), {
    cwd: root, stdio: "inherit", windowsHide: true,
  });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`Actors Rust generation exited with ${result.status ?? "unknown"}`);
}