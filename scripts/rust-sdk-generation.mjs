import { spawnSync } from "node:child_process";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const [operation, ...args] = process.argv.slice(2);
if (!operation) {
  console.error("Usage: rust-sdk-generation.mjs <generate|check|drift|inventory|qualify> [--source-root PATH] [--output PATH]");
  process.exitCode = 1;
} else {
  // Resolve caller paths before selecting the source checkout's toolchain.
  for (let index = 0; index < args.length - 1; index++) {
    if (args[index] === "--source-root" || args[index] === "--output") {
      args[index + 1] = resolve(root, args[index + 1]);
      index++;
    }
  }
  const sourceArgument = args.lastIndexOf("--source-root");
  const sourceRoot = sourceArgument >= 0 && args[sourceArgument + 1]
    ? resolve(root, args[sourceArgument + 1])
    : root;
  const cliArgs = [operation, "--source-root", sourceRoot];
  if (!args.includes("--output")) cliArgs.push("--output", join(sourceRoot, "target", "sdk-generation"));
  cliArgs.push(...args);
  const result = spawnSync(process.env.ACYCLIC_CARGO_BIN || "cargo", [
    "run", "--quiet", "--locked",
    "--manifest-path", join(sourceRoot, "rust", "crates", "sdk-generation", "Cargo.toml"),
    "--", ...cliArgs,
  ], {
    cwd: sourceRoot,
    env: { ...process.env, CARGO_TARGET_DIR: process.env.CARGO_TARGET_DIR || join(sourceRoot, "target", "sdk-generation-cli") },
    stdio: "inherit",
  });
  if (result.error) console.error(result.error.message);
  process.exitCode = result.status ?? 1;
}
