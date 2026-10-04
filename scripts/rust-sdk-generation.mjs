import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

export function generationInvocation(operation, inputArgs, {
  repositoryRoot = root, callerDirectory = process.cwd(),
  temporaryRoot = tmpdir(), environment = process.env,
} = {}) {
  if (!operation) throw new Error("Usage: rust-sdk-generation.mjs <generate|check|drift|inventory|qualify> [--source-root PATH] [--output PATH]");
  const args = [...inputArgs];
  for (let index = 0; index < args.length; index++) {
    if (args[index] === "--source-root" || args[index] === "--output") {
      if (!args[index + 1] || args[index + 1].startsWith("--")) throw new Error(`${args[index]} requires a path`);
      args[index + 1] = resolve(callerDirectory, args[index + 1]);
      index++;
    }
  }
  const sourceArgument = args.lastIndexOf("--source-root");
  const sourceRoot = sourceArgument >= 0 ? args[sourceArgument + 1] : resolve(repositoryRoot);
  // Stable per-checkout defaults keep both generated artifacts and build caches
  // outside the frozen source tree. Explicit caller paths remain authoritative.
  const identity = createHash("sha256").update(sourceRoot).digest("hex").slice(0, 24);
  const cacheRoot = join(resolve(temporaryRoot), "acyclic-sdk-generation", identity);
  const cliArgs = [operation, "--source-root", sourceRoot];
  if (!args.includes("--output")) cliArgs.push("--output", join(cacheRoot, "artifacts"));
  cliArgs.push(...args);
  return {
    program: environment.ACYCLIC_CARGO_BIN || environment.SDK_CARGO || "cargo",
    args: ["run", "--quiet", "--locked", "--bin", "sdk-generation", "--manifest-path",
      join(sourceRoot, "rust", "crates", "sdk-generation", "Cargo.toml"), "--", ...cliArgs],
    options: { cwd: sourceRoot,
      env: { ...environment, CARGO_TARGET_DIR: environment.CARGO_TARGET_DIR || join(cacheRoot, "compiler") },
      stdio: "inherit" },
  };
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try {
    const [operation, ...args] = process.argv.slice(2);
    const invocation = generationInvocation(operation, args);
    const result = spawnSync(invocation.program, invocation.args, invocation.options);
    if (result.error) console.error(result.error.message);
    process.exitCode = result.status ?? 1;
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
