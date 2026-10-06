import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { isAbsolute, join, relative, resolve } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";

export function runRustContractGenerator(root, mode, outputDirectory) {
  const sourceRoot = resolve(root);
  const outputRoot = outputDirectory
    ? resolve(outputDirectory)
    : mkdtempSync(join(tmpdir(), "acyclic-rust-contract-output-"));
  const ownsOutputDirectory = !outputDirectory;
  const isWithin = (parent, child) => {
    const path = relative(parent, child);
    return path === "" || (!path.startsWith("..") && !isAbsolute(path));
  };
  if (isWithin(sourceRoot, outputRoot) || isWithin(outputRoot, sourceRoot)) {
    if (ownsOutputDirectory) rmSync(outputRoot, { recursive: true, force: true });
    throw new Error(`Contract output must be outside the Rust source root: ${outputRoot}`);
  }
  const configuredTarget = process.env.CARGO_TARGET_DIR;
  const targetDirectory = resolve(root, configuredTarget
    ?? mkdtempSync(join(tmpdir(), "acyclic-rust-contract-target-")));
  const ownsTargetDirectory = !process.env.CARGO_TARGET_DIR;
  if (isWithin(sourceRoot, targetDirectory) || isWithin(targetDirectory, sourceRoot)) {
    if (ownsTargetDirectory) rmSync(targetDirectory, { recursive: true, force: true });
    throw new Error(`Cargo target directory must be outside the Rust source root: ${targetDirectory}`);
  }
  try {
    const result = spawnSync(
      process.env.ACYCLIC_CARGO_BIN || "cargo",
      [
        "run", "--manifest-path", join(root, "rust/crates/sdk-typescript/Cargo.toml"),
        "--bin", "sdk-contracts", "--locked", "--quiet", "--",
        mode, sourceRoot, outputRoot,
      ],
      {
        cwd: root,
        encoding: "utf8",
        env: { ...process.env, CARGO_TARGET_DIR: targetDirectory },
      },
    );
    if (result.status !== 0) {
      process.stderr.write(result.stdout ?? "");
      process.stderr.write(result.stderr ?? "");
      throw new Error(`Rust contract generator failed with status ${result.status ?? "unknown"}`);
    }
  } finally {
    if (ownsTargetDirectory) rmSync(targetDirectory, { recursive: true, force: true });
    if (ownsOutputDirectory) rmSync(outputRoot, { recursive: true, force: true });
  }
}

export function checkRustContractGenerator(root, mode, files, relativeDirectory) {
  const temporary = mkdtempSync(join(tmpdir(), "acyclic-rust-contracts-"));
  try {
    runRustContractGenerator(root, mode.replace(/-check$/, "-write"), temporary);
    for (const file of files) {
      const generated = join(temporary, relativeDirectory, file);
      const committed = join(root, relativeDirectory, file);
      if (!existsSync(generated) || !existsSync(committed) || !readFileSync(generated).equals(readFileSync(committed))) {
        throw new Error(`Rust-generated contract is stale; run bun run generate (${file})`);
      }
    }
  } finally {
    rmSync(temporary, { recursive: true, force: true });
  }
}

const invokedAsCli = process.argv[1] && resolve(process.argv[1]) === resolve(fileURLToPath(import.meta.url));
if (invokedAsCli) {
  const [mode, rootArgument, outputArgument] = process.argv.slice(2);
  if (!mode || !rootArgument || !outputArgument) {
    console.error("Usage: run-rust-contract-generator.mjs <family-write|family-check|all-write|all-check> <source-root> <output-root>");
    process.exitCode = 1;
  } else {
    try {
      const root = resolve(rootArgument);
      const output = resolve(outputArgument);
      if (!isAbsolute(root) || !isAbsolute(output)) {
        throw new Error("source-root and output-root must resolve to absolute paths");
      }
      runRustContractGenerator(root, mode, output);
    } catch (error) {
      console.error(error instanceof Error ? error.message : String(error));
      process.exitCode = 1;
    }
  }
}
