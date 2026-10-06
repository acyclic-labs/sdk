import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { dirname, isAbsolute, join, relative, resolve } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";

function configuredBuildRoot(root, options = {}, outputDirectory) {
  const configured = options.buildRoot
    ?? process.env.SDK_BUILD_ROOT
    ?? process.env.ACYCLIC_SDK_WORK_ROOT;
  if (configured) return resolve(root, configured);
  if (outputDirectory) return resolve(outputDirectory, "..");
  return undefined;
}

export function runRustContractGenerator(root, mode, outputDirectory, options = {}) {
  const sourceRoot = resolve(root);
  const buildRoot = configuredBuildRoot(sourceRoot, options, outputDirectory);
  const disposableRoot = buildRoot
    ? resolve(buildRoot, "acyclic-sdk-contracts")
    : undefined;
  if (disposableRoot) mkdirSync(disposableRoot, { recursive: true });
  const outputRoot = outputDirectory
    ? resolve(outputDirectory)
    : mkdtempSync(join(disposableRoot ?? tmpdir(), "output-"));
  const ownsOutputDirectory = !outputDirectory;
  const isWithin = (parent, child) => {
    const path = relative(parent, child);
    return path === "" || (!path.startsWith("..") && !isAbsolute(path));
  };
  if (isWithin(sourceRoot, outputRoot) || isWithin(outputRoot, sourceRoot)) {
    if (ownsOutputDirectory) rmSync(outputRoot, { recursive: true, force: true });
    throw new Error(`Contract output must be outside the Rust source root: ${outputRoot}`);
  }
  const configuredTarget = options.cargoTargetDirectory
    ?? process.env.CARGO_TARGET_DIR;
  const sharedTarget = configuredTarget
    ?? (disposableRoot ? join(disposableRoot, "cargo-target") : join(dirname(outputRoot), "cargo-target"));
  const targetDirectory = resolve(root, sharedTarget);
  // An explicit/shared target is intentionally retained for reuse. Only the
  // legacy fallback temporary target is disposable.
  const ownsTargetDirectory = !configuredTarget && !disposableRoot && !outputDirectory;
  if (isWithin(sourceRoot, targetDirectory) || isWithin(targetDirectory, sourceRoot)) {
    if (ownsTargetDirectory) rmSync(targetDirectory, { recursive: true, force: true });
    throw new Error(`Cargo target directory must be outside the Rust source root: ${targetDirectory}`);
  }
  try {
    const result = spawnSync(
      options.cargoProgram ?? process.env.ACYCLIC_CARGO_BIN ?? "cargo",
      [
        ...(options.cargoArgs ?? []),
        "run", "--manifest-path", join(root, "rust/crates/sdk-typescript/Cargo.toml"),
        "--bin", "sdk-contracts", "--locked", "--quiet", "--",
        mode, sourceRoot, outputRoot,
      ],
      {
        cwd: root,
        encoding: "utf8",
        env: {
          ...process.env,
          SDK_BUILD_ROOT: buildRoot ?? dirname(outputRoot),
          CARGO_TARGET_DIR: targetDirectory,
        },
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
  const sourceRoot = resolve(root);
  const buildRoot = configuredBuildRoot(sourceRoot);
  const temporaryParent = buildRoot
    ? join(buildRoot, "acyclic-rust-contracts")
    : tmpdir();
  mkdirSync(temporaryParent, { recursive: true });
  const temporary = mkdtempSync(join(temporaryParent, "output-"));
  try {
    runRustContractGenerator(sourceRoot, mode.replace(/-check$/, "-write"), temporary, { buildRoot });
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
