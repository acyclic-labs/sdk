import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { realpathSync } from "node:fs";
import { basename, dirname, isAbsolute, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function sourceCacheKey(sourceRoot) {
  let canonicalRoot = sourceRoot;
  try {
    canonicalRoot = realpathSync.native(sourceRoot);
  } catch {
    // Unit tests and callers may describe a source root before creating it.
  }
  return createHash("sha256").update(canonicalRoot, "utf8").digest("hex").slice(0, 16);
}

function optionValue(args, option) {
  const index = args.lastIndexOf(option);
  if (index < 0) return undefined;
  if (!args[index + 1] || args[index + 1].startsWith("--")) {
    throw new Error(`${option} requires a path`);
  }
  return args[index + 1];
}

function withoutPathOption(args, option) {
  const result = [];
  for (let index = 0; index < args.length; index += 1) {
    if (args[index] === option) {
      if (!args[index + 1] || args[index + 1].startsWith("--")) {
        throw new Error(`${option} requires a path`);
      }
      index += 1;
      continue;
    }
    result.push(args[index]);
  }
  return result;
}

/** Build a reproducible invocation without writing compiler output into the source checkout. */
export function generationInvocation(operation, rawArgs = [], options = {}) {
  if (!/^(generate|catalog|check|drift|inventory|qualify)$/.test(operation)) {
    throw new Error(`unsupported generation operation: ${operation}`);
  }
  const callerDirectory = resolve(options.callerDirectory ?? process.cwd());
  const sourceRoot = resolve(callerDirectory, optionValue(rawArgs, "--source-root") ?? options.repositoryRoot ?? repositoryRoot);
  const environment = { ...(options.environment ?? {}) };
  const outputArgument = optionValue(rawArgs, "--output");
  const configuredOutput = outputArgument ?? environment.ACYCLIC_SDK_GENERATION_OUTPUT;
  // SDK_BUILD_ROOT is the one caller-owned location for all disposable
  // generation state.  Keep ACYCLIC_SDK_WORK_ROOT as a compatibility alias
  // for the PowerShell entrypoint while migrating callers to the canonical
  // setting.  The launcher deliberately does not invent a repository-local
  // or OS-specific path: local callers configure this to Q:\\sdk\\work and
  // CI configures it to the runner's temporary directory.
  const configuredBuildRoot = options.buildRoot
    ?? environment.SDK_BUILD_ROOT
    ?? environment.ACYCLIC_SDK_WORK_ROOT
    ?? process.env.SDK_BUILD_ROOT
    ?? process.env.ACYCLIC_SDK_WORK_ROOT;
  const temporaryRoot = resolve(
    callerDirectory,
    options.temporaryRoot
      ?? configuredBuildRoot
      ?? process.env.TEMP
      ?? process.env.TMP
      ?? ".",
  );
  const generationRoot = join(temporaryRoot, "acyclic-sdk-generation");
  const output = configuredOutput
    ? resolve(callerDirectory, configuredOutput)
    : join(generationRoot, basename(sourceRoot), "output");
  const sourceToOutput = relative(sourceRoot, output);
  const outputToSource = relative(output, sourceRoot);
  if (sourceToOutput === "" || (!sourceToOutput.startsWith("..") && !isAbsolute(sourceToOutput)) ||
      outputToSource === "" || (!outputToSource.startsWith("..") && !isAbsolute(outputToSource))) {
    throw new Error(`generation output must be outside the Rust source root: ${output}`);
  }
  // Cargo target and rustdoc cache state are keyed by the canonical source
  // root. A package name is not a sufficient identity when independent
  // worktrees share a build root: Cargo dep-info and rustdoc JSON can otherwise
  // be reused for the wrong checkout.
  const sourceKey = sourceCacheKey(sourceRoot);
  const target = resolve(callerDirectory, environment.CARGO_TARGET_DIR ?? join(
    generationRoot,
    sourceKey,
    "cargo-target",
  ));
  const docsCache = resolve(callerDirectory, environment.SDK_DOCS_RUSTDOC_CACHE_DIR
    ?? join(generationRoot, sourceKey, "rustdoc-cache"));
  const targetToSource = relative(sourceRoot, target);
  const sourceToTarget = relative(target, sourceRoot);
  if (targetToSource === "" || (!targetToSource.startsWith("..") && !isAbsolute(targetToSource)) ||
      sourceToTarget === "" || (!sourceToTarget.startsWith("..") && !isAbsolute(sourceToTarget))) {
    throw new Error(`Cargo target directory must be outside the Rust source root: ${target}`);
  }
  const docsCacheToSource = relative(sourceRoot, docsCache);
  const sourceToDocsCache = relative(docsCache, sourceRoot);
  if (docsCacheToSource === "" || (!docsCacheToSource.startsWith("..") && !isAbsolute(docsCacheToSource)) ||
      sourceToDocsCache === "" || (!sourceToDocsCache.startsWith("..") && !isAbsolute(sourceToDocsCache))) {
    throw new Error(`Rustdoc cache directory must be outside the Rust source root: ${docsCache}`);
  }
  const args = [
    "run", "--quiet", "--locked",
    "--manifest-path", join(sourceRoot, "rust", "crates", "sdk-generation", "Cargo.toml"),
    "--bin", "sdk-generation", "--",
    operation,
    "--source-root", sourceRoot,
    "--output", output,
    ...withoutPathOption(withoutPathOption(rawArgs, "--source-root"), "--output"),
  ];
  return {
    program: environment.SDK_CARGO ?? process.env.ACYCLIC_CARGO_BIN ?? "cargo",
    args,
    options: {
      cwd: sourceRoot,
      env: {
        ...environment,
        SDK_BUILD_ROOT: temporaryRoot,
        SDK_DOCS_RUSTDOC_CACHE_DIR: docsCache,
        CARGO_TARGET_DIR: target,
      },
    },
  };
}

const invokedAsCli = process.argv[1] && resolve(process.argv[1]) === resolve(fileURLToPath(import.meta.url));
const [operation, ...rawArgs] = process.argv.slice(2);
if (invokedAsCli && operation) {
  try {
    const plan = generationInvocation(operation, rawArgs, { repositoryRoot });
    const result = spawnSync(plan.program, plan.args, {
      cwd: plan.options.cwd,
      env: { ...process.env, ...plan.options.env },
      stdio: "inherit",
    });
    if (result.error) console.error(result.error.message);
    process.exitCode = result.status ?? 1;
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
} else if (invokedAsCli) {
  console.error("Usage: rust-sdk-generation.mjs <generate|check|drift|inventory|qualify> [--source-root PATH] [--output PATH]");
  process.exitCode = 1;
}
