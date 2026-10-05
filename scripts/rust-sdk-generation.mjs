import { spawnSync } from "node:child_process";
import { basename, dirname, isAbsolute, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");

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
  if (!/^(generate|check|drift|inventory|qualify)$/.test(operation)) {
    throw new Error(`unsupported generation operation: ${operation}`);
  }
  const callerDirectory = resolve(options.callerDirectory ?? process.cwd());
  const sourceRoot = resolve(callerDirectory, optionValue(rawArgs, "--source-root") ?? options.repositoryRoot ?? repositoryRoot);
  const environment = { ...(options.environment ?? {}) };
  const outputArgument = optionValue(rawArgs, "--output");
  const configuredOutput = outputArgument ?? environment.ACYCLIC_SDK_GENERATION_OUTPUT;
  const temporaryRoot = resolve(options.temporaryRoot ?? process.env.TEMP ?? process.env.TMP ?? ".");
  const output = configuredOutput
    ? resolve(callerDirectory, configuredOutput)
    : join(temporaryRoot, "acyclic-sdk-generation", basename(sourceRoot), "output");
  const sourceToOutput = relative(sourceRoot, output);
  const outputToSource = relative(output, sourceRoot);
  if (sourceToOutput === "" || (!sourceToOutput.startsWith("..") && !isAbsolute(sourceToOutput)) ||
      outputToSource === "" || (!outputToSource.startsWith("..") && !isAbsolute(outputToSource))) {
    throw new Error(`generation output must be outside the Rust source root: ${output}`);
  }
  const target = environment.CARGO_TARGET_DIR ?? join(
    temporaryRoot,
    "acyclic-sdk-generation",
    basename(sourceRoot),
    "cargo-target",
  );
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
    options: { cwd: sourceRoot, env: { ...environment, CARGO_TARGET_DIR: target } },
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
