import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { NapiCli } from "@napi-rs/cli";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const actorsPackageJson = "typescript/packages/actors/package.json";
const actorsManifest = "rust/crates/actors-napi/Cargo.toml";
const actorsOutput = "typescript/packages/actors/generated/native";

function usage() {
  return `usage:
  node scripts/build-actors-native.mjs build --target <rust-triple> [--debug]
  node scripts/build-actors-native.mjs prepare [--dry-run]
  node scripts/build-actors-native.mjs artifacts
  node scripts/build-actors-native.mjs stage [--dry-run]

build is intentionally single-target. Release automation fans out the pinned
matrix and then uses artifacts/stage to assemble installable packages.`;
}

function parseArgs(argv) {
  const command = argv[0]?.startsWith("-") ? "build" : (argv[0] ?? "build");
  const rest = argv[0]?.startsWith("-") ? argv : argv.slice(1);
  const options = { command, debug: false, dryRun: false };
  for (let index = 0; index < rest.length; index += 1) {
    const arg = rest[index];
    if (arg === "--target") {
      options.target = rest[++index];
    } else if (arg === "--target-dir") {
      options.targetDir = rest[++index];
    } else if (arg === "--debug") {
      options.debug = true;
    } else if (arg === "--dry-run") {
      options.dryRun = true;
    } else if (arg === "--help" || arg === "-h") {
      options.help = true;
    } else {
      throw new Error(`unknown option ${arg}\n\n${usage()}`);
    }
  }
  return options;
}

function targetFromEnvironment(options) {
  return options.target ?? process.env.NAPI_ACTORS_TARGET;
}

async function readActorsNapiTargets() {
  const packageJson = JSON.parse(
    await readFile(resolve(root, actorsPackageJson), "utf8"),
  );
  const targets = packageJson.napi?.targets;
  if (
    !Array.isArray(targets) ||
    targets.length === 0 ||
    targets.some((target) => typeof target !== "string")
  ) {
    throw new Error(
      `${actorsPackageJson} must define napi.targets; the package manifest is the authoritative Actors native target matrix`,
    );
  }
  return targets;
}

function assertTarget(target, targets) {
  if (!targets.includes(target)) {
    throw new Error(
      `unsupported Actors N-API target ${JSON.stringify(target)}; expected one of ${targets.join(", ")}`,
    );
  }
}

function commonOptions() {
  return {
    cwd: root,
    manifestPath: actorsManifest,
    packageJsonPath: actorsPackageJson,
    outputDir: actorsOutput,
    npmDir: "typescript/packages/actors/npm",
  };
}

async function build(options) {
  const target = targetFromEnvironment(options);
  if (target === undefined) {
    throw new Error(`build requires --target or NAPI_ACTORS_TARGET\n\n${usage()}`);
  }
  const targets = await readActorsNapiTargets();
  assertTarget(target, targets);
  const build = await new NapiCli().build({
    ...commonOptions(),
    target,
    targetDir: options.targetDir,
    platform: true,
    jsPackageName: "@acyclic-labs/actors",
    // The package is ESM, so the generated CommonJS loader must have a .cjs
    // extension. The package facade can import this file without knowing the
    // host platform, architecture, or Linux libc.
    jsBinding: "binding.cjs",
    dts: "binding.d.ts",
    release: !options.debug,
  });
  await build.task;
}

async function run(options) {
  if (options.command === "build") {
    return build(options);
  }

  if (options.command === "prepare") {
    return new NapiCli().createNpmDirs({
      ...commonOptions(),
      dryRun: options.dryRun,
    });
  }

  if (options.command === "artifacts") {
    return new NapiCli().artifacts({
      ...commonOptions(),
      outputDir: actorsOutput,
    });
  }

  if (options.command === "stage") {
    return new NapiCli().prePublish({
      ...commonOptions(),
      ghRelease: false,
      skipOptionalPublish: true,
      rootPublisher: "npm",
      dryRun: options.dryRun,
    });
  }

  throw new Error(`unknown command ${options.command}\n\n${usage()}`);
}

if (
  process.argv[1] !== undefined &&
  resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    console.log(usage());
  } else {
    await run(options);
  }
}
