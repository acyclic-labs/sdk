import { createHash, randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
import { lstat, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { NapiCli } from "@napi-rs/cli";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const actorsPackageJson = "typescript/packages/actors/package.json";
const actorsManifest = "rust/crates/actors-napi/Cargo.toml";
const actorsOutput = "typescript/packages/actors/generated/native";
const nativeTargetsArtifact = "generated/native-targets.json";
const nativeTargetsSchema = "acyclic.actors.native-targets.v1";
const generationManifestName = "generation-manifest.json";
const generationSchema = "acyclic.sdk.generation.v1";
const rustGenerationWrapper = "scripts/build-with-rust-actors.mjs";

function usage() {
  return `usage:
  node scripts/build-actors-native.mjs build --bundle <generation-bundle>
    --target <rust-triple> [--debug]
  node scripts/build-actors-native.mjs prepare --bundle <generation-bundle> [--dry-run]
  node scripts/build-actors-native.mjs artifacts --bundle <generation-bundle>
  node scripts/build-actors-native.mjs stage --bundle <generation-bundle> [--dry-run]

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
    } else if (arg === "--bundle") {
      options.bundle = rest[++index];
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

async function readActorsNativeConfiguration(options) {
  if (!options.bundle) {
    throw new Error(`native builds require --bundle <generation-bundle>\n\n${usage()}`);
  }
  const packageJson = JSON.parse(await readFile(resolve(root, actorsPackageJson), "utf8"));
  const bundle = resolve(options.bundle);
  const manifestPath = resolve(bundle, generationManifestName);
  const artifactPath = resolve(bundle, nativeTargetsArtifact);
  const [manifest, artifactBytes] = await Promise.all([
    readJson(manifestPath, "generation manifest"),
    readFileChecked(artifactPath),
  ]);
  const artifact = JSON.parse(artifactBytes.toString("utf8"));
  assertGenerationManifest(manifest, packageJson, artifact, artifactBytes, artifactPath);
  if (artifact.schema !== nativeTargetsSchema) {
    throw new Error(`${artifactPath} has unsupported native target schema`);
  }
  if (artifact.package !== "acyclic-actors-napi") {
    throw new Error(`${artifactPath} names ${JSON.stringify(artifact.package)}, expected acyclic-actors-napi`);
  }
  if (artifact.source_path !== actorsManifest) {
    throw new Error(`${artifactPath} is not generated from ${actorsManifest}`);
  }
  if (artifact.version !== packageJson.version) {
    throw new Error(
      `${artifactPath} version ${JSON.stringify(artifact.version)} does not match ${actorsPackageJson} version ${JSON.stringify(packageJson.version)}`,
    );
  }
  const sourceDigest = canonicalSha256(artifact.source_sha256);
  if (!sourceDigest) {
    throw new Error(`${artifactPath} is missing its Rust source digest`);
  }
  if (!/^[0-9a-f]{40,64}$/.test(artifact.source_revision ?? "")) {
    throw new Error(`${artifactPath} is missing its source revision`);
  }
  const targets = artifact.targets;
  if (
    !Array.isArray(targets) ||
    targets.length === 0 ||
    targets.some((target) => typeof target !== "string" || target.length === 0) ||
    new Set(targets).size !== targets.length
  ) {
    throw new Error(
      `${artifactPath} must contain a non-empty unique Rust target list`,
    );
  }
  return { packageJson, artifact, targets, artifactPath, bundle, manifestPath };
}

async function readJson(path, description) {
  try {
    return JSON.parse((await readFile(path)).toString("utf8"));
  } catch (error) {
    throw new Error(`cannot read ${description} at ${path}: ${error.message}`);
  }
}

async function readFileChecked(path) {
  let stat;
  try {
    stat = await lstat(path);
  } catch (error) {
    throw new Error(`generated native target artifact is missing: ${path}: ${error.message}`);
  }
  if (!stat.isFile()) throw new Error(`generated native target artifact is not a regular file: ${path}`);
  return readFile(path);
}

function canonicalSha256(value) {
  if (typeof value !== "string") return undefined;
  if (/^[0-9a-f]{64}$/.test(value)) return value;
  if (/^sha256:[0-9a-f]{64}$/.test(value)) return value.slice("sha256:".length);
  return undefined;
}

function assertGenerationManifest(manifest, packageJson, artifact, artifactBytes, artifactPath) {
  if (manifest?.schema !== generationSchema || manifest.family !== "acyclic_actors") {
    throw new Error("generation bundle is not an Actors SDK bundle");
  }
  if (manifest.version !== packageJson.version) {
    throw new Error(
      `generation manifest version ${JSON.stringify(manifest.version)} does not match ${actorsPackageJson} version ${JSON.stringify(packageJson.version)}`,
    );
  }
  if (typeof manifest.revision !== "string" || manifest.revision !== artifact.source_revision) {
    throw new Error(`generation manifest revision does not match ${nativeTargetsArtifact}`);
  }
  const manifestDigest = canonicalSha256(manifest.source_sha256);
  const artifactDigest = canonicalSha256(artifact.source_sha256);
  if (!manifestDigest || !artifactDigest || manifestDigest !== artifactDigest) {
    throw new Error(`generation manifest source digest does not match ${nativeTargetsArtifact}`);
  }
  const attestedArtifact = manifest.artifacts?.find(entry => entry?.path === nativeTargetsArtifact);
  const actualSha = `sha256:${createHash("sha256").update(artifactBytes).digest("hex")}`;
  if (!attestedArtifact || attestedArtifact.sha256 !== actualSha || attestedArtifact.bytes !== artifactBytes.length) {
    throw new Error(`generation manifest does not attest ${nativeTargetsArtifact} (${artifactPath})`);
  }
}

function assertTarget(target, targets) {
  if (!targets.includes(target)) {
    throw new Error(
      `unsupported Actors N-API target ${JSON.stringify(target)}; expected one of ${targets.join(", ")}`,
    );
  }
}

function commonOptions(packageJsonPath = actorsPackageJson) {
  return {
    cwd: root,
    packageJsonPath,
    npmDir: "typescript/packages/actors/npm",
  };
}

function buildOptions(packageJsonPath) {
  return {
    ...commonOptions(packageJsonPath),
    manifestPath: actorsManifest,
    outputDir: actorsOutput,
  };
}

async function verifyCurrentRustSource(bundle) {
  await new Promise((resolvePromise, reject) => {
    const child = spawn(
      process.execPath,
      [resolve(root, rustGenerationWrapper), "drift", bundle],
      { cwd: root, stdio: "inherit", windowsHide: true },
    );
    child.once("error", reject);
    child.once("close", code => {
      if (code === 0) resolvePromise();
      else reject(new Error(`Rust generation drift check failed with exit code ${code ?? "unknown"}`));
    });
  });
}

async function withGeneratedPackage(options, callback) {
  const configuration = await readActorsNativeConfiguration(options);
  if (!options.dryRun) await verifyCurrentRustSource(configuration.bundle);
  const packagePath = resolve(
    root,
    `typescript/packages/actors/.napi-generated-${process.pid}-${randomUUID()}.json`,
  );
  const packageJson = {
    ...configuration.packageJson,
    napi: {
      ...configuration.packageJson.napi,
      targets: configuration.targets,
    },
  };
  await writeFile(packagePath, `${JSON.stringify(packageJson, null, 2)}\n`, { flag: "wx" });
  try {
    return await callback(configuration, packagePath);
  } finally {
    await rm(packagePath, { force: true });
  }
}

async function build(options) {
  const target = targetFromEnvironment(options);
  if (target === undefined) {
    throw new Error(`build requires --target or NAPI_ACTORS_TARGET\n\n${usage()}`);
  }
  const configuration = await readActorsNativeConfiguration(options);
  assertTarget(target, configuration.targets);
  await withGeneratedPackage(options, async ({ targets }, packageJsonPath) => {
    assertTarget(target, targets);
    const build = await new NapiCli().build({
      ...buildOptions(packageJsonPath),
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
  });
}

async function run(options) {
  if (options.command === "build") {
    return build(options);
  }

  if (options.command === "prepare") {
    return withGeneratedPackage(options, (_, packageJsonPath) => new NapiCli().createNpmDirs({
      ...commonOptions(packageJsonPath),
      dryRun: options.dryRun,
    }));
  }

  if (options.command === "artifacts") {
    return withGeneratedPackage(options, (_, packageJsonPath) => new NapiCli().artifacts({
      ...commonOptions(packageJsonPath),
      outputDir: actorsOutput,
    }));
  }

  if (options.command === "stage") {
    return withGeneratedPackage(options, (_, packageJsonPath) => new NapiCli().prePublish({
      ...commonOptions(packageJsonPath),
      ghRelease: false,
      skipOptionalPublish: true,
      rootPublisher: "npm",
      dryRun: options.dryRun,
    }));
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
