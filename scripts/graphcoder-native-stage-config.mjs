#!/usr/bin/env node

// Create the source-bound native stage suite configuration after a fresh
// package and native runtime build. This records provenance; it does not build
// artifacts or launch the qualified runtime.

import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describeArtifact } from "./graphcoder-artifact.mjs";

function fail(message) {
  throw new Error(`graphcoder-native-stage-config: ${message}`);
}

function required(value, label) {
  if (typeof value !== "string" || value.trim() === "") fail(`${label} is required`);
  return value;
}

function validateWindowsExecutable(path) {
  const verifier = fileURLToPath(new URL("./verify-release-binary.mjs", import.meta.url));
  const result = spawnSync(process.execPath, [verifier, "win32-x64", path], {
    encoding: "utf8",
    shell: false,
    windowsHide: true,
    stdio: ["ignore", "pipe", "pipe"],
  });
  if (result.error) fail(`runtime PE validation failed: ${result.error.message}`);
  if (result.status !== 0) {
    const detail = String(result.stderr ?? result.stdout ?? "").trim();
    fail(`runtime is not a valid win32-x64 PE executable${detail === "" ? "" : `: ${detail}`}`);
  }
}

export function makeNativeStageConfig({ sourceCwd, packageArchive, runtime, packageRoot, output, buildId, builtAt, gitOps }) {
  const source = resolve(required(sourceCwd, "sourceCwd"));
  const archive = resolve(required(packageArchive, "packageArchive"));
  const executable = resolve(required(runtime, "runtime"));
  const installedPackage = resolve(required(packageRoot, "packageRoot"));
  const outputPath = resolve(required(output, "output"));
  if (!existsSync(source)) fail(`sourceCwd does not exist: ${source}`);
  if (!existsSync(executable)) fail(`runtime does not exist: ${executable}`);
  if (!existsSync(installedPackage)) fail(`packageRoot does not exist: ${installedPackage}`);
  validateWindowsExecutable(executable);
  const id = required(buildId, "buildId");
  const packageArtifact = describeArtifact({ path: archive, sourceCwd: source, buildId: `${id}-package`, builtAt, gitOps });
  const nativeArtifact = describeArtifact({ path: executable, sourceCwd: source, buildId: `${id}-native`, builtAt, gitOps });
  if (packageArtifact.source_commit !== nativeArtifact.source_commit || packageArtifact.source_tree !== nativeArtifact.source_tree) {
    fail("package and native artifacts do not share one source commit/tree");
  }
  const driver = fileURLToPath(new URL("./graphcoder-native-stage-e2e.mjs", import.meta.url));
  const packageContract = fileURLToPath(new URL("./fixtures/graphcoder-qualification/package-contract.mjs", import.meta.url));
  const suiteDescriptor = fileURLToPath(new URL("../docs/graphcoder-swarm/graphcoder-native-scenarios.json", import.meta.url));
  const driverArtifact = describeArtifact({ path: driver, sourceCwd: source, buildId: `${id}-driver`, builtAt, gitOps });
  const packageContractArtifact = describeArtifact({ path: packageContract, sourceCwd: source, buildId: `${id}-package-contract`, builtAt, gitOps });
  const suiteDescriptorArtifact = describeArtifact({ path: suiteDescriptor, sourceCwd: source, buildId: `${id}-suite-descriptor`, builtAt, gitOps });
  return {
    id: `native-stage-${id}`,
    descriptor: "docs/graphcoder-swarm/graphcoder-native-scenarios.json",
    execution_kind: "native",
    platform: "windows-x86_64",
    command: {
      executable: process.execPath,
      args: [driver],
      cwd: source,
      env: {
        GRAPHCODER_NATIVE_RUNTIME: executable,
        GRAPHCODER_PACKAGE_ROOT: installedPackage,
        GRAPHCODER_PACKAGE_ARTIFACT: archive,
        GRAPHCODER_LAZY_OBSERVATION_PATH: `${outputPath}.lazy-observation.json`,
        GRAPHCODER_REQUIRE_LAZY_COUNTERS: "1",
      },
    },
    artifacts: [nativeArtifact, packageArtifact, driverArtifact, packageContractArtifact, suiteDescriptorArtifact],
    expected_exit_code: 0,
    output: outputPath,
  };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const [, , sourceCwd, packageArchive, runtime, packageRoot, output, buildId, builtAt] = process.argv;
    if (process.argv.length < 8 || process.argv.length > 9) fail("usage: graphcoder-native-stage-config.mjs SOURCE_CWD PACKAGE_ARCHIVE RUNTIME PACKAGE_ROOT OUTPUT BUILD_ID [BUILT_AT]");
    const config = makeNativeStageConfig({ sourceCwd, packageArchive, runtime, packageRoot, output, buildId, builtAt });
    mkdirSync(dirname(resolve(output)), { recursive: true });
    writeFileSync(resolve(output), `${JSON.stringify(config, null, 2)}\n`, { flag: "wx" });
    process.stdout.write(`${JSON.stringify({ config: resolve(output), source_commit: config.artifacts[0].source_commit, source_tree: config.artifacts[0].source_tree }, null, 2)}\n`);
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}
