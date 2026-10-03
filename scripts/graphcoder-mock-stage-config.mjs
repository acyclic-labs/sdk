#!/usr/bin/env node

// Create the source-bound installed mock fixture suite configuration. The
// package is loaded from the installed package root and the JSON-lines bridge
// is loaded from that same package. This lane exercises package exports and
// process framing while remaining explicitly separate from native Harness
// and PTY qualification.

import { existsSync, lstatSync, mkdirSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describeArtifact } from "./graphcoder-artifact.mjs";

const FIXTURES = new Set(["deterministic"]);

function fail(message) {
  throw new Error(`graphcoder-mock-stage-config: ${message}`);
}

function required(value, label) {
  if (typeof value !== "string" || value.trim() === "") fail(`${label} is required`);
  return value;
}

function sharedProvenance(artifacts) {
  const first = artifacts[0];
  if (artifacts.some(item => item.source_commit !== first.source_commit || item.source_tree !== first.source_tree)) {
    fail("package, bridge, and entrypoint artifacts do not share one source commit/tree");
  }
}

export function makeMockStageConfig({
  sourceCwd,
  packageArchive,
  packageRoot,
  output,
  buildId,
  builtAt,
  fixture = "deterministic",
  commands,
  gitOps,
}) {
  const source = resolve(required(sourceCwd, "sourceCwd"));
  const archive = resolve(required(packageArchive, "packageArchive"));
  const installedPackage = resolve(required(packageRoot, "packageRoot"));
  const outputPath = resolve(required(output, "output"));
  const id = required(buildId, "buildId");
  if (!existsSync(source)) fail(`sourceCwd does not exist: ${source}`);
  if (!existsSync(installedPackage)) fail(`packageRoot does not exist: ${installedPackage}`);
  const packageJson = lstatSync(resolve(installedPackage, "package.json"), { throwIfNoEntry: false });
  if (!packageJson?.isFile() || packageJson.isSymbolicLink()) fail(`packageRoot must contain a regular package.json: ${installedPackage}`);
  if (!FIXTURES.has(fixture)) fail(`unsupported fixture: ${fixture}`);
  const suiteCommands = commands ?? [
    "start inspect the repository",
    "list",
    "activity",
    "messages",
    "approvals",
    "approve {{approval_id}} yes",
    "changes",
    "diff README.md",
    "file README.md",
    "writeback {{writeback_operation_id}} {{workspace_generation}} yes",
    "cancel",
    "resume {{session_id}}",
    "cancel",
  ];
  if (!Array.isArray(suiteCommands) || suiteCommands.length === 0 || suiteCommands.some(item => typeof item !== "string" || item.trim() === "")) {
    fail("commands must be a nonempty string array");
  }
  if (suiteCommands.some(item => item.startsWith("--fixture="))) {
    fail("mock fixture selection belongs in the explicit bridge environment, not a production command");
  }

  const bridge = fileURLToPath(new URL("./graphcoder-mock-bridge.mjs", import.meta.url));
  const entrypoint = fileURLToPath(new URL("./graphcoder-production-entrypoint.mjs", import.meta.url));
  const packageContract = fileURLToPath(new URL("./fixtures/graphcoder-qualification/package-contract.mjs", import.meta.url));
  const packageArtifact = describeArtifact({ path: archive, sourceCwd: source, buildId: `${id}-package`, builtAt, gitOps });
  const bridgeArtifact = describeArtifact({ path: bridge, sourceCwd: source, buildId: `${id}-bridge`, builtAt, gitOps });
  const entrypointArtifact = describeArtifact({ path: entrypoint, sourceCwd: source, buildId: `${id}-entrypoint`, builtAt, gitOps });
  const packageContractArtifact = describeArtifact({ path: packageContract, sourceCwd: source, buildId: `${id}-package-contract`, builtAt, gitOps });
  const artifacts = [packageArtifact, bridgeArtifact, entrypointArtifact, packageContractArtifact];
  sharedProvenance(artifacts);

  return {
    id: `installed-mock-${id}`,
    descriptor: "docs/graphcoder-swarm/graphcoder-real-backend-scenarios.json",
    execution_kind: "package",
    platform: "windows-x86_64",
    command: {
      executable: process.execPath,
      args: [entrypoint, ...suiteCommands],
      cwd: source,
      env: {
        GRAPHCODER_PACKAGE_ROOT: installedPackage,
        GRAPHCODER_BRIDGE_EXECUTABLE: process.execPath,
        GRAPHCODER_BRIDGE_ARGS_JSON: JSON.stringify([bridge]),
        GRAPHCODER_BRIDGE_ENV_JSON: JSON.stringify({
          GRAPHCODER_PACKAGE_ROOT: installedPackage,
          GRAPHCODER_MOCK_FIXTURE: fixture,
        }),
        GRAPHCODER_BRIDGE_CWD: source,
        GRAPHCODER_PACKAGE_ARTIFACT: archive,
        GRAPHCODER_IDENTITY_PATH: `${outputPath}.package-identity.json`,
        GRAPHCODER_REQUIRE_PACKAGE_IDENTITY: "1",
      },
    },
    artifacts,
    expected_exit_code: 0,
    output: outputPath,
  };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const [, , sourceCwd, packageArchive, packageRoot, output, buildId, builtAt, fixture] = process.argv;
    if (process.argv.length < 8 || process.argv.length > 10) {
      fail("usage: graphcoder-mock-stage-config.mjs SOURCE_CWD PACKAGE_ARCHIVE PACKAGE_ROOT OUTPUT BUILD_ID [BUILT_AT] [FIXTURE]");
    }
    const config = makeMockStageConfig({ sourceCwd, packageArchive, packageRoot, output, buildId, builtAt, fixture });
    mkdirSync(dirname(resolve(output)), { recursive: true });
    writeFileSync(resolve(output), `${JSON.stringify(config, null, 2)}\n`, { flag: "wx" });
    process.stdout.write(`${JSON.stringify({ config: resolve(output), source_commit: config.artifacts[0].source_commit, source_tree: config.artifacts[0].source_tree }, null, 2)}\n`);
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}
