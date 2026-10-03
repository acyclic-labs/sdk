import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { makeMockStageConfig } from "./graphcoder-mock-stage-config.mjs";

test("installed mock stage binds package and driver source artifacts", () => {
  const directory = mkdtempSync(join(tmpdir(), "graphcoder-mock-config-"));
  try {
    const packageArchive = join(directory, "graphcoder.tgz");
    writeFileSync(packageArchive, "package bytes\n");
    writeFileSync(join(directory, "package.json"), "{}\n");
    const config = makeMockStageConfig({
      sourceCwd: process.cwd(),
      packageArchive,
      packageRoot: directory,
      output: join(directory, "suites"),
      buildId: "build-1",
      builtAt: "2026-10-03T10:00:00.000Z",
      gitOps: { git: (_cwd, ...args) => args.at(-1) === "HEAD" ? "a".repeat(40) : "b".repeat(40) },
    });
    assert.equal(config.execution_kind, "package");
    assert.equal(config.artifacts.length, 4);
    assert.deepEqual(new Set(config.artifacts.map(item => item.build_id)), new Set(["build-1-package", "build-1-bridge", "build-1-entrypoint", "build-1-package-contract"]));
    assert.ok(config.artifacts.every(item => item.source_commit === "a".repeat(40) && item.source_tree === "b".repeat(40)));
    assert.equal(JSON.parse(config.command.env.GRAPHCODER_BRIDGE_ENV_JSON).GRAPHCODER_MOCK_FIXTURE, "deterministic");
    assert.equal(config.command.args.some(item => item.startsWith("--fixture=")), false);
    assert.equal(config.command.env.GRAPHCODER_PACKAGE_ROOT, directory);
    assert.equal(config.command.env.GRAPHCODER_PACKAGE_ARTIFACT, packageArchive);
    assert.equal(config.command.env.GRAPHCODER_REQUIRE_PACKAGE_IDENTITY, "1");
    assert.match(config.command.env.GRAPHCODER_IDENTITY_PATH, /\.package-identity\.json$/u);
    assert.equal(config.command.args[0].endsWith("graphcoder-production-entrypoint.mjs"), true);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("installed mock stage rejects unsupported fixtures and fixture flags", () => {
  const directory = mkdtempSync(join(tmpdir(), "graphcoder-mock-config-"));
  try {
    const packageArchive = join(directory, "graphcoder.tgz");
    writeFileSync(packageArchive, "package bytes\n");
    writeFileSync(join(directory, "package.json"), "{}\n");
    const base = {
      sourceCwd: process.cwd(), packageArchive, packageRoot: directory,
      output: join(directory, "suites"), buildId: "build-1",
      gitOps: { git: (_cwd, ...args) => args.at(-1) === "HEAD" ? "a".repeat(40) : "b".repeat(40) },
    };
    assert.throws(() => makeMockStageConfig({ ...base, fixture: "unknown" }), /unsupported fixture/);
    assert.throws(() => makeMockStageConfig({ ...base, commands: ["--fixture=deterministic"] }), /fixture selection/);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
