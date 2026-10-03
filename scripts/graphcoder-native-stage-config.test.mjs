import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { makeNativeStageConfig } from "./graphcoder-native-stage-config.mjs";

test("native stage config binds package and runtime to one source provenance", () => {
  const directory = mkdtempSync(join(tmpdir(), "graphcoder-native-config-"));
  try {
    const packageArchive = join(directory, "graphcoder.tgz");
    const runtime = join(directory, "graphcoder-runtime.exe");
    writeFileSync(packageArchive, "package bytes\n");
    writeFileSync(runtime, "runtime bytes\n");
    const gitOps = { git: (_cwd, ...args) => args.at(-1) === "HEAD" ? "a".repeat(40) : "b".repeat(40) };
    const config = makeNativeStageConfig({
      sourceCwd: directory,
      packageArchive,
      runtime,
      packageRoot: directory,
      output: join(directory, "suites"),
      buildId: "build-1",
      builtAt: "2026-10-03T10:00:00.000Z",
      gitOps,
    });
    assert.equal(config.execution_kind, "native");
    assert.equal(config.artifacts.length, 4);
    assert.ok(config.artifacts.every(item => item.source_commit === config.artifacts[0].source_commit));
    assert.ok(config.artifacts.every(item => item.source_tree === config.artifacts[0].source_tree));
    assert.equal(config.command.env.GRAPHCODER_PACKAGE_ROOT, directory);
    assert.equal(config.command.env.GRAPHCODER_PACKAGE_ARTIFACT, packageArchive);
    assert.equal(config.command.env.GRAPHCODER_REQUIRE_LAZY_COUNTERS, "1");
    assert.equal(config.command.env.GRAPHCODER_LAZY_OBSERVATION_PATH, `${join(directory, "suites")}.lazy-observation.json`);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("native stage config refuses package and runtime provenance drift", () => {
  const directory = mkdtempSync(join(tmpdir(), "graphcoder-native-config-"));
  try {
    const packageArchive = join(directory, "graphcoder.tgz");
    const runtime = join(directory, "graphcoder-runtime.exe");
    writeFileSync(packageArchive, "package bytes\n");
    writeFileSync(runtime, "runtime bytes\n");
    let call = 0;
    const gitOps = { git: (_cwd, ...args) => args.at(-1) === "HEAD" ? "a".repeat(40) : `${"b".repeat(39)}${call++ % 2}` };
    assert.throws(() => makeNativeStageConfig({
      sourceCwd: directory,
      packageArchive,
      runtime,
      packageRoot: directory,
      output: join(directory, "suites"),
      buildId: "build-1",
      gitOps,
    }), /one source commit\/tree/);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
