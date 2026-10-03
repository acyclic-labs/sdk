import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { loadConfig } from "./graphcoder-qualification-suite.mjs";

const digest = value => createHash("sha256").update(value).digest("hex");

test("suite configuration binds declared artifact digests to current bytes", () => {
  const directory = mkdtempSync(join(tmpdir(), "graphcoder-suite-config-"));
  try {
    const artifactPath = join(directory, "graphcoder.tgz");
    const configPath = join(directory, "suite.json");
    const bytes = "package bytes\n";
    writeFileSync(artifactPath, bytes);
    const config = {
      id: "package-smoke",
      execution_kind: "package",
      platform: "windows-x86_64",
      command: { executable: process.execPath, args: ["-e", ""], cwd: process.cwd(), env: {} },
      artifacts: [{
        path: artifactPath,
        sha256: digest(bytes),
        source_commit: "a".repeat(40),
        source_tree: "b".repeat(40),
        built_at: "2026-10-03T00:00:00.000Z",
        build_id: "package-smoke-build",
        fresh: true,
      }],
    };
    writeFileSync(configPath, JSON.stringify(config));
    assert.equal(loadConfig(configPath).artifacts[0].sha256, digest(bytes));
    config.artifacts[0].sha256 = "0".repeat(64);
    writeFileSync(configPath, JSON.stringify(config));
    assert.throws(() => loadConfig(configPath), /digest does not match its bytes/);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
