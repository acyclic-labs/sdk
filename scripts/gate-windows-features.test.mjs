import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { chmodSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

test("Windows gate preserves affected scopes and propagates cross-check failures", { skip: process.platform === "win32" }, () => {
  const cwd = mkdtempSync(join(tmpdir(), "windows-feature-scope-"));
  try {
    mkdirSync(join(cwd, "bin"));
    const python = join(cwd, "bin/python3");
    writeFileSync(python, '#!/bin/bash\nprintf "%s\\n" "$@" > args\nprintf "%s\\n" "$GATE_FEATURE_TARGET_DIR" > target\nexit "$WINDOWS_RC"\n');
    chmodSync(python, 0o755);
    const script = join(dirname(fileURLToPath(import.meta.url)), "gate-windows-features.sh");
    const invoke = (mode, status = 0) => spawnSync("bash", [script, mode], {
      cwd, encoding: "utf8", env: { ...process.env, PATH: `${join(cwd, "bin")}:${process.env.PATH}`, GATE_WINDOWS_FEATURE_TARGET_DIR: "", GATE_FEATURE_TARGET_DIR: join(cwd, "job-target"), WINDOWS_RC: String(status) },
    });
    writeFileSync(join(cwd, ".gate-tests-packages"), "acyclic-fs\r\nacyclic-native-runtime");
    writeFileSync(join(cwd, ".gate-clippy-packages"), "acyclic-stream");
    for (const status of [0, 101]) {
      const result = invoke("affected", status);
      assert.equal(result.status, status, result.stdout + result.stderr);
      const args = readFileSync(join(cwd, "args"), "utf8").trim().split("\n");
      assert.deepEqual(args.slice(-6), ["bash", "scripts/gate-feature-matrix.sh", "--windows-msvc", "--package=acyclic-fs", "--package=acyclic-native-runtime", "--package=acyclic-stream"]);
      assert.match(result.stdout, new RegExp(`GATE_WINDOWS_FEATURES rc=${status}`));
      assert.equal(readFileSync(join(cwd, "target"), "utf8").trim(), join(cwd, "job-target"));
    }
    assert.equal(invoke("full").status, 0);
    assert.ok(!readFileSync(join(cwd, "args"), "utf8").includes("--package="));
    writeFileSync(join(cwd, ".gate-tests-packages"), "");
    writeFileSync(join(cwd, ".gate-clippy-packages"), "");
    assert.equal(invoke("affected").status, 2);
    assert.equal(invoke("invalid").status, 2);
  } finally {
    rmSync(cwd, { recursive: true, force: true });
  }
});
