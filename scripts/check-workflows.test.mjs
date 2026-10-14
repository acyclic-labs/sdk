import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { chmodSync, mkdirSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { join, resolve, sep } from "node:path";
import test from "node:test";

test("workflow validation requires ShellCheck, forwards scoped/default files and propagates lint errors", () => {
  mkdirSync(".tools", { recursive: true });
  const root = mkdtempSync(".tools/workflow-check-").replaceAll("\\", "/");
  const bash = process.platform === "win32" ? join(process.env.ProgramFiles ?? "C:/Program Files", "Git", "bin", "bash.exe") : "/bin/bash";
  const binary = `${root}/actionlint`;
  const checker = `${root}/bin/shellcheck`;
  mkdirSync(`${root}/bin`);
  writeFileSync(binary, '#!/bin/sh\nprintf "%s\\n" "$@"\nexit "${FIXTURE_STATUS:-0}"\n');
  chmodSync(binary, 0o755);
  const run = (files, status = 0) => spawnSync(bash, ["scripts/check-workflows.sh", binary, ...files], {
    env: { ...process.env, PATH: resolve(`${root}/bin`), FIXTURE_STATUS: String(status) }, encoding: "utf8", timeout: 60_000,
  });
  try {
    const missing = run([]);
    assert.ifError(missing.error);
    assert.equal(missing.status, 1);
    assert.match(missing.stderr, /shellcheck is required/);
    assert.equal(missing.stdout, "");
    writeFileSync(checker, '#!/bin/sh\nexit 0\n');
    chmodSync(checker, 0o755);
    for (const files of [[], [".github/workflows/release-acyclic.yml"]]) {
      for (const status of [0, 7]) {
        const result = run(files, status);
        assert.ifError(result.error);
        assert.equal(result.status, status, result.stderr);
        const args = result.stdout.trim().split(/\r?\n/);
        assert.equal(args[0], "-shellcheck");
        assert.ok(args[1].replaceAll("\\", "/").endsWith(`${root}/bin/shellcheck`), args[1]);
        assert.deepEqual(args.slice(2), files.length ? files : readdirSync(".github/workflows").filter(name => name.endsWith(".yml")).sort().map(name => `.github/workflows/${name}`));
      }
    }
    const disabled = run(["-shellcheck="]);
    assert.equal(disabled.status, 1);
    assert.equal(disabled.stdout, "");
  } finally {
    assert.ok(resolve(root).startsWith(resolve(".tools") + sep));
    rmSync(root, { recursive: true, force: true });
  }
});
