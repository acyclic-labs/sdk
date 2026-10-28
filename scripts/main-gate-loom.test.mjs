import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { chmodSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

const scripts = dirname(fileURLToPath(import.meta.url));

test("main gate executes complete harness Loom qualifications and propagates each failure", () => {
  const cwd = mkdtempSync(join(tmpdir(), "main-gate-loom-"));
  try {
    mkdirSync(join(cwd, "bin"));
    mkdirSync(join(cwd, "scripts"));
    for (const name of ["check-harness-models.sh"]) {
      writeFileSync(join(cwd, "scripts", name), readFileSync(join(scripts, name)));
    }
    mkdirSync(join(cwd, "scripts/external-gate"), { recursive: true });
    writeFileSync(join(cwd, "scripts/external-gate/gate-clippy.py"),
      readFileSync(process.env.SDK_GATE_CLIPPY || "/home/var/bin/gate-clippy.py"));
    writeFileSync(join(cwd, "bin/cargo"), `#!/bin/bash
if [ "$1" = metadata ]; then
  echo '{"workspace_members":["a"],"packages":[{"id":"a","name":"harness"}]}'
fi
exit 0
`);
    chmodSync(join(cwd, "bin/cargo"), 0o755);
    for (const name of ["with-owner-keychain.sh", "gate-test-runner.sh", "gate-rust-format.sh", "check-browser-targets.sh", "gate-feature-matrix.sh", "gate-windows-features.sh"]) {
      writeFileSync(join(cwd, "scripts", name), "#!/bin/bash\nexit 0\n");
    }
    writeFileSync(join(cwd, "scripts/qualify-mcp-loom.mjs"), `
import { appendFileSync } from "node:fs";
appendFileSync("loom-calls", JSON.stringify({ args: process.argv.slice(2), target: process.env.CARGO_TARGET_DIR }) + "\\n");
if (process.argv[2] === process.env.FAIL_MODEL) {
  console.error("error: Loom compile or execution failed");
  process.exit(101);
}
`);
    const flags = ["--harness-models"];
    for (const fail of ["", ...flags]) {
      writeFileSync(join(cwd, "loom-calls"), "");
      const result = spawnSync("bash", [process.env.MAIN_GATE_LOOM_TEST_RUNNER || join(scripts, "main-gate-checks.sh")], {
        cwd, encoding: "utf8", env: { ...process.env, SDK_GATE_CLIPPY: join(cwd,"scripts/external-gate/gate-clippy.py"), SDK_WINDOWS_GATE: join(cwd,"scripts/external-gate/gate-checks.sh"), PATH: `${join(cwd, "bin")}:${process.env.PATH}`, GATE_MAIN_TARGET_DIR: join(cwd, "cache"), FAIL_MODEL: fail },
      });
      assert.equal(result.status, fail ? 101 : 0, result.stdout + result.stderr);
      const calls = readFileSync(join(cwd, "loom-calls"), "utf8").trim().split("\n").filter(Boolean).map(JSON.parse);
      assert.deepEqual(calls, flags.slice(0, fail ? flags.indexOf(fail) + 1 : flags.length).map(flag => ({
        args: [flag, "--all-features"], target: join(cwd, "cache/loom"),
      })), "every required model runs once; no retry or ignored failure");
      assert.match(result.stdout, new RegExp(`GATE_HARNESS_LOOM_WALL seconds=\\d+ rc=${fail ? 101 : 0}`));
      assert.match(result.stdout, new RegExp(`GATE_DONE rc=${fail ? 101 : 0}`));
      if (fail) assert.match(result.stdout, /error: Loom compile or execution failed/);
    }
  } finally {
    rmSync(cwd, { recursive: true, force: true });
  }
});
