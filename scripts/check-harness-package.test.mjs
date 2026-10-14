import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { join, resolve, sep } from "node:path";
import test from "node:test";

test("packaged Harness tests retain their inventory, logs and failure status at the lane's worker budget", () => {
  const source = readFileSync("scripts/check-harness-package.sh", "utf8");
  const start = source.indexOf('export RUST_MIN_STACK=');
  const end = source.indexOf('"$cargo_bin" run --manifest-path', start);
  assert.ok(start >= 0 && end > start);
  const command = source.slice(start, end);
  assert.ok(command.includes('--all-features --offline'));
  assert.doesNotMatch(command, /--skip|--exclude|--ignored|--no-run|--lib|--tests/);
  mkdirSync(".tools", { recursive: true });
  const root = mkdtempSync(".tools/harness-workers-").replaceAll("\\", "/");
  const bash = process.platform === "win32" ? join(process.env.ProgramFiles ?? "C:/Program Files", "Git", "bin", "bash.exe") : "bash";
  /** @type {Array<[string | undefined, number]>} */
  const cases = [[undefined, 0], ["16", 0], ["16", 7]];
  try {
    for (const [workers, status] of cases) {
      const env = { ...process.env };
      delete env.RUST_TEST_THREADS;
      if (workers) env.RUST_TEST_THREADS = workers;
      const result = spawnSync(bash, ["-c", `set -euo pipefail
work='${root}'
harness_version=0.2.0
cargo_bin=cargo
cargo() {
  [[ "$*" == 'test --manifest-path acyclic-harness-0.2.0/Cargo.toml --all-features --offline -- --test-threads=${workers ?? "1"}' ]]
  [[ "$RUST_MIN_STACK" -ge 8388608 ]]
  printf 'test fixture ... ok\\n'
  return ${status}
}
${command}
printf 'completed\\n'
`], { env, encoding: "utf8", timeout: 60_000 });
      assert.ifError(result.error);
      assert.equal(result.status, status, result.stderr);
      assert.equal(result.stdout.includes("completed"), status === 0);
      assert.equal(readFileSync(`${root}/rust-package-test.log`, "utf8"), "test fixture ... ok\n");
    }
  } finally {
    assert.ok(resolve(root).startsWith(resolve(".tools") + sep));
    rmSync(root, { recursive: true, force: true });
  }
});
