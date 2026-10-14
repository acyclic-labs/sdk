import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join, resolve, sep } from "node:path";
import test from "node:test";

test("Harness packaging stages the qualified bytes and rejects missing WASM before staging", () => {
  const source = readFileSync("scripts/check-harness-package.sh", "utf8");
  assert.doesNotMatch(source, /build-wasm\.mjs|bun x tsc|bun scripts\/check-metadata\.mjs/);
  const lane = readFileSync("scripts/qualify-ci.sh", "utf8").split("  linux)\n")[1]?.split("  linux-musl|")[0] ?? assert.fail("missing Linux lane");
  const workspaceTests = lane.indexOf("bun run test");
  const packageCheck = lane.indexOf("bash scripts/check-harness-package.sh");
  assert.ok(workspaceTests >= 0 && packageCheck > workspaceTests, "packaging requires preceding workspace qualification");
  assert.ok(lane.includes("bun scripts/check-metadata.mjs"));
  const start = source.indexOf("for generated in acyclic_harness_wasm.js");
  const end = source.indexOf('cd "$npm_stage"', start);
  assert.ok(start >= 0 && end > start);
  const command = source.slice(start, end);
  mkdirSync(".tools", { recursive: true });
  const root = resolve(mkdtempSync(".tools/harness-built-")).replaceAll("\\", "/");
  const packageRoot = join(root, "typescript/packages/harness");
  const wasm = join(packageRoot, "generated/wasm").replaceAll("\\", "/");
  const bash = process.platform === "win32" ? join(process.env.ProgramFiles ?? "C:/Program Files", "Git", "bin", "bash.exe") : "bash";
  try {
    for (const directory of [wasm, join(packageRoot, "generated/proto"), join(packageRoot, "dist")]) mkdirSync(directory, { recursive: true });
    for (const name of ["package.json", "README.md", "CHANGELOG.md", "dist/index.js", "generated/proto/fixture.js",
      ...["acyclic_harness_wasm.js", "acyclic_harness_wasm.d.ts", "acyclic_harness_wasm_bg.wasm", "acyclic_harness_wasm_bg.wasm.d.ts"].map(name => `generated/wasm/${name}`)]) {
      writeFileSync(join(packageRoot, name), Buffer.from([0, 1, 2, 3]));
    }
    for (const missing of [false, true]) {
      const work = join(root, missing ? "missing" : "complete").replaceAll("\\", "/");
      mkdirSync(work);
      if (missing) rmSync(join(wasm, "acyclic_harness_wasm_bg.wasm"));
      const result = spawnSync(bash, ["-c", `set -euo pipefail\nwork='${work}'\nwasm_output='${wasm}'\n${command}`], {
        cwd: root, encoding: "utf8", timeout: 60_000,
      });
      assert.ifError(result.error);
      assert.equal(result.status, missing ? 1 : 0, result.stderr);
      const staged = join(work, "npm-package");
      if (missing) {
        assert.match(result.stderr, /missing generated Harness artifact: acyclic_harness_wasm_bg\.wasm/);
        assert.equal(existsSync(staged), false);
      } else {
        for (const name of ["dist/index.js", "generated/proto/fixture.js", "generated/wasm/acyclic_harness_wasm_bg.wasm"])
          assert.deepEqual(readFileSync(join(staged, name)), readFileSync(join(packageRoot, name)));
      }
    }
  } finally {
    assert.ok(resolve(root).startsWith(resolve(".tools") + sep));
    rmSync(root, { recursive: true, force: true });
  }
});

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
