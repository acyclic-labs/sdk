import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join, resolve, sep } from "node:path";
import test from "node:test";
import YAML from "./vendor/yaml.cjs";

const workflow = YAML.parse(readFileSync(".github/workflows/release-acyclic.yml", "utf8"));

test("native compiler caches refresh each run while restoring the previous working set", () => {
  const action = YAML.parse(readFileSync(".github/actions/rust-build-cache/action.yml", "utf8"));
  const compiler = action.runs.steps.find(step => step.name === "Restore compiler output");
  assert.equal(compiler.uses, "actions/cache@caa296126883cff596d87d8935842f9db880ef25");
  assert.equal(compiler.with.path, "${{ runner.temp }}/sccache");
  const [matchingInputs, matchingTarget] = compiler.with["restore-keys"].trim().split("\n");
  assert.equal(compiler.with.key, matchingInputs + "${{ github.run_id }}-${{ github.run_attempt }}");
  assert.equal(matchingInputs, matchingTarget + "${{ hashFiles('rust-toolchain.toml', 'Cargo.lock', '**/Cargo.toml') }}-");
  assert.equal(matchingTarget, "rust-objects-v2-${{ inputs.scope }}-${{ runner.os }}-${{ runner.arch }}-");
  // Each successful run can save new source objects instead of returning an
  // immutable exact hit before its updated working set is uploaded.
  const key = (run, attempt) => compiler.with.key.replace("${{ github.run_id }}", run).replace("${{ github.run_attempt }}", attempt);
  assert.notEqual(key("100", "1"), key("101", "1"));
  assert.notEqual(key("100", "1"), key("100", "2"));
  for (const value of [key("100", "1"), key("101", "1"), key("100", "2")]) {
    assert.ok(value.startsWith(matchingInputs));
  }
  const downloads = action.runs.steps.find(step => step.name === "Restore Cargo downloads");
  assert.equal(downloads.with.key, "rust-deps-v2-${{ runner.os }}-${{ runner.arch }}-${{ hashFiles('rust-toolchain.toml', 'Cargo.lock') }}");
  assert.doesNotMatch(downloads.with.key, /run_id|run_attempt/);
});

test("native cache snapshot runs before leaving the build step and preserves its result", () => {
  const build = workflow.jobs.build.steps.find(step => step.name === "Build the unified executable");
  const start = build.run.indexOf("trap '\n");
  const end = build.run.indexOf("\n' EXIT", start);
  assert.ok(start >= 0 && end > start);
  const trap = build.run.slice(start, end + "\n' EXIT".length) + "\n";
  assert.ok(end < build.run.indexOf("node scripts/build-product.mjs"));
  assert.ok(build.run.includes('wait "${pids[$index]}"'));
  mkdirSync(".tools", { recursive: true });
  const root = mkdtempSync(".tools/native-cache-").replaceAll("\\", "/");
  const bash = process.platform === "win32" ? join(process.env.ProgramFiles ?? "C:/Program Files", "Git", "bin", "bash.exe") : "bash";
  try {
    for (const [buildStatus, snapshotStatus] of [[0, 0], [7, 0], [0, 9], [7, 9]]) {
      const finish = Math.floor(Date.now() / 1000);
      writeFileSync(`${root}/summary.md`, "");
      const result = spawnSync(bash, ["-c", `set -euo pipefail
RUNNER_TEMP='${root}'
export GITHUB_STEP_SUMMARY='${root}/summary.md'
CARGO_BUILD_TARGET=fixture-target
build_started=${finish - 12}
date() { printf '${finish}\\n'; }
sccache() {
  [[ "$*" == '--show-stats --stats-format=json' ]]
  printf '{"stats":{"compile_requests":5,"cache_hits":{"counts":{"Rust":2}},"cache_misses":{"counts":{"Rust":3}},"requests_not_cacheable":0}}\\n'
  return ${snapshotStatus}
}
${trap}
exit ${buildStatus}
`], { encoding: "utf8", timeout: 60_000 });
      assert.ifError(result.error);
      assert.equal(result.status, buildStatus, result.stderr);
      assert.equal(JSON.parse(readFileSync(`${root}/native-build-timings/sccache.json`, "utf8")).stats.compile_requests, 5);
      for (const output of [result.stdout, readFileSync(`${root}/summary.md`, "utf8")]) {
        assert.ok(output.includes("## fixture-target native build"));
        assert.ok(output.includes("| `Native builds` | 12s |"));
        assert.ok(output.includes("| Compile requests | 5 |"));
        assert.ok(output.includes("| Cache hits | 2 |"));
        assert.ok(output.includes("| Cache misses | 3 |"));
      }
    }
  } finally {
    assert.ok(resolve(root).startsWith(resolve(".tools") + sep));
    rmSync(root, { recursive: true, force: true });
  }
});

test("every native target retains its live cache evidence from the current run and attempt", () => {
  const steps = workflow.jobs.build.steps;
  const index = steps.findIndex(step => step.name === "Retain native compiler-cache evidence");
  const step = steps[index];
  assert.equal(index, steps.findIndex(step => step.name === "Build the unified executable") + 1);
  assert.equal(step.if, "${{ !cancelled() }}");
  assert.equal(step["continue-on-error"], true);
  assert.equal(step.with.name, "native-build-timings-${{ matrix.target }}-${{ github.run_id }}-${{ github.run_attempt }}");
  assert.equal(step.with.path, "${{ runner.temp }}/native-build-timings");
  assert.equal(step.with["retention-days"], "${{ contains(github.workflow, 'Release') && 30 || 3 }}");
  assert.equal(step.with["if-no-files-found"], "ignore");
  assert.equal(workflow.jobs.build.env.SCCACHE_IDLE_TIMEOUT, "0");
});
