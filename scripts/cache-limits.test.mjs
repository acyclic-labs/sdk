import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join, resolve, sep } from "node:path";
import test from "node:test";
import YAML from "./vendor/yaml.cjs";

const read = file => YAML.parse(readFileSync(file, "utf8"));
const lanes = JSON.parse(readFileSync(".github/qualification-lanes.json", "utf8"));

test("every compiler-cache scope receives its matrix limit without increasing existing caps", () => {
  const native = read(".github/workflows/release-acyclic.yml").jobs.build;
  const hosts = read(".github/workflows/agent-host-qualification.yml").jobs.qualify;
  const qualification = read(".github/workflows/qualification.yml").jobs.qualification;
  const restores = [native, hosts].map(job => job.steps.find(step => step.uses === "./.github/actions/rust-build-cache"));
  assert.equal(restores[0].with.cache_size, "${{ matrix.cache_size }}");
  assert.equal(restores[1].with.cache_size, "${{ matrix.platform.cache_size }}");
  const lane = qualification.steps.find(step => step.uses === "./.github/actions/qualification-lane");
  assert.equal(lane.with.cache_size, "${{ matrix.cache_size }}");
  assert.equal(qualification.env.SCCACHE_CACHE_SIZE, undefined);
  for (const [rows, previousCap] of [[native.strategy.matrix.include, 2048], [hosts.strategy.matrix.platform, 2048], [lanes, 8192]]) {
    for (const row of rows) {
      assert.match(row.cache_size, /^[1-9]\d*M$/);
      const cap = Number.parseInt(row.cache_size);
      assert.ok(cap <= (row.lane?.startsWith("windows") ? 3072 : previousCap));
    }
  }
  // No numeric cache_size was retained for this target; keep its existing cap.
  assert.equal(native.strategy.matrix.include.find(row => row.target === "win32-arm64").cache_size, "2048M");
});

test("the release planner retains every lane's cache limit", () => {
  const result = spawnSync(process.execPath, ["scripts/plan-qualification.mjs", "select"], {
    encoding: "utf8", env: { ...process.env, GITHUB_OUTPUT: "", PATH: "" }, timeout: 10_000,
  });
  assert.equal(result.status, 0, result.stderr);
  assert.deepEqual(JSON.parse(result.stdout.trim().slice("matrix=".length)), lanes);
});

test("compiler configuration applies the supplied limit on Linux and Windows", () => {
  const actions = [
    read(".github/actions/rust-build-cache/action.yml"),
    read(".github/actions/qualification-lane/action.yml"),
  ];
  mkdirSync(".tools", { recursive: true });
  const root = mkdtempSync(".tools/cache-limits-").replaceAll("\\", "/");
  const bash = process.platform === "win32" ? join(process.env.ProgramFiles ?? "C:/Program Files", "Git", "bin", "bash.exe") : "bash";
  try {
    for (const action of actions) {
      assert.equal(action.inputs.cache_size.required, true);
      const step = action.runs.steps.find(step => step.name.startsWith("Configure"));
      assert.equal(step.env.CACHE_SIZE, "${{ inputs.cache_size }}");
      for (const os of ["Linux", "Windows"]) {
        const envFile = join(root, "env");
        writeFileSync(envFile, "");
        writeFileSync(join(root, "output"), "");
        const result = spawnSync(bash, ["-c", `set -euo pipefail
CACHE_SIZE=384M
RUNNER_TEMP='${root}'
RUNNER_OS=${os}
GITHUB_ENV='${envFile}'
GITHUB_OUTPUT='${root}/output'
GITHUB_EVENT_NAME=workflow_dispatch
${step.run}`], { encoding: "utf8", timeout: 60_000 });
        assert.ifError(result.error);
        assert.equal(result.status, 0, result.stderr);
        assert.match(readFileSync(envFile, "utf8"), /^SCCACHE_CACHE_SIZE=384M$/m);
      }
    }
  } finally {
    assert.ok(resolve(root).startsWith(resolve(".tools") + sep));
    rmSync(root, { recursive: true, force: true });
  }
});
