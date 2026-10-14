import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join, resolve, sep } from "node:path";
import test from "node:test";
import { cacheSaveDecision } from "./cache-save-decision.mjs";
import YAML from "./vendor/yaml.cjs";

test("only current-day snapshots with complete zero-miss statistics suppress saving", () => {
  mkdirSync(".tools", { recursive: true });
  const root = mkdtempSync(".tools/cache-save-");
  const snapshot = join(root, "sccache.json");
  try {
    assert.equal(cacheSaveDecision("today", "today", []).save, true);
    assert.equal(cacheSaveDecision("today", "today", [snapshot]).save, true);
    assert.equal(cacheSaveDecision("", "", []).save, true);
    const check = (value, expected) => {
      writeFileSync(snapshot, JSON.stringify(value));
      assert.equal(cacheSaveDecision("today", "today", [snapshot]).save, expected);
      assert.equal(cacheSaveDecision("today", "yesterday", [snapshot]).save, true);
    };
    check({ stats: { compile_requests: 12, cache_misses: { counts: {} } } }, false);
    check({ stats: { compile_requests: 0, cache_misses: { counts: { Rust: 0 } } } }, true);
    check({ stats: { compile_requests: 12, cache_misses: { counts: { Rust: 0, "C/C++": 1 } } } }, true);
    assert.equal(cacheSaveDecision("today", "today", [snapshot]).save, true);
    for (const value of [null, {}, { stats: {} }, { stats: { compile_requests: 1 } },
      { stats: { compile_requests: 1, cache_misses: { counts: [] } } },
      ...["0", -1, null, 0.5].map(value => ({ stats: { compile_requests: 1, cache_misses: { counts: { Rust: value } } } }))]) check(value, true);
    assert.equal(cacheSaveDecision("today", "", [snapshot]).save, true);
    assert.equal(cacheSaveDecision("today", "matched", []).save, true);
    assert.equal(cacheSaveDecision("today", "matched", [join(root, "missing.json")]).save, true);
    writeFileSync(snapshot, "{");
    assert.equal(cacheSaveDecision("today", "matched", [snapshot]).save, true);
    writeFileSync(snapshot, JSON.stringify({ stats: { compile_requests: 1, cache_misses: { counts: {} } } }));
    const prior = join(root, "earlier-step.json");
    writeFileSync(prior, JSON.stringify({ stats: { compile_requests: 1, cache_misses: { counts: { Rust: 1 } } } }));
    assert.equal(cacheSaveDecision("today", "today", [prior, snapshot]).save, true);
    writeFileSync(prior, readFileSync(snapshot));
    assert.equal(cacheSaveDecision("today", "today", [prior, snapshot]).save, false);
    writeFileSync(prior, JSON.stringify({ stats: { compile_requests: 0, cache_misses: { counts: {} } } }));
    assert.equal(cacheSaveDecision("today", "today", [snapshot, prior]).save, true);
    const output = join(root, "output");
    writeFileSync(output, "");
    const result = spawnSync(process.execPath, ["scripts/cache-save-decision.mjs", "today", "today", snapshot], {
      encoding: "utf8", env: { ...process.env, GITHUB_OUTPUT: output }, timeout: 10_000,
    });
    assert.equal(result.status, 0, result.stderr);
    assert.equal(readFileSync(output, "utf8"), "save=false\n");
    assert.match(result.stdout, /zero compiler misses/);
  } finally {
    assert.ok(resolve(root).startsWith(resolve(".tools") + sep));
    rmSync(root, { recursive: true, force: true });
  }
});

test("all compiler cache consumers gate saving on the shared decision", () => {
  const read = file => YAML.parse(readFileSync(file, "utf8"));
  const native = read(".github/workflows/release-acyclic.yml").jobs.build.steps;
  const hosts = read(".github/workflows/agent-host-qualification.yml").jobs.qualify.steps;
  for (const steps of [native, hosts]) {
    const decision = steps.find(step => step.id === "compiler-cache");
    assert.equal(decision.env.PRIMARY_CACHE_KEY, "${{ steps.rust-cache.outputs.compiler-key }}");
    assert.equal(decision.env.MATCHED_CACHE_KEY, "${{ steps.rust-cache.outputs.compiler-matched-key }}");
    assert.ok(decision.run.startsWith('node scripts/cache-save-decision.mjs "$PRIMARY_CACHE_KEY" "$MATCHED_CACHE_KEY"'));
    const save = steps.find(step => step.name === "Save native compiler output");
    assert.ok(save.if.includes("steps.compiler-cache.outputs.save == 'true'"));
    assert.ok(steps.indexOf(save) > steps.indexOf(decision));
  }
  assert.ok(native.find(step => step.id === "compiler-cache").run.includes("native-build-timings/sccache.json"));
  const cargoSteps = hosts.filter(step => step.run?.includes("cargo test"));
  assert.equal(cargoSteps.length, 4);
  for (const step of cargoSteps) assert.match(step.run, /trap 'sccache --show-stats --stats-format=json > .*host-cache-.*\|\| true' EXIT/);
  assert.ok(hosts.find(step => step.id === "compiler-cache").run.includes('"$RUNNER_TEMP"/host-cache-*.json'));
  const lane = read(".github/actions/qualification-lane/action.yml").runs.steps;
  assert.equal(lane.find(step => step.id === "build-cache").name, "Restore lane build cache");
  const decision = lane.find(step => step.id === "cache-save");
  assert.equal(decision.env.PRIMARY_CACHE_KEY, "${{ steps.build-cache.outputs.cache-primary-key }}");
  assert.equal(decision.env.MATCHED_CACHE_KEY, "${{ steps.build-cache.outputs.cache-matched-key }}");
  assert.ok(decision.run.includes("sdk/observability/sccache.json"));
  const save = lane.find(step => step.name === "Save lane build cache");
  assert.ok(save.if.includes("steps.cache-save.outputs.save == 'true'"));
  assert.ok(save.if.includes("!cancelled()"));
  assert.ok(save.if.includes("github.ref == 'refs/heads/main'"));
  assert.ok(save.if.includes("inputs.lane != 'windows-rust'"));
  assert.ok(lane.indexOf(decision) > lane.findIndex(step => step.name === "Summarize lane timings"));
  const restore = lane.find(step => step.id === "build-cache");
  assert.ok(restore.with.key.endsWith("${{ steps.sccache.outputs.day }}"));
  assert.doesNotMatch(restore.with.key, /run_id|run_attempt/);
  assert.equal(save.with.key, restore.with.key);
});

test("native and qualification snapshots use UTC day boundaries", () => {
  const actions = [".github/actions/rust-build-cache/action.yml", ".github/actions/qualification-lane/action.yml"];
  mkdirSync(".tools", { recursive: true });
  const root = mkdtempSync(".tools/cache-save-day-").replaceAll("\\", "/");
  const bash = process.platform === "win32" ? join(process.env.ProgramFiles ?? "C:/Program Files", "Git", "bin", "bash.exe") : "bash";
  try {
    for (const file of actions) {
      const step = YAML.parse(readFileSync(file, "utf8")).runs.steps.find(step => step.name.startsWith("Configure"));
      for (const day of ["20261014", "20261015"]) {
        writeFileSync(join(root, "output"), "");
        const command = `set -euo pipefail
CACHE_SIZE=384M
RUNNER_TEMP='${root}'
GITHUB_ENV='${root}/env'
GITHUB_OUTPUT='${root}/output'
GITHUB_EVENT_NAME=workflow_dispatch
RUNNER_OS=Linux
date() { [[ "$*" == '-u +%Y%m%d' ]] || return 1; printf '${day}\\n'; }
${step.run}`;
        const result = spawnSync(bash, ["-c", command], { encoding: "utf8", timeout: 60_000 });
        assert.ifError(result.error);
        assert.equal(result.status, 0, result.stderr);
        assert.match(readFileSync(join(root, "output"), "utf8"), new RegExp(`^day=${day}$`, "m"));
        writeFileSync(join(root, "output"), "");
        const localDate = spawnSync(bash, ["-c", command.replace("date -u ", "date ")], { encoding: "utf8", timeout: 60_000 });
        assert.ifError(localDate.error);
        assert.equal(localDate.status, 0, localDate.stderr);
        assert.doesNotMatch(readFileSync(join(root, "output"), "utf8"), /^day=[0-9]{8}$/m);
      }
    }
  } finally {
    assert.ok(resolve(root).startsWith(resolve(".tools") + sep));
    rmSync(root, { recursive: true, force: true });
  }
});

test("host live snapshots preserve failed builds even when diagnostics fail", () => {
  const hosts = YAML.parse(readFileSync(".github/workflows/agent-host-qualification.yml", "utf8"));
  const step = hosts.jobs.qualify.steps.find(step => step.name === "Build host scenario");
  const trap = step.run.split("\n")[0];
  mkdirSync(".tools", { recursive: true });
  const root = mkdtempSync(".tools/cache-save-host-").replaceAll("\\", "/");
  const bash = process.platform === "win32" ? join(process.env.ProgramFiles ?? "C:/Program Files", "Git", "bin", "bash.exe") : "bash";
  try {
    for (const [buildStatus, snapshotStatus] of [[0, 0], [7, 0], [0, 9], [7, 9]]) {
      const result = spawnSync(bash, ["-c", `set -euo pipefail
RUNNER_TEMP='${root}'
sccache() {
  printf '{"stats":{"compile_requests":1,"cache_misses":{"counts":{"Rust":1}}}}\\n'
  return ${snapshotStatus}
}
${trap}
exit ${buildStatus}
`], { encoding: "utf8", timeout: 60_000 });
      assert.ifError(result.error);
      assert.equal(result.status, buildStatus, result.stderr);
      assert.equal(cacheSaveDecision("today", "matched", [join(root, "host-cache-build.json")]).save, true);
    }
  } finally {
    assert.ok(resolve(root).startsWith(resolve(".tools") + sep));
    rmSync(root, { recursive: true, force: true });
  }
});
