import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join, resolve, sep } from "node:path";
import test from "node:test";
import { cacheSaveDecision } from "./cache-save-decision.mjs";
import YAML from "./vendor/yaml.cjs";

test("only a matched restore with complete zero-miss snapshots suppresses saving", () => {
  mkdirSync(".tools", { recursive: true });
  const root = mkdtempSync(".tools/cache-save-");
  const snapshot = join(root, "sccache.json");
  try {
    const check = (value, expected) => {
      writeFileSync(snapshot, JSON.stringify(value));
      assert.equal(cacheSaveDecision("restored-prefix-key", [snapshot]).save, expected);
    };
    check({ stats: { compile_requests: 12, cache_misses: { counts: {} } } }, false);
    check({ stats: { compile_requests: 0, cache_misses: { counts: { Rust: 0 } } } }, true);
    check({ stats: { compile_requests: 12, cache_misses: { counts: { Rust: 0, "C/C++": 1 } } } }, true);
    for (const value of [null, {}, { stats: {} }, { stats: { compile_requests: 1 } },
      { stats: { compile_requests: 1, cache_misses: { counts: [] } } },
      ...["0", -1, null, 0.5].map(value => ({ stats: { compile_requests: 1, cache_misses: { counts: { Rust: value } } } }))]) check(value, true);
    assert.equal(cacheSaveDecision("", [snapshot]).save, true);
    assert.equal(cacheSaveDecision("matched", []).save, true);
    assert.equal(cacheSaveDecision("matched", [join(root, "missing.json")]).save, true);
    writeFileSync(snapshot, "{");
    assert.equal(cacheSaveDecision("matched", [snapshot]).save, true);
    writeFileSync(snapshot, JSON.stringify({ stats: { compile_requests: 1, cache_misses: { counts: {} } } }));
    const prior = join(root, "earlier-step.json");
    writeFileSync(prior, JSON.stringify({ stats: { compile_requests: 1, cache_misses: { counts: { Rust: 1 } } } }));
    assert.equal(cacheSaveDecision("matched", [prior, snapshot]).save, true);
    writeFileSync(prior, readFileSync(snapshot));
    assert.equal(cacheSaveDecision("matched", [prior, snapshot]).save, false);
    writeFileSync(prior, JSON.stringify({ stats: { compile_requests: 0, cache_misses: { counts: {} } } }));
    assert.equal(cacheSaveDecision("matched", [snapshot, prior]).save, true);
    const output = join(root, "output");
    writeFileSync(output, "");
    const result = spawnSync(process.execPath, ["scripts/cache-save-decision.mjs", "matched", snapshot], {
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

test("all per-run compiler cache consumers gate saving on the shared decision", () => {
  const read = file => YAML.parse(readFileSync(file, "utf8"));
  const native = read(".github/workflows/release-acyclic.yml").jobs.build.steps;
  const hosts = read(".github/workflows/agent-host-qualification.yml").jobs.qualify.steps;
  for (const steps of [native, hosts]) {
    const decision = steps.find(step => step.id === "compiler-cache");
    assert.equal(decision.env.MATCHED_CACHE_KEY, "${{ steps.rust-cache.outputs.compiler-matched-key }}");
    assert.ok(decision.run.startsWith('node scripts/cache-save-decision.mjs "$MATCHED_CACHE_KEY"'));
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
  assert.equal(decision.env.MATCHED_CACHE_KEY, "${{ steps.build-cache.outputs.cache-matched-key }}");
  assert.ok(decision.run.includes("sdk/observability/sccache.json"));
  const save = lane.find(step => step.name === "Save lane build cache");
  assert.ok(save.if.includes("steps.cache-save.outputs.save == 'true'"));
  assert.ok(save.if.includes("!cancelled()"));
  assert.ok(save.if.includes("github.ref == 'refs/heads/main'"));
  assert.ok(save.if.includes("inputs.lane != 'windows-rust'"));
  assert.ok(lane.indexOf(decision) > lane.findIndex(step => step.name === "Summarize lane timings"));
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
      assert.equal(cacheSaveDecision("matched", [join(root, "host-cache-build.json")]).save, true);
    }
  } finally {
    assert.ok(resolve(root).startsWith(resolve(".tools") + sep));
    rmSync(root, { recursive: true, force: true });
  }
});
