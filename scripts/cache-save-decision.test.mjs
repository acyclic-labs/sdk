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

test("native compiler output is saved on main after the shared decision", () => {
  const native = YAML.parse(readFileSync(".github/workflows/release-acyclic.yml", "utf8")).jobs.build.steps;
  const decision = native.find(step => step.id === "compiler-cache");
  assert.equal(decision.env.PRIMARY_CACHE_KEY, "${{ steps.rust-cache.outputs.compiler-key }}");
  assert.equal(decision.env.MATCHED_CACHE_KEY, "${{ steps.rust-cache.outputs.compiler-matched-key }}");
  assert.ok(decision.run.includes("native-build-timings/sccache.json"));
  const save = native.find(step => step.name === "Save native compiler output");
  assert.ok(save.if.includes("steps.compiler-cache.outputs.save == 'true'"));
  assert.ok(save.if.includes("github.ref == 'refs/heads/main'"));
  assert.ok(native.indexOf(save) > native.indexOf(decision));
});

test("native snapshots use UTC day boundaries", () => {
  const actions = [".github/actions/rust-build-cache/action.yml"];
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
