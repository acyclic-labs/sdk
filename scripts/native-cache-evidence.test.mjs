import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join, resolve, sep } from "node:path";
import test from "node:test";
import YAML from "./vendor/yaml.cjs";

const workflow = YAML.parse(readFileSync(".github/workflows/release-acyclic.yml", "utf8"));

function assertCacheRevisions(action, release) {
  let revision;
  for (const [steps, names, operation] of [
    [action.runs.steps, ["Restore Cargo downloads", "Restore compiler output"], "restore"],
    [release.jobs.build.steps, ["Save native compiler output", "Save native Cargo downloads"], "save"],
  ]) {
    for (const name of names) {
      const uses = steps.find(step => step.name === name)?.uses;
      const pinned = new RegExp(`^actions/cache/${operation}@([0-9a-f]{40})$`).exec(uses);
      assert.ok(pinned, `${name} requires an immutable ${operation} action`);
      revision ??= pinned[1];
      assert.equal(pinned[1], revision, `${name} must preserve the cache action revision`);
    }
  }
}

test("cache revision agreement covers every immutable restore/save pin", () => {
  const fixture = () => {
    const action = YAML.parse(readFileSync(".github/actions/rust-build-cache/action.yml", "utf8"));
    const release = structuredClone(workflow);
    const steps = [
      ...["downloads", "compiler"].map(id => action.runs.steps.find(step => step.id === id)),
      ...["Save native compiler output", "Save native Cargo downloads"].map(name => release.jobs.build.steps.find(step => step.name === name)),
    ];
    return { action, release, steps };
  };
  for (let mask = 0; mask < 16; mask++) {
    const { action, release, steps } = fixture();
    steps.forEach((step, index) => { step.uses = step.uses.split("@")[0] + "@" + ((mask >> index) & 1 ? "a" : "b").repeat(40); });
    if (mask === 0 || mask === 15) assertCacheRevisions(action, release);
    else assert.throws(() => assertCacheRevisions(action, release), /preserve the cache action revision/);
  }
  for (let index = 0; index < 4; index++) {
    for (const pin of ["v6", "a".repeat(39), "g".repeat(40)]) {
      const { action, release, steps } = fixture();
      steps[index].uses = steps[index].uses.split("@")[0] + "@" + pin;
      assert.throws(() => assertCacheRevisions(action, release), /requires an immutable/);
    }
    const { action, release, steps } = fixture();
    steps[index].uses = `actions/cache/${index < 2 ? "save" : "restore"}@${"a".repeat(40)}`;
    assert.throws(() => assertCacheRevisions(action, release), /requires an immutable/);
  }
});

test("the product Cargo invocation preserves the configured compiler wrapper", () => {
  const result = spawnSync(process.execPath, ["--input-type=module", "--eval", `
    import childProcess from 'node:child_process';
    import { syncBuiltinESMExports } from 'node:module';
    childProcess.spawnSync = (command, args, options) => {
      console.log(JSON.stringify({ command, args, wrapper: options.env.RUSTC_WRAPPER }));
      return { status: 0 };
    };
    syncBuiltinESMExports();
    await import('./scripts/build-product.mjs');
  `], { encoding: "utf8", timeout: 60_000,
    env: { ...process.env, CARGO: "fixture-cargo", RUSTC_WRAPPER: "fixture-sccache",
      RUSTFLAGS: "", CARGO_ENCODED_RUSTFLAGS: "" } });
  assert.ifError(result.error);
  assert.equal(result.status, 0, result.stderr);
  const invocation = JSON.parse(result.stdout);
  assert.equal(invocation.command, "fixture-cargo");
  assert.equal(invocation.wrapper, "fixture-sccache");
  assert.deepEqual(invocation.args.slice(0, 6), ["build", "--locked", "--profile", "dist", "-p", "acyclic-plugin"]);
  assert.ok(invocation.args.includes("--timings"));
});

test("native compiler caches keep one daily snapshot while restoring the previous working set", () => {
  const action = YAML.parse(readFileSync(".github/actions/rust-build-cache/action.yml", "utf8"));
  const compiler = action.runs.steps.find(step => step.name === "Restore compiler output");
  assertCacheRevisions(action, workflow);
  assert.equal(compiler.with.path, "${{ runner.temp }}/sccache");
  const [matchingInputs, matchingTarget] = compiler.with["restore-keys"].trim().split("\n");
  assert.equal(compiler.with.key, matchingInputs + "${{ steps.cache-day.outputs.day }}");
  assert.equal(matchingInputs, matchingTarget + "${{ hashFiles('rust-toolchain.toml', 'Cargo.lock', '**/Cargo.toml') }}-");
  assert.equal(matchingTarget, "rust-objects-v2-${{ inputs.scope }}-${{ runner.os }}-${{ runner.arch }}-");
  const key = day => compiler.with.key.replace("${{ steps.cache-day.outputs.day }}", day);
  assert.notEqual(key("20261014"), key("20261015"));
  assert.doesNotMatch(compiler.with.key, /run_id|run_attempt/);
  for (const value of [key("20261014"), key("20261015")]) {
    assert.ok(value.startsWith(matchingInputs));
  }
  const downloads = action.runs.steps.find(step => step.name === "Restore Cargo downloads");
  assert.equal(downloads.with.key, "rust-deps-v2-${{ runner.os }}-${{ runner.arch }}-${{ hashFiles('rust-toolchain.toml', 'Cargo.lock') }}");
  assert.doesNotMatch(downloads.with.key, /run_id|run_attempt/);
});

test("native restore summaries use matched-key outputs and post-build saves preserve cache versions", () => {
  const action = YAML.parse(readFileSync(".github/actions/rust-build-cache/action.yml", "utf8"));
  const summary = action.runs.steps.find(step => step.name === "Summarize cache restores");
  for (const [id, label] of [["downloads", "DOWNLOAD"], ["compiler", "COMPILER"]]) {
    assert.equal(summary.env[label + "_REQUESTED"], `\${{ steps.${id}.outputs.cache-primary-key }}`);
    assert.equal(summary.env[label + "_MATCHED"], `\${{ steps.${id}.outputs.cache-matched-key }}`);
    assert.equal(action.outputs[id + "-key"].value, `\${{ steps.${id}.outputs.cache-primary-key }}`);
    if (id === "compiler") assert.equal(action.outputs["compiler-matched-key"].value, "${{ steps.compiler.outputs.cache-matched-key }}");
    else assert.equal(action.outputs["downloads-hit"].value, "${{ steps.downloads.outputs.cache-hit }}");
  }
  assert.ok(summary.run.includes('tee -a "$GITHUB_STEP_SUMMARY"'));
  assert.ok(summary.run.includes('${DOWNLOAD_MATCHED:-none}'));
  assert.ok(summary.run.includes('${COMPILER_MATCHED:-none}'));
  for (const steps of [workflow.jobs.build.steps]) {
    const restore = steps.find(step => step.uses === "./.github/actions/rust-build-cache");
    assert.equal(restore.id, "rust-cache");
    for (const [id, name] of [["compiler", "Save native compiler output"], ["downloads", "Save native Cargo downloads"]]) {
      const save = steps.find(step => step.name === name);
      const cached = action.runs.steps.find(step => step.id === id);
      assert.equal(save.with.path, cached.with.path);
      assert.equal(save.with.key, `\${{ steps.rust-cache.outputs.${id}-key }}`);
      assert.equal(save.if, id === "compiler"
        ? "github.ref == 'refs/heads/main' && github.event_name != 'pull_request' && github.event_name != 'pull_request_target' && steps.rust-cache.outputs.compiler-key != '' && steps.compiler-cache.outputs.save == 'true'"
        : "github.ref == 'refs/heads/main' && github.event_name != 'pull_request' && github.event_name != 'pull_request_target' && steps.rust-cache.outputs.downloads-key != '' && steps.rust-cache.outputs.downloads-hit != 'true'");
      assert.ok(steps.indexOf(save) > steps.findLastIndex(step => step.run));
      assert.ok(steps.indexOf(save) > steps.findLastIndex(step => step.uses?.startsWith("actions/upload-artifact@")));
    }
  }
});

test("native restore summary prints exact requested and matched keys, including misses", () => {
  const action = YAML.parse(readFileSync(".github/actions/rust-build-cache/action.yml", "utf8"));
  const summary = action.runs.steps.find(step => step.name === "Summarize cache restores");
  mkdirSync(".tools", { recursive: true });
  const root = mkdtempSync(".tools/cache-restore-keys-");
  const bash = process.platform === "win32" ? join(process.env.ProgramFiles ?? "C:/Program Files", "Git", "bin", "bash.exe") : "bash";
  try {
    for (const matched of ["compiler-current", "compiler-previous-run", ""]) {
      const path = join(root, "summary.md");
      writeFileSync(path, "");
      const result = spawnSync(bash, ["-c", summary.run], {
        encoding: "utf8", timeout: 60_000,
        env: { ...process.env, GITHUB_STEP_SUMMARY: resolve(path).replaceAll("\\", "/"),
          DOWNLOAD_REQUESTED: "downloads-requested", DOWNLOAD_MATCHED: "downloads-restored",
          COMPILER_REQUESTED: "compiler-current", COMPILER_MATCHED: matched },
      });
      assert.ifError(result.error);
      assert.equal(result.status, 0, result.stderr);
      const expected = `| Compiler output | compiler-current | ${matched || "none"} |`;
      for (const output of [result.stdout, readFileSync(path, "utf8")]) {
        assert.ok(output.includes("| Cargo downloads | downloads-requested | downloads-restored |"));
        assert.ok(output.includes(expected));
      }
    }
  } finally {
    assert.ok(resolve(root).startsWith(resolve(".tools") + sep));
    rmSync(root, { recursive: true, force: true });
  }
});

test("native cache snapshot runs before leaving the build step and preserves its result", () => {
  const build = workflow.jobs.build.steps.find(step => step.name === "Build the unified executable");
  const start = build.run.indexOf("trap '\n");
  const end = build.run.indexOf("\n' EXIT", start);
  assert.ok(start >= 0 && end > start);
  const trap = build.run.slice(start, end + "\n' EXIT".length) + "\n";
  assert.ok(end < build.run.indexOf("node scripts/build-product.mjs"));
  const startup = build.run.indexOf("sccache --start-server");
  assert.ok(startup >= 0, "native builds must start the compiler-cache daemon");
  assert.ok(startup < build.run.indexOf("cargo build "));
  assert.ok(startup < build.run.indexOf("node scripts/build-product.mjs"));
  assert.ok(build.run.includes('command -v "$RUSTC_WRAPPER"'));
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
  assert.equal(index, steps.findIndex(step => step.name === "Build the unified executable") + 2);
  const timings = steps[index - 1];
  assert.equal(timings.name, "Summarize native Cargo unit timings");
  assert.equal(timings.if, "${{ !cancelled() }}");
  assert.ok(timings.run.includes('"product=$GITHUB_WORKSPACE/target"'));
  assert.ok(timings.run.includes('"qualifier=$RUNNER_TEMP/target-qualifier"'));
  assert.ok(timings.run.includes('"install=$RUNNER_TEMP/cargo-install-target"'));
  const build = steps.find(step => step.name === "Build the unified executable");
  assert.ok(build.run.includes("cargo build --timings --locked --release"));
  assert.ok(build.run.includes("cargo install --timings --locked --path plugin"));
  assert.equal(step.if, "${{ !cancelled() }}");
  assert.equal(step["continue-on-error"], true);
  assert.equal(step.with.name, "native-build-timings-${{ matrix.target }}-${{ github.run_id }}-${{ github.run_attempt }}");
  assert.equal(step.with.path, "${{ runner.temp }}/native-build-timings");
  assert.equal(step.with["retention-days"], "${{ contains(github.workflow, 'Release') && 30 || 3 }}");
  assert.equal(step.with["if-no-files-found"], "ignore");
  assert.equal(workflow.jobs.build.env.SCCACHE_IDLE_TIMEOUT, "0");
  const qualification = YAML.parse(readFileSync(".github/workflows/qualification.yml", "utf8"));
  const hosts = YAML.parse(readFileSync(".github/workflows/agent-host-qualification.yml", "utf8"));
  assert.equal(qualification.jobs.qualification.env.SCCACHE_IDLE_TIMEOUT, "0");
  assert.equal(hosts.jobs.qualify.env.SCCACHE_IDLE_TIMEOUT, "0");
});
