import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test, { beforeEach } from "node:test";
import { spawnSync } from "node:child_process";
import { loomEnvironment, qualificationMain, qualifyFsPolicyLoom } from "./qualify-mcp-loom.mjs";
import { harnessModelSuites, qualifyHarnessModelsLoom, qualifyNativeHostLoom, qualifyNativeExecutor, qualifyNativeSources, qualifyNativeWakeLoom } from "./qualify-mcp-loom.mjs";
import { gateQualifications, qualificationPlan, qualifyMcpLoom, qualifyFsBindingLoom, qualifyHookLifecycleLoom, qualifyE2eProgressLoom, qualifyNativeProgressLoom, qualifySessionFsLoom, qualifyOwnershipLoom, qualifyNativeRegistryLoom } from "./qualify-mcp-loom.mjs";

const artifact = { reason: "compiler-artifact", target: { name: "main" }, profile: { test: true }, executable: "model-binary" };
const summary = (passed, ignored = 0, filtered = 0) => `test result: ok. ${passed} passed; 0 failed; ${ignored} ignored; 0 measured; ${filtered} filtered out;\n`;
// libtest's terse listing of `count` tests for the same selection arguments.
const listing = count => ({ status: 0, stderr: "", stdout: Array.from({ length: count }, (_, i) => `model::case_${i}: test\n`).join("") });
const lists = args => args.includes("--list");

beforeEach(t => {
  const runner = process.env.SDK_CHECKER_RUNNER;
  delete process.env.SDK_CHECKER_RUNNER;
  t.after(() => {
    if (runner === undefined) delete process.env.SDK_CHECKER_RUNNER;
    else process.env.SDK_CHECKER_RUNNER = runner;
  });
});

test("shared runner receives the binary and original arguments and must return a complete suite", t => {
  const directory = mkdtempSync(join(tmpdir(), "checker runner "));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const runner = join(directory, "runner.mjs");
  process.env.SDK_CHECKER_RUNNER = runner;
  for (const passed of [6, 0]) {
    writeFileSync(runner, `import assert from "node:assert/strict";
assert.deepEqual(process.argv.slice(2), ["--", "model-binary", "mcp_loom::"]);
process.stdout.write(${JSON.stringify(summary(passed))});
`);
    const run = (command, args, options) => {
      if (command === "cargo") return { status: 0, stdout: JSON.stringify(artifact) };
      if (command !== "model-binary") return spawnSync(command, args, options);
      // Listing bypasses the runner and asks the same binary about the same selection.
      assert.deepEqual(args, ["mcp_loom::", "--list", "--format", "terse"]);
      return listing(passed);
    };
    qualifyOrReject(passed === 6 ? null : "empty suite", () => qualifyMcpLoom(undefined, run));
  }
});

test("checker CLI publishes a terminal FAIL even when selection fails before compilation", () => {
  const result = spawnSync(process.execPath, ["scripts/qualify-mcp-loom.mjs", "--missing-obligation"], { encoding: "utf8" });
  assert.equal(result.status, 1);
  const markers = result.stdout.split(/\r?\n/).filter(line => line.startsWith("MODEL_CHECK_RESULT "));
  assert.equal(markers.length, 1);
  const terminal = JSON.parse(markers[0].slice("MODEL_CHECK_RESULT ".length));
  assert.equal(terminal.status, "FAIL");
  assert.deepEqual(terminal.args, ["--missing-obligation"]);
  assert.match(result.stderr, /unknown qualification argument/);
});

test("filesystem policy qualification requires real cache and authority models in the gate", () => {
  const suites = ["facade::policy_cache::loom_tests::", "distributed::head_payload_loom_tests::"];
  for (const missing of [null, ...suites]) {
    let executed = 0;
    const run = (command, args, options) => {
      if (command === "cargo") {
        assert.deepEqual(args, ["test", "--locked", "--message-format=json", "-p", "acyclic-fs", "--lib", "--no-run",
          "--no-default-features", "--features", "model-checking", "--features", "model-checking",]);
        assert.equal(options.env.ACYCLIC_LOOM, "1");
        return { status: 0, stdout: JSON.stringify({ ...artifact, target: { name: "acyclic_fs" } }) };
      }
      if (lists(args)) return listing(missing === args[0] ? 0 : 2);
      assert.deepEqual(args, [suites[executed++]]);
      return { status: 0, stdout: summary(missing === args[0] ? 0 : 2, 0, 100) };
    };
    qualifyOrReject(missing, () => qualifyFsPolicyLoom(run));
    if (missing === null) assert.equal(executed, suites.length);
  }
  assert.throws(() => qualifyFsPolicyLoom(() => ({ status: 101, stdout: "", stderr: "stale provider signature" })),
    /build failed.*stale provider signature/s);
  assertGateStep(["--fs-policy"]);
});

test("gate runs every harness model suite and rejects an absent suite or a stale model build", () => {
  const suites = Object.values(harnessModelSuites).flat();
  for (const missing of [null, ...suites]) {
    let target;
    let executed = 0;
    const run = (command, args, options) => {
      if (command === "cargo") {
        assert.equal(options.env.ACYCLIC_LOOM, "1");
        const crate = args[args.indexOf("-p") + 1];
        target = crate.replaceAll("-", "_");
        return { status: 0, stdout: JSON.stringify({ ...artifact, target: { name: target } }) };
      }
      assert.ok(suites.includes(args[0]), "execution must name an original suite");
      if (lists(args)) return listing(missing === args[0] ? 0 : 3);
      executed++;
      return { status: 0, stdout: summary(missing === args[0] ? 0 : 3, 0, 10) };
    };
    qualifyOrReject(missing, () => {
      qualifyHarnessModelsLoom(undefined, run);
    });
    if (missing === null) assert.equal(executed, suites.length);
  }
  assert.throws(() => qualifyHarnessModelsLoom(undefined, () => ({ status: 101, stdout: "", stderr: "stale record constructor" })), /build failed.*stale record constructor/s);
  assertGateStep(["--harness-models", "--all-features"]);
});

test("qualification rejects a run that differs from the same binary's listing", () => {
  // ran < listed: a selected test did not pass; ran > listed: the run is not the
  // listed selection. The rule is the same for prefix, exact and unfiltered runs.
  const listed = 3;
  for (const [qualify, target, selection] of [
    [run => qualifyMcpLoom(undefined, run), "main", ["mcp_loom::"]],
    [run => qualifySessionFsLoom(run), "main", ["session_fs_model::change_resume_exhausts_append_registration_and_wakeup", "--exact"]],
    [run => qualifyNativeWakeLoom(run), "wake_loom", []],
  ]) for (const ran of [listed - 1, listed, listed + 1]) {
    const calls = [];
    const run = (command, args) => {
      if (command === "cargo") return { status: 0, stdout: JSON.stringify({ ...artifact, target: { name: target } }) };
      calls.push(args);
      return lists(args) ? listing(listed) : { status: 0, stdout: summary(ran, 0, 7) };
    };
    if (ran === listed) qualify(run);
    else assert.throws(() => qualify(run), new RegExp(`requires a nonzero, complete test suite: listed=${listed} ran=${ran}$`));
    assert.deepEqual(calls.slice(0, 2), [selection, [...selection, "--list", "--format", "terse"]]);
  }
});

function qualifyOrReject(missing, qualify) {
  if (missing === null) qualify();
  else assert.throws(qualify, /requires a nonzero/);
}

test("gate rejects every trailing argument before starting a build", () => {
  for (const trailing of [["--bogus"], ["--features", "x",], ["--all-features"], ["--stream-retry"], ["--gate"], ["x"]]) {
    let builds = 0;
    const run = () => { builds++; throw new Error("unexpected build"); };
    assert.throws(() => qualificationMain(["--gate", ...trailing], run), /unknown qualification argument/);
    assert.equal(builds, 0);
  }
});

function assertGateStep(args) {
  const gate = readFileSync("scripts/qualify-ci.sh", "utf8").match(/  gate\)\n([\s\S]*?);;/)?.[1];
  assert.ok(gate, "the original qualification must retain its gate branch");
  assert.match(gate, /node scripts\/qualify-mcp-loom\.mjs --gate/);
  assert.ok(gateQualifications.some(step => JSON.stringify(step.args) === JSON.stringify(args)), `missing gate step: ${args}`);
}

test("progress qualification requires the cleanup and runnable-wake models and controls", () => {
  const names = ["e2e_progress_model::registration_reporting_and_cancel_or_panic_cleanup_are_atomic",
    "e2e_progress_model::forgotten_cleanup_is_detected_on_the_real_registration",
    "e2e_progress_model::polling_wake_and_reporting_never_lose_runnability",
    "e2e_progress_model::an_unobserved_wake_fails_the_same_runnability_oracle"];
  for (const missing of [null, 1, 2, 3, 4]) {
    let models = 0;
    const run = (command, args, options) => {
      if (command === "cargo") {
        assert.equal(options.env.ACYCLIC_LOOM, "1");
        assert.deepEqual(args.slice(0, 8), ["test", "--locked", "--message-format=json", "-p", "acyclic-harness", "--test", "main", "--no-run"]);
        return {status: 0, stdout: JSON.stringify({...artifact, target: {name: "main"}})};
      }
      if (lists(args)) { assert.equal(args[0], names[models - 1]); return listing(1); }
      assert.equal(args[0], names[models++]);
      return {status: 0, stdout: summary(models === missing ? 0 : 1, 0, 1)};
    };
    if (missing === null) { qualifyE2eProgressLoom(undefined, run); assert.equal(models, 4); }
    else assert.throws(() => qualifyE2eProgressLoom(undefined, run), /requires a nonzero/);
  }
});

test("native progress qualification instruments the real mutex and requires every selected model", () => {
  for (const [qualify, count, pattern, ordinary = false] of [
    [qualifyNativeProgressLoom, 11, /^(?:testing::(?:(?:queue_)?model::|(?:durability|entropy)::models::)|device_flush::model::)/],
    [run => qualifyNativeHostLoom(run, true), 3, /^testing::host::models::/],
    [qualifyNativeExecutor, 2, /^testing::executor::models::/],
    [run => qualifyNativeExecutor(run, true), 1, /^testing::executor::tests::/, true],
    [qualifyNativeSources, 1, /^testing::clock::tests::/, true],
    [run => qualifyNativeSources(run, true), 3, /^(?:testing::entropy::models::|device_flush::model::)/],
  ]) for (const missing of [null, ...Array.from({ length: count }, (_, index) => index + 1)]) {
    let models = 0;
    const run = (command, args, options) => {
      if (command === "cargo") {
        if (ordinary) assert.equal(options.env, undefined);
        else assert.equal(options.env.ACYCLIC_LOOM, "1");
        assert.ok(args.includes("--lib") && args.includes("--no-run"));
        return {status: 0, stdout: JSON.stringify({...artifact, target: {name: "acyclic_native_runtime"}})};
      }
      assert.match(args[0], pattern);
      if (lists(args)) return listing(1);
      return {status: 0, stdout: summary(++models === missing ? 0 : 1, 0, 4)};
    };
    if (missing === null) { qualify(run); assert.equal(models, count); }
    else assert.throws(() => qualify(run), /requires a nonzero/);
  }
});

test("fs Session qualification instruments real provider and Session locks and requires every model", () => {
  for (const missing of [null, 1, 2]) {
    let models = 0;
    const run = (command, args, options) => {
      if (command === "cargo") {
        assert.equal(options.env.ACYCLIC_LOOM, "1");
        assert.ok(args.includes("main"));
        return { status: 0, stderr: "", stdout: JSON.stringify({ ...artifact, target: { name: "main" } }) };
      }
      assert.match(args[0], /^session_fs_model::change_resume_exhausts_/);
      assert.equal(args[1], "--exact");
      if (lists(args)) return listing(1);
      models++;
      return { status: 0, stderr: "", stdout: summary(models === missing ? 0 : 1, 0, 2) };
    };
    if (missing === null) { qualifySessionFsLoom(run); assert.equal(models, 2); }
    else assert.throws(() => qualifySessionFsLoom(run), /requires a nonzero/);
  }
  assertGateStep(["--session-fs"]);
});

test("hook qualification requires every retained projection model", () => {
  /** @type {Array<[typeof qualifyHookLifecycleLoom, string, number]>} */
  const hooks = [[qualifyHookLifecycleLoom, "acyclic_harness", 6]];
  for (const [qualify, target, count] of hooks) {
    for (const missing of [null, ...Array.from({ length: count }, (_, i) => i + 1)]) {
      let models = 0;
      const run = (command, args, options) => {
        if (command === "cargo") {
          assert.equal(options.env.ACYCLIC_LOOM, "1");
          return { status: 0, stderr: "", stdout: JSON.stringify({ ...artifact, target: { name: target } }) };
        }
        assert.match(args[0], /local::workspace::projection::tests::/);
        assert.equal(args[1], "--exact");
        if (lists(args)) return listing(1);
        models++;
        return { status: 0, stderr: "", stdout: summary(models === missing ? 0 : 1, 0, 100) };
      };
      if (missing === null) { qualify(undefined, run); assert.equal(models, count); }
      else assert.throws(() => qualify(undefined, run), /requires a nonzero/);
    }
  }
  assertGateStep(["--hook-lifecycle", "--all-features"]);
});

test("qualification opts into the crate cfg only for the model build and executes its binary", () => {
  const calls = [];
  qualifyMcpLoom(["--all-features"], (command, args, options) => {
    if (command === "cargo") {
      assert.equal(options.env?.ACYCLIC_LOOM, "1");
    }
    calls.push([command, args]);
    if (lists(args)) return listing(6);
    return { status: 0, stderr: "", stdout: calls.length === 1 ? JSON.stringify(artifact) : summary(6) };
  });
  assert.deepEqual(calls, [
    ["cargo", ["test", "--locked", "--message-format=json", "-p", "acyclic-harness-core", "--test", "main", "--no-run", "--all-features", "--features", "model-checking", "--features", "model-checking",]],
    ["model-binary", ["mcp_loom::"]],
    ["model-binary", ["mcp_loom::", "--list", "--format", "terse"]],
  ]);
  const qualification = readFileSync("scripts/qualify-ci.sh", "utf8");
  assertGateStep(["--all-features"]);
  assertGateStep(["--stream-retry"]);
  assertGateStep(["--guest"]);
  const policy = qualification.match(/  policy\)\n([\s\S]*?);;/)?.[1];
  assert.ok(policy, "the original qualification must retain its policy branch");
  assert.match(policy, /node --test[^\n]*scripts\/qualify-mcp-loom\.test\.mjs/);
});

test("guest qualification selects every guest model in the instrumented main target", () => {
  const plan = qualificationPlan(["--guest"]);
  assert.deepEqual(plan.args, ["test", "--locked", "--message-format=json", "-p", "acyclic-actors", "--test", "main", "--no-run", "--features", "model-checking",]);
  assert.equal(plan.loom, loomEnvironment().ACYCLIC_LOOM);
  assert.deepEqual(plan.tests, [{ args: ["guest_loom::"] }]);
});

test("qualification rejects empty, skipped and failed model execution", () => {
  /** @type {Array<[number, string, number]>} */
  const executions = [[0, summary(0), 0], [0, summary(5, 1), 6], [1, summary(6), 6]];
  for (const [status, output, listed] of executions) {
    assert.throws(() => qualifyMcpLoom(undefined, (command, args) => command === "cargo"
      ? { status: 0, stdout: JSON.stringify(artifact), stderr: "" }
      : lists(args) ? listing(listed) : { status, stdout: output, stderr: "" }), /MCP Loom (qualification requires|tests failed)/);
  }
});

test("qualification rejects failed builds and missing or ambiguous artifacts", () => {
  /** @type {Array<[number, string]>} */
  const builds = [[1, ""], [0, ""], [0, `${JSON.stringify(artifact)}\n${JSON.stringify(artifact)}`]];
  for (const [status, stdout] of builds) {
    assert.throws(() => qualifyMcpLoom(undefined, () => ({ status, stdout, stderr: "" })),
      /MCP Loom build (failed|must produce)/);
  }
});


test("filesystem qualification selects every retained binding model in the unified target", () => {
  const calls = [];
  qualifyFsBindingLoom(["--all-features"], (command, args, options) => {
    if (command === "cargo") assert.equal(options.env?.ACYCLIC_LOOM, "1");
    calls.push([command, args]);
    const fs = { ...artifact, target: { name: "main" } };
    if (lists(args)) return listing(5);
    return { status: 0, stderr: "", stdout: command === "cargo" ? JSON.stringify(fs) : summary(5, 0, 4) };
  });
  assert.deepEqual(calls, [
    ["cargo", ["test", "--locked", "--message-format=json", "-p", "acyclic-fs", "--test", "main", "--no-run", "--all-features", "--features", "model-checking"]],
    ["model-binary", ["native_binding_model::"]],
    ["model-binary", ["native_binding_model::", "--list", "--format", "terse"]],
  ]);
  assertGateStep(["--fs-binding", "--all-features"]);
});

test("registry gate instruments real native locks and rejects either omitted proof", () => {
  for (const missing of [null, 1, 2]) {
    let models = 0;
    const run = (command, args, options) => {
      if (command === "cargo") {
        assert.equal(options.env.ACYCLIC_LOOM, "1");
        assert.ok(args.includes("--lib"));
        return { status: 0, stderr: "", stdout: JSON.stringify({ ...artifact, target: { name: "acyclic_fs" } }) };
      }
      assert.match(args[0], /native_mount::fence::models::/);
      assert.equal(args[1], "--exact");
      if (lists(args)) return listing(1);
      models++;
      return { status: 0, stderr: "", stdout: summary(models === missing ? 0 : 1, 0, 100) };
    };
    if (missing === null) { qualifyNativeRegistryLoom(run); assert.equal(models, 2); }
    else assert.throws(() => qualifyNativeRegistryLoom(run), /requires a nonzero/);
  }
  assertGateStep(["--native-registry"]);
});
