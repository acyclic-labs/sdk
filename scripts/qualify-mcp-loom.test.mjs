import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import test from "node:test";
import { loomEnvironment, qualificationMain, qualifySkillMaterializerLoom, qualifyFsPolicyLoom } from "./qualify-mcp-loom.mjs";
import { harnessModelSuites, qualifyHarnessModelsLoom } from "./qualify-mcp-loom.mjs";
import { gateQualifications, qualifyMcpLoom, qualifyTimerLoom, qualifyGuestLoom, qualifyFsBindingLoom, qualifyHookLifecycleLoom, qualifyE2eProgressLoom, qualifyNativeProgressLoom, qualifySessionFsLoom, qualifyOwnershipLoom, qualifyNativeRegistryLoom } from "./qualify-mcp-loom.mjs";

const artifact = { reason: "compiler-artifact", target: { name: "mcp-loom" }, profile: { test: true }, executable: "model-binary" };
const summary = (passed, ignored = 0, filtered = 0) => `test result: ok. ${passed} passed; 0 failed; ${ignored} ignored; 0 measured; ${filtered} filtered out;\n`;

test("filesystem policy qualification requires real cache and authority models in the gate", () => {
  const suites = [["facade::policy_cache::loom_tests::", 2], ["distributed::head_payload_loom_tests::", 8]];
  for (const missing of [null, ...suites.map(([prefix]) => prefix)]) {
    let executed = 0;
    const run = (command, args, options) => {
      if (command === "cargo") {
        assert.deepEqual(args, ["test", "--locked", "--message-format=json", "-p", "acyclic-fs", "--lib", "--no-run",
          "--no-default-features", "--features", "memory,model-checking", "--features", "model-checking"]);
        assert.match(options.env.RUSTC_WORKSPACE_WRAPPER, /loom-rustc\.(?:sh|cmd)$/);
        return { status: 0, stdout: JSON.stringify({ ...artifact, target: { name: "acyclic_fs" } }) };
      }
      const [prefix, count] = suites[executed++];
      assert.deepEqual(args, [prefix]);
      return { status: 0, stdout: summary(missing === prefix ? 0 : count, 0, 100) };
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
  for (const missing of [null, ...suites.map(([prefix]) => prefix)]) {
    let target;
    let executed = 0;
    const run = (command, args, options) => {
      if (command === "cargo") {
        assert.match(options.env.RUSTC_WORKSPACE_WRAPPER, /loom-rustc\.(?:sh|cmd)$/);
        const crate = args[args.indexOf("-p") + 1];
        target = crate.replaceAll("-", "_");
        return { status: 0, stdout: JSON.stringify({ ...artifact, target: { name: target } }) };
      }
      assert.equal(options.timeout, undefined);
      executed++;
      const passed = suites.find(([prefix]) => prefix === args[0])[1];
      return { status: 0, stdout: summary(missing === args[0] ? 0 : passed, 0, 10) };
    };
    qualifyOrReject(missing, () => {
      qualifyHarnessModelsLoom(undefined, run);
    });
    if (missing === null) assert.equal(executed, suites.length);
  }
  assert.throws(() => qualifyHarnessModelsLoom(undefined, () => ({ status: 101, stdout: "", stderr: "stale record constructor" })), /build failed.*stale record constructor/s);
  assertGateStep(["--harness-models", "--all-features"]);
});

test("checkout qualification requires all nine tests including nested fencing", () => {
  assert.deepEqual(harnessModelSuites["acyclic-harness"].find(([prefix]) => prefix === "local::checkout_tools::tests::"),
    ["local::checkout_tools::tests::", 9]);
  for (const checkoutCount of [8, 9, 10]) {
    const run = command => command === "cargo"
      ? { status: 0, stdout: JSON.stringify({ ...artifact, target: { name: "acyclic_harness" } }) }
      : undefined;
    const execute = (command, args) => command === "cargo" ? run(command) : {
      status: 0,
      stdout: summary(args[0] === "local::checkout_tools::tests::" ? checkoutCount
        : harnessModelSuites["acyclic-harness"].find(([prefix]) => prefix === args[0])[1], 0, 100),
    };
    if (checkoutCount === 9) qualifyHarnessModelsLoom(undefined, execute);
    else assert.throws(() => qualifyHarnessModelsLoom(undefined, execute), /requires a nonzero/);
  }
});

function qualifyOrReject(missing, qualify) {
  if (missing === null) qualify();
  else assert.throws(qualify, /requires a nonzero/);
}

test("skill materializer qualification requires the production owner model and its negative control", () => {
  for (const missing of [null, 1, 2]) {
    let models = 0;
    const run = (command, args, options) => {
      if (command === "cargo") {
        assert.deepEqual(args.slice(-3), ["--", "--cfg", "loom"]);
        assert.ok(args.includes("skill-materializer-loom"));
        return { status: 0, stdout: JSON.stringify({ ...artifact, target: { name: "skill-materializer-loom" } }) };
      }
      assert.equal(args[0], ["journal_owner_serializes_first_publications", "former_independent_publishers_can_conflict"][models]);
      assert.equal(args[1], "--exact");
      assert.equal(options.timeout, 9500);
      return { status: 0, stdout: summary(++models === missing ? 0 : 1, 0, 1) };
    };
    if (missing === null) { qualifySkillMaterializerLoom(run); assert.equal(models, 2); }
    else assert.throws(() => qualifySkillMaterializerLoom(run), /requires a nonzero/);
  }
  assertGateStep(["--skill-materializer"]);
});

test("gate rejects every trailing argument before starting a build", () => {
  for (const trailing of [["--bogus"], ["--features", "x"], ["--all-features"], ["--timer"], ["--gate"], ["x"]]) {
    let builds = 0;
    const run = () => { builds++; throw new Error("unexpected build"); };
    assert.throws(() => qualificationMain(["--gate", ...trailing], run), /unknown qualification argument/);
    assert.equal(builds, 0);
  }
});

function assertGateStep(args) {
  const gate = readFileSync("scripts/qualify-ci.sh", "utf8").match(/  gate\)\n([\s\S]*?);;/)?.[1];
  assert.match(gate, /node scripts\/qualify-mcp-loom\.mjs --gate/);
  assert.ok(gateQualifications.some(step => JSON.stringify(step.args) === JSON.stringify(args)), `missing gate step: ${args}`);
}

test("ownership qualification requires every dedicated real-lock model and compiler controls in CI", () => {
  for (const passed of [0, 4, 7, 8, 9]) {
    const run = (command, args, options) => {
      if (command === "cargo") {
        assert.deepEqual(args.slice(0, 7), ["test", "--locked", "--message-format=json", "-p", "acyclic-native-runtime", "--lib", "--no-run"]);
        assert.match(options.env.RUSTC_WORKSPACE_WRAPPER, /loom-rustc\.(?:sh|cmd)$/);
        return {status: 0, stderr: "", stdout: JSON.stringify({...artifact, target: {name: "acyclic_native_runtime"}})};
      }
      assert.deepEqual(args, ["ownership::model::"]);
      assert.equal(options.timeout, 9500);
      return {status: 0, stderr: "", stdout: summary(passed, 0, 50)};
    };
    if (passed === 8) qualifyOwnershipLoom(run);
    else assert.throws(() => qualifyOwnershipLoom(run), /requires a nonzero/);
  }
  const gate = readFileSync("scripts/qualify-ci.sh", "utf8").match(/  gate\)\n([\s\S]*?);;/)?.[1];
  const requireProofs = text => {
    assert.match(text, /^    node scripts\/check-hermetic-clippy\.mjs$/m);
    assertGateStep(["--ownership"]);
  };
  requireProofs(gate);
  for (const line of ["    node scripts/check-hermetic-clippy.mjs"]) {
    assert.throws(() => requireProofs(gate.replace(line, "")), assert.AssertionError);
  }
});

test("progress qualification requires the cleanup and runnable-wake models and controls", () => {
  const names = ["registration_reporting_and_cancel_or_panic_cleanup_are_atomic",
    "forgotten_cleanup_is_detected_on_the_real_registration",
    "polling_wake_and_reporting_never_lose_runnability",
    "an_unobserved_wake_fails_the_same_runnability_oracle"];
  for (const missing of [null, 1, 2, 3, 4]) {
    let models = 0;
    const run = (command, args) => {
      if (command === "cargo") {
        assert.deepEqual(args.slice(-3), ["--", "--cfg", "loom"]);
        return {status: 0, stdout: JSON.stringify({...artifact, target: {name: "e2e-progress-model"}})};
      }
      assert.equal(args[0], names[models++]);
      return {status: 0, stdout: summary(models === missing ? 0 : 1, 0, 1)};
    };
    if (missing === null) { qualifyE2eProgressLoom(undefined, run); assert.equal(models, 4); }
    else assert.throws(() => qualifyE2eProgressLoom(undefined, run), /requires a nonzero/);
  }
});

test("native progress qualification instruments the real mutex and requires every ownership model", () => {
  for (const missing of [null, ...Array.from({ length: 18 }, (_, index) => index + 1)]) {
    let models = 0;
    const run = (command, args, options) => {
      if (command === "cargo") {
        assert.match(options.env.RUSTC_WORKSPACE_WRAPPER, /loom-rustc\.(?:sh|cmd)$/);
        assert.ok(args.includes("--lib") && args.includes("--no-run"));
        return {status: 0, stdout: JSON.stringify({...artifact, target: {name: "acyclic_native_runtime"}})};
      }
      assert.match(args[0], /^testing::(?:queue_)?model::/);
      return {status: 0, stdout: summary(++models === missing ? 0 : 1, 0, 4)};
    };
    if (missing === null) { qualifyNativeProgressLoom(run); assert.equal(models, 18); }
    else assert.throws(() => qualifyNativeProgressLoom(run), /requires a nonzero/);
  }
});

test("fs Session qualification instruments real provider and Session locks and requires every model", () => {
  for (const missing of [null, 1, 2]) {
    let models = 0;
    const run = (command, args, options) => {
      if (command === "cargo") {
        assert.match(options.env.RUSTC_WORKSPACE_WRAPPER, /loom-rustc\.(?:sh|cmd)$/);
        assert.ok(args.includes("memory,session,acyclic-stream/local"));
        assert.ok(args.includes("session_fs_model"));
        return { status: 0, stderr: "", stdout: JSON.stringify({ ...artifact, target: { name: "session_fs_model" } }) };
      }
      assert.equal(args[1], "--exact");
      assert.equal(options.timeout, 9500);
      models++;
      return { status: 0, stderr: "", stdout: summary(models === missing ? 0 : 1, 0, 2) };
    };
    if (missing === null) { qualifySessionFsLoom(run); assert.equal(models, 2); }
    else assert.throws(() => qualifySessionFsLoom(run), /requires a nonzero/);
  }
  assertGateStep(["--session-fs"]);
});

test("hook qualification requires every retained checkout and projection model", () => {
  for (const [qualify, target, count] of [[qualifyHookLifecycleLoom, "acyclic_harness", 10]]) {
    for (const missing of [null, ...Array.from({ length: count }, (_, i) => i + 1)]) {
      let models = 0;
      const run = (command, args, options) => {
        if (command === "cargo") {
          assert.match(options.env.RUSTC_WORKSPACE_WRAPPER, /loom-rustc\.(?:sh|cmd)$/);
          assert.match(options.env.RUSTC_WORKSPACE_WRAPPER, /loom-rustc\.(?:sh|cmd)$/);
          return { status: 0, stderr: "", stdout: JSON.stringify({ ...artifact, target: { name: target } }) };
        }
        assert.match(args[0], /(?:cancelled_bash_retains_projection|local::workspace::projection::tests::)/);
        assert.equal(args[1], "--exact");
        assert.equal(options.timeout, 9500);
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
    if (command === "cargo") assert.match(options.env.RUSTC_WORKSPACE_WRAPPER, /loom-rustc\.(?:sh|cmd)$/);
    calls.push([command, args]);
    return { status: 0, stderr: "", stdout: calls.length === 1 ? JSON.stringify(artifact) : summary(6) };
  });
  assert.deepEqual(calls, [
    ["cargo", ["rustc", "--locked", "--message-format=json", "-p", "acyclic-harness-core", "--test", "mcp-loom", "--all-features", "--features", "model-checking", "--features", "model-checking"]],
    ["model-binary", []],
  ]);
  const qualification = readFileSync("scripts/qualify-ci.sh", "utf8");
  assertGateStep(["--all-features"]);
  assertGateStep(["--timer"]);
  assertGateStep(["--guest"]);
  const policy = qualification.match(/  policy\)\n([\s\S]*?);;/)?.[1];
  assert.match(policy, /node --test[^\n]*scripts\/qualify-mcp-loom\.test\.mjs/);
});

test("timer qualification instruments production Stream locks through the workspace wrapper", () => {
  const calls = [];
  qualifyTimerLoom((command, args, options) => {
    calls.push([command, args, options.env?.RUSTC_WORKSPACE_WRAPPER]);
    const timer = { ...artifact, target: { name: "main" } };
    return { status: 0, stderr: "", stdout: command === "cargo" ? JSON.stringify(timer) : summary(2, 0, 4) };
  });
  assert.deepEqual(calls, [
    ["cargo", ["rustc", "--locked", "--message-format=json", "-p", "acyclic-actors", "--test", "main", "--features", "acyclic-stream/local", "--target-dir", resolve(process.env.CARGO_TARGET_DIR || "target", "timer-loom"), "--features", "model-checking"], loomEnvironment().RUSTC_WORKSPACE_WRAPPER],
    ["model-binary", ["timer_loom::"], undefined],
  ]);
});

test("guest qualification selects every guest model in the instrumented main target", () => {
  const calls = [];
  qualifyGuestLoom((command, args, options) => {
    calls.push([command, args, options.env?.RUSTC_WORKSPACE_WRAPPER]);
    const guest = { ...artifact, target: { name: "main" } };
    return { status: 0, stderr: "", stdout: command === "cargo" ? JSON.stringify(guest) : summary(4, 0, 2) };
  });
  assert.deepEqual(calls, [
    ["cargo", ["rustc", "--locked", "--message-format=json", "-p", "acyclic-actors", "--test", "main", "--features", "model-checking"], loomEnvironment().RUSTC_WORKSPACE_WRAPPER],
    ["model-binary", ["guest_loom::"], undefined],
  ]);
});

test("qualification rejects zero, skipped, filtered and failed model execution", () => {
  for (const [status, output] of [[0, summary(0)], [0, summary(5, 1)], [0, summary(5, 0, 1)], [1, summary(6)]]) {
    assert.throws(() => qualifyMcpLoom(undefined, command => ({
      status: command === "cargo" ? 0 : status,
      stdout: command === "cargo" ? JSON.stringify(artifact) : output,
      stderr: "",
    })), /MCP Loom (qualification requires|tests failed)/);
  }
});

test("qualification rejects failed builds and missing or ambiguous artifacts", () => {
  for (const [status, stdout] of [[1, ""], [0, ""], [0, `${JSON.stringify(artifact)}\n${JSON.stringify(artifact)}`]]) {
    assert.throws(() => qualifyMcpLoom(undefined, () => ({ status, stdout, stderr: "" })),
      /MCP Loom build (failed|must produce)/);
  }
});


test("filesystem qualification models the production mutex only in its dedicated target", () => {
  const calls = [];
  qualifyFsBindingLoom(["--all-features"], (command, args) => {
    calls.push([command, args]);
    const fs = { ...artifact, target: { name: "native_binding_model" } };
    return { status: 0, stderr: "", stdout: command === "cargo" ? JSON.stringify(fs) : summary(3) };
  });
  assert.deepEqual(calls, [
    ["cargo", ["rustc", "--locked", "--message-format=json", "-p", "acyclic-fs", "--test", "native_binding_model", "--all-features", "--features", "model-checking", "--", "--cfg", "loom"]],
    ["model-binary", []],
  ]);
  assertGateStep(["--fs-binding", "--all-features"]);
});

test("registry gate instruments real native locks and rejects either omitted proof", () => {
  for (const missing of [null, 1, 2]) {
    let models = 0;
    const run = (command, args, options) => {
      if (command === "cargo") {
        assert.match(options.env.RUSTC_WORKSPACE_WRAPPER, /loom-rustc\.(?:sh|cmd)$/);
        assert.ok(args.includes("--lib"));
        return { status: 0, stderr: "", stdout: JSON.stringify({ ...artifact, target: { name: "acyclic_fs" } }) };
      }
      assert.match(args[0], /native_mount::fence::models::/);
      assert.equal(args[1], "--exact");
      assert.equal(options.timeout, 9500);
      models++;
      return { status: 0, stderr: "", stdout: summary(models === missing ? 0 : 1, 0, 100) };
    };
    if (missing === null) { qualifyNativeRegistryLoom(run); assert.equal(models, 2); }
    else assert.throws(() => qualifyNativeRegistryLoom(run), /requires a nonzero/);
  }
  assertGateStep(["--native-registry"]);
});
