import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import test from "node:test";
import { qualifyMcpLoom, qualifyTimerLoom, qualifyGuestLoom, qualifyFsBindingLoom, qualifyMemoryAccountingLoom, qualifyCoordinatorOriginsLoom, qualifySummaryCheckpointsLoom, qualifyHookLifecycleLoom } from "./qualify-mcp-loom.mjs";

const artifact = { reason: "compiler-artifact", target: { name: "mcp-loom" }, profile: { test: true }, executable: "model-binary" };
const summary = (passed, ignored = 0, filtered = 0) => `test result: ok. ${passed} passed; 0 failed; ${ignored} ignored; 0 measured; ${filtered} filtered out;\n`;

test("hook qualification builds real synchronization once and rejects either missing model", () => {
  for (const missing of [null, 1, 2]) {
    let models = 0;
    const run = (command, args, options) => {
      if (command === "cargo") {
        assert.match(options.env.RUSTFLAGS, /--cfg acyclic_harness_loom/);
        assert.match(options.env.RUSTFLAGS, /--cfg acyclic_loom/);
        return { status: 0, stderr: "", stdout: JSON.stringify({ ...artifact, target: { name: "acyclic_harness" } }) };
      }
      assert.ok(args[0].includes(models === 0 ? "competing_dispatches_claim_once" : "cancellation_keeps_hook_workers"));
      assert.equal(args[1], "--exact");
      assert.equal(options.timeout, 9500);
      models++;
      return { status: 0, stderr: "", stdout: summary(models === missing ? 0 : 1, 0, 100) };
    };
    if (missing === null) { qualifyHookLifecycleLoom(undefined, run); assert.equal(models, 2); }
    else assert.throws(() => qualifyHookLifecycleLoom(undefined, run), /requires a nonzero/);
  }
  const gate = readFileSync("scripts/qualify-ci.sh", "utf8").match(/  gate\)\n([\s\S]*?);;/)?.[1];
  assert.match(gate, /node scripts\/qualify-mcp-loom\.mjs --hook-lifecycle/);
});

test("qualification opts into the crate cfg only for the model build and executes its binary", () => {
  const calls = [];
  qualifyMcpLoom(["--all-features"], (command, args, options) => {
    if (command === "cargo") assert.match(options.env.RUSTFLAGS, /--cfg acyclic_harness_loom(?: |$)/);
    calls.push([command, args]);
    return { status: 0, stderr: "", stdout: calls.length === 1 ? JSON.stringify(artifact) : summary(6) };
  });
  assert.deepEqual(calls, [
    ["cargo", ["rustc", "--locked", "--message-format=json", "-p", "acyclic-harness", "--test", "mcp-loom", "--all-features", "--features", "model-checking"]],
    ["model-binary", []],
  ]);
  const qualification = readFileSync("scripts/qualify-ci.sh", "utf8");
  const gate = qualification.match(/  gate\)\n([\s\S]*?);;/)?.[1];
  assert.match(gate, /node scripts\/qualify-mcp-loom\.mjs --all-features/);
  assert.match(gate, /node scripts\/qualify-mcp-loom\.mjs --timer/);
  assert.match(gate, /node scripts\/qualify-mcp-loom\.mjs --guest/);
  const policy = qualification.match(/  policy\)\n([\s\S]*?);;/)?.[1];
  assert.match(policy, /node --test[^\n]*scripts\/qualify-mcp-loom\.test\.mjs/);
});

test("timer qualification instruments production Stream locks only through RUSTFLAGS", () => {
  const calls = [];
  qualifyTimerLoom((command, args, options) => {
    calls.push([command, args, options.env?.RUSTFLAGS]);
    const timer = { ...artifact, target: { name: "timer_loom" } };
    return { status: 0, stderr: "", stdout: command === "cargo" ? JSON.stringify(timer) : summary(1) };
  });
  assert.deepEqual(calls, [
    ["cargo", ["rustc", "--locked", "--message-format=json", "-p", "acyclic-actors", "--test", "timer_loom", "--features", "acyclic-stream/local", "--target-dir", resolve(process.env.CARGO_TARGET_DIR || "target", "timer-loom")], "--cfg acyclic_loom"],
    ["model-binary", [], undefined],
  ]);
});

test("guest qualification instruments only the guest target and runs its models", () => {
  const calls = [];
  qualifyGuestLoom((command, args) => {
    calls.push([command, args]);
    const guest = { ...artifact, target: { name: "guest_loom" } };
    return { status: 0, stderr: "", stdout: command === "cargo" ? JSON.stringify(guest) : summary(4) };
  });
  assert.deepEqual(calls, [
    ["cargo", ["rustc", "--locked", "--message-format=json", "-p", "acyclic-actors", "--test", "guest_loom", "--", "--cfg", "loom"]],
    ["model-binary", []],
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
    ["cargo", ["rustc", "--locked", "--message-format=json", "-p", "acyclic-fs", "--test", "native_binding_model", "--all-features", "--", "--cfg", "acyclic_fs_loom"]],
    ["model-binary", []],
  ]);
  const gate = readFileSync("scripts/qualify-ci.sh", "utf8").match(/  gate\)\n([\s\S]*?);;/)?.[1];
  assert.match(gate, /node scripts\/qualify-mcp-loom\.mjs --fs-binding --all-features/);
  assert.match(gate, /node scripts\/qualify-mcp-loom\.mjs --memory-accounting/);
});

test("memory accounting instruments dependency cancellation and the real ledger mutex", () => {
  const calls = [];
  qualifyMemoryAccountingLoom((command, args, options) => {
    calls.push([command, args]);
    if (command === "cargo") {
      assert.match(options.env.RUSTFLAGS, /--cfg acyclic_fs_loom(?: |$)/);
      assert.doesNotMatch(options.env.RUSTFLAGS, /acyclic_harness_loom/);
    }
    const memory = { ...artifact, target: { name: "memory-accounting-model" } };
    return { status: 0, stderr: "", stdout: command === "cargo" ? JSON.stringify(memory) : summary(6) };
  });
  assert.deepEqual(calls, [
    ["cargo", ["rustc", "--locked", "--message-format=json", "-p", "acyclic-harness", "--test", "memory-accounting-model",
      "--features", "filesystem,acyclic-fs/model-checking", "--target-dir", resolve(process.env.CARGO_TARGET_DIR || "target", "memory-accounting-loom"),
      "--", "--cfg", "acyclic_harness_loom"]],
    ["model-binary", []],
  ]);
});

test("coordinator qualification instruments real Stream locks and rejects skipped models", () => {
  for (const passed of [0, 1]) {
    const calls = [];
    const run = (command, args, options) => {
      calls.push([command, args]);
      if (command === "cargo") {
        assert.match(options.env.RUSTFLAGS, /--cfg acyclic_loom/);
        assert.ok(args.includes("--lib"));
      }
      return { status: 0, stderr: "", stdout: command === "cargo"
        ? JSON.stringify({ ...artifact, target: { name: "acyclic_harness" } })
        : summary(passed, 0, 100) };
    };
    if (passed) qualifyCoordinatorOriginsLoom(undefined, run);
    else assert.throws(() => qualifyCoordinatorOriginsLoom(undefined, run), /requires a nonzero/);
    assert.deepEqual(calls[1][1], ["distributed::tests::model_checking::origin_publication_fences_competing_writers", "--exact"]);
  }
});

test("summary checkpoint qualification instruments real locks and requires both race models and controls", () => {
  for (const passed of [0, 1, 2, 3, 4]) {
    const calls = [];
    const run = (command, args, options) => {
      calls.push([command, args]);
      if (command !== "cargo") assert.equal(options.timeout, 9500);
      if (command === "cargo") assert.match(options.env.RUSTFLAGS, /--cfg acyclic_loom(?: |$)/);
      return { status: 0, stderr: "", stdout: command === "cargo"
        ? JSON.stringify({ ...artifact, target: { name: "acyclic_harness" } }) : summary(passed, 0, 100) };
    };
    if (passed === 4) qualifySummaryCheckpointsLoom(undefined, run);
    else assert.throws(() => qualifySummaryCheckpointsLoom(undefined, run), /requires a nonzero/);
    assert.deepEqual(calls[1], ["model-binary", ["local::compact::checkpoints::model::"]]);
  }
  assert.match(readFileSync("scripts/qualify-ci.sh", "utf8"), /node scripts\/qualify-mcp-loom\.mjs --summary-checkpoints --all-features/);
});
