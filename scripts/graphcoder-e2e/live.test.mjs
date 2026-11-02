import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Processes } from "./process.mjs";
import { requests } from "./assertions.mjs";
const captured = JSON.parse(readFileSync(new URL("./fixtures/shipping-output.json", import.meta.url), "utf8")).captures;
import { answer, requireTools } from "./live.mjs";

test("live oracle uses the shipping text and committed JSONL contracts", () => {
  const result = stdout => ({ code: 0, signal: null, stdout, stderr: "" });
  assert.equal(answer(captured["text-responses"], "text"), "hé λ 🙂 **streamed answer**\n");
  const message = { id: "e2e", inbox: 1, text: "done" };
  assert.deepEqual(answer(captured["jsonl-reopen"], "jsonl"), message);
  for (const output of ["", "answer", "{}", JSON.stringify({ ...message, error: "failed" }),
    JSON.stringify({ ok: { text: "obsolete ExecutionResult" } }),
    JSON.stringify(message) + "\n" + JSON.stringify(message)]) {
    assert.throws(() => answer(result(output), "jsonl"));
  }
  assert.throws(() => answer({ ...result("answer"), code: 1, stderr: "startup failed" }, "text"), /startup failed/);
  assert.throws(() => answer({ ...result("answer"), signal: "SIGKILL" }, "text"), /interrupted/);
});

test("model prose and pre-tool events cannot prove a completed native tool", () => {
  assert.throws(() => requireTools([], ["shell"]));
  assert.throws(() => requireTools([{ hook_event_name: "PreToolUse", tool_name: "shell" }], ["shell"]));
  // SDK hook envelopes are internal contract controls, not external captures.
  requireTools([{ hook_event_name: "PostToolUse", tool_name: "shell", tool_response: { text: "done" } }], ["shell"]);
  assert.throws(() => requireTools([{ hook_event_name: "PostToolUse", tool_name: "shell" }], ["shell"]), /admit completed shell result/);
  assert.throws(() => requireTools([{ hook_event_name: "PostToolUse", tool_name: "shell", tool_response: null }], ["shell"]), /admit completed shell result/);
  for (const tool_response of [{ error: "denied" }, { error: "" }, { error: false },
    "tool executor rejected the admitted call (conflict): workspace generation changed"]) {
    assert.throws(() => requireTools([{ hook_event_name: "PostToolUse", tool_name: "shell", tool_response }], ["shell"]), /unexpected tool error/);
  }
  // Even a later successful call cannot erase an earlier failure or a failed
  // auxiliary tool that the required-tool list did not mention.
  assert.throws(() => requireTools([
    { hook_event_name: "PostToolUse", tool_name: "fs", tool_response: { error: "merge failed" } },
    { hook_event_name: "PostToolUse", tool_name: "shell", tool_response: { text: "done" } },
  ], ["shell"]), /merge failed/);
  assert.throws(() => requireTools([{ hook_event_name: "PostToolUse", tool_name: "spawn" }], ["spawn", "wait"]));
});

test("collaboration requires the spawned child's exact settled public result", () => {
  // Generated SDK hook contract controls, not captured provider output.
  const child = "agents/project/worker", expected = "child result";
  const hooks = () => [
    { hook_event_name: "PostToolUse", tool_name: "spawn", tool_response: { agent: child } },
    { hook_event_name: "PostToolUse", tool_name: "wait", tool_input: { agents: [child] },
      tool_response: { settled: [{ agent: child, phase: "idle", answer: expected }], running: [] } },
  ];
  requireTools(hooks(), ["spawn", "wait"], expected);
  for (const mutate of [
    events => events.pop(),
    events => { events[1].hook_event_name = "PreToolUse"; },
    events => { events[1].tool_input.agents = ["agents/project/other"]; },
    events => { events[1].tool_response.settled[0].agent = "agents/project/other"; },
    events => { events[1].tool_response.running = [child]; },
    events => { events[1].tool_response.settled = []; },
    events => { events[1].tool_response.settled[0].phase = "failed"; },
    events => { events[1].tool_response.settled[0].answer = "parent's invented result"; },
    events => { events[1].tool_response.error = "wait failed"; },
  ]) {
    const events = hooks();
    mutate(events);
    assert.throws(() => requireTools(events, ["spawn", "wait"], expected));
  }
});

test("a real command's published bytes cannot conceal its failed shell status", async () => {
  const root = await mkdtemp(join(tmpdir(), "live-shell-result-")), processes = new Processes();
  try {
    const path = join(root, "effect.txt");
    const result = await processes.run(process.execPath,
      ["-e", "require('node:fs').writeFileSync(process.argv[1], 'EXPECTED_BYTES'); process.exitCode = 17", path]);
    assert.equal(result.code, 17, result.stderr);
    assert.equal(await readFile(path, "utf8"), "EXPECTED_BYTES");
    // Original OS completion supplies these SDK ToolResult fields; the two
    // mutations require each status field to reject failure independently.
    const status = { success: result.code === 0, exit_code: result.code, stop: null };
    const success = { success: true, exit_code: 0, stop: null };
    const hook = tool_response => ({ hook_event_name: "PostToolUse", tool_name: "shell", tool_response });
    const fixture = { id: "shell-status", model: { steps: [{}] }, assert: { requests: 1 } };
    const record = value => [{ path: "responses", step: 0, status: 200,
      body: { input: [{ type: "function_call_output", output: JSON.stringify(value) }] } }];
    requireTools([hook(success)], ["shell"]);
    requests(fixture, record(success));
    for (const value of [status, { ...status, success: true }, { ...status, exit_code: 0 }]) {
      assert.throws(() => requireTools([hook(value), hook(success)], ["shell"]), /unexpected tool error/);
      assert.throws(() => requests(fixture, record(value)), /unexpected tool error/);
    }
    assert.throws(() => requireTools([hook(status), { hook_event_name: "PostToolUse",
      tool_name: "spawn", tool_response: { agent: "agents/worker" } }], ["spawn"]), /unexpected tool error/);
    requests({ ...fixture, assert: { requests: 1, tool_errors: ["exit_code=17"] } }, record(status));
  } finally {
    await processes.close();
    await rm(root, { recursive: true, force: true });
  }
});
