import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
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
  requireTools([{ hook_event_name: "PostToolUse", tool_name: "shell" }], ["shell"]);
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
  ]) {
    const events = hooks();
    mutate(events);
    assert.throws(() => requireTools(events, ["spawn", "wait"], expected));
  }
});
