import assert from "node:assert/strict";
import test, { after } from "node:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { recordedRequests, requests, subset, toolResults } from "./assertions.mjs";
import { requireTools } from "./live.mjs";

const rejectedWait = JSON.parse(readFileSync(new URL("./fixtures/rejected-wait.json", import.meta.url), "utf8"));

test("captured argument refusal cannot qualify a successful child conversation", () => {
  const fixture = { id: rejectedWait.case, assert: { requests: 1, steps: [rejectedWait.record.step] } };
  const records = [rejectedWait.record];
  const output = toolResults(records)[0];
  assert.throws(() => requests(fixture, records), /unexpected tool error/);
  // The live driver shares the same refusal admission after hook projection.
  assert.throws(() => requireTools([
    { hook_event_name: "PostToolUse", tool_name: "wait", tool_response: output },
  ], ["wait"]), /unexpected tool error/);
  requests({ ...fixture, assert: { ...fixture.assert, tool_errors: [output] } }, records);
  assert.throws(() => requests({ ...fixture, assert: { ...fixture.assert, tool_errors: ["another refusal"] } }, records), /unexpected tool error/);
});

const directory = mkdtempSync(join(tmpdir(), "e2e-records-"));
after(() => rmSync(directory, { recursive: true }));
let files = 0;
// The records file appears only while the history is read, as when the mock
// appends a record and then publishes its event: reading the file before the
// history fails every test here.
const recorded = (text, events) => {
  const path = join(directory, `${files++}.jsonl`);
  return recordedRequests({ history: async match => { writeFileSync(path, text); return events.filter(match); } }, path);
};

test("only flushed acknowledged records survive interruption, with all case assertions retained", async () => {
  const record = { sequence: 1, case: "a", step: 0, path: "responses", status: 200, body: {} };
  const event = { event: "request", sequence: 1, case: "a", step: 0 };
  for (const tail of ["", "{", JSON.stringify({ ...record, sequence: 2 })]) {
    const parsed = await recorded(JSON.stringify(record) + "\n" + tail, [event]);
    assert.deepEqual(parsed, [record]);
    requests({ assert: { requests: 1 }, model: { steps: [{}] } }, parsed);
    assert.throws(() => requests({ assert: { requests: 2 }, model: { steps: [{}, {}] } }, parsed), /generation count/);
  }
});
test("acknowledged corruption and missing records always fail", async () => {
  const event = { event: "request", sequence: 1, case: "a", step: 0 };
  await assert.rejects(recorded("{", [event]), /complete JSONL/);
  await assert.rejects(recorded("{\n", [event]), SyntaxError);
  await assert.rejects(recorded('{"sequence":2}\n', [event]), /sequence/);
});

test("request assertions reject omitted, duplicated, unmatched and fabricated results", () => {
  const fixture = { model: { steps: [{}, {}] }, assert: { requests: 2, tool_contains: ["REAL_RESULT"] } };
  const record = (step, input) => ({ path: "responses", step, status: 200, body: { input } });
  const valid = [record(0, [{ role: "user", content: "hello" }]), record(1, [{ type: "function_call_output", output: '{"text":"REAL_RESULT"}' }])];
  requests(fixture, valid);
  requests({ ...fixture, assert: { ...fixture.assert, http: [{ path: "responses", body: { input: valid[0].body.input } }] } }, valid);
  assert.throws(() => requests({ ...fixture, assert: { ...fixture.assert, http: [{ path: "search", body: { query: "real query" } }] } }, valid), /missing HTTP request/);
  assert.throws(() => requests(fixture, valid.slice(1)), /generation count/);
  assert.throws(() => requests(fixture, [valid[0], valid[0]]), /exactly once/);
  assert.throws(() => requests(fixture, [valid[0], { ...valid[1], status: 409 }]), /unmatched/);
  assert.throws(() => requests(fixture, [valid[0], record(1, [{ role: "user", content: "REAL_RESULT" }])]), /actual tool result missing/);
  assert.throws(() => requests({ ...fixture, assert: { requests: 2 } }, [valid[0], record(1, [{ type: "function_call_output", output: '{"error":"failed"}' }])]), /unexpected tool error/);
  const gemini = [{ path: "models/tiny:streamGenerateContent", step: 0, status: 200,
    body: { contents: [{ role: "user", parts: [{ functionResponse: { name: "bash", response: { text: "REAL_RESULT" } } }] }] } }];
  requests({ model: { steps: [{}] }, assert: { requests: 1, tool_contains: ["REAL_RESULT"] } }, gemini);
  assert.throws(() => requests({ model: { steps: [{}] }, assert: { requests: 1, tool_contains: ["FORGED"] } }, gemini), /actual tool result missing/);
});

test("auxiliary HTTP schemas cannot enter provider tool-result projection", async () => {
  const fixture = { model: { steps: [{}] }, assert: { requests: 1, tool_contains: ["REAL_RESULT"] } };
  const generation = { path: "responses", step: 0, status: 200,
    body: { input: [{ type: "function_call_output", output: '{"text":"REAL_RESULT"}' }] } };
  const auxiliary = [
    { path: "search", step: null, status: 200, body: { input: { query: "fixture query" } } },
    { path: "fetch", step: null, status: 200,
      body: { input: [{ type: "function_call_output", output: '{"text":"FORGED_RESULT"}' }] } },
  ];
  requests(fixture, [generation, ...auxiliary]);
  assert.throws(() => requests({ ...fixture, assert: { requests: 1, tool_contains: ["FORGED_RESULT"] } },
    [generation, ...auxiliary]), /actual tool result missing/);
  const source = readFileSync(new URL("./assertions.mjs", import.meta.url), "utf8");
  const boundary = "toolResults(generations).map(";
  assert.equal(source.split(boundary).length, 2);
  const former = await import("data:text/javascript;base64," +
    Buffer.from(source.replace(boundary, "toolResults(records).map(")).toString("base64"));
  assert.throws(() => former.requests(fixture, [generation, ...auxiliary]), TypeError);
  const forged = { ...fixture, assert: { requests: 1, tool_contains: ["FORGED_RESULT"] } };
  former.requests(forged, [generation, auxiliary[1]]);
});

test("native result projection and nested JSONL assertions retain identity", () => {
  assert.deepEqual(toolResults([
    { body: { input: [{ type: "function_call_output", output: "responses" }] } },
    { body: { messages: [{ role: "tool", content: "chat" }] } },
    { body: { messages: [{ role: "user", content: [{ type: "tool_result", content: "anthropic" }] }] } },
  ]), ["responses", "chat", "anthropic"]);
  const expected = { method: "tools/call", params: { name: "echo", arguments: { text: "λ\n\"" } } };
  assert.ok(subset({ ...expected, id: 1 }, expected));
  assert.ok(!subset({ ...expected, params: { ...expected.params, name: "wrong" } }, expected));
});
