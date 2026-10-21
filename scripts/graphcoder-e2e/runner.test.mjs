import assert from "node:assert/strict";
import test from "node:test";
import { PassThrough } from "node:stream";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import { Events, Processes } from "./process.mjs";
import { requests, subset, toolResults } from "./assertions.mjs";
import { matches } from "./pty.mjs";
import { Terminal } from "../graphcoder-pty/screen.mjs";

test("case selection rejects misspellings before starting either executable", async () => {
  const processes = new Processes();
  try {
    const child = processes.start(process.execPath, [fileURLToPath(new URL("./run.mjs", import.meta.url)), "missing-binary", "missing-mock", "text-responses", "misspelled-case"]);
    const result = await child.closed;
    assert.equal(result.code, 1);
    assert.match(result.stderr, /unknown case misspelled-case/);
    assert.doesNotMatch(result.stderr, /ENOENT/);
  } finally { await processes.close(); }
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

test("ready and recorded-request waits handle every fragmentation boundary without timers", async () => {
  const bytes = JSON.stringify({ event: "request", case: "λ", step: 0 }) + "\n";
  for (let boundary = 0; boundary <= bytes.length; boundary++) {
    const stream = new PassThrough();
    const events = new Events(stream, new Promise(() => {}));
    const awaited = events.wait(event => event.case === "λ");
    stream.write(bytes.slice(0, boundary)); stream.write(bytes.slice(boundary));
    assert.equal((await awaited).step, 0);
    assert.equal((await events.wait(event => event.step === 0)).case, "λ");
    stream.end();
  }
  const stream = new PassThrough();
  const events = new Events(stream, new Promise(() => {}));
  const failed = assert.rejects(events.wait(() => true), SyntaxError);
  stream.end("broken JSON\n");
  await failed;
});

test("screen conditions require rendered styles and reject literal markdown", async () => {
  const screen = new Terminal({ cols: 80, rows: 24, allowProposedApi: true });
  const write = data => new Promise(resolve => screen.write(data, resolve));
  await write("**answer**");
  assert.ok(!matches(screen, "", { bold: "answer", absent: ["**answer**"] }));
  await write("\r\x1b[2K\x1b[1manswer\x1b[0m");
  assert.ok(matches(screen, "\x07", { contains: ["answer"], bold: "answer", absent: ["**answer**"], raw: ["\x07"] }));
  assert.ok(!matches(screen, "", { contains: ["answer"], raw: ["\x07"] }));
  await write("\r\x1b[2K\u25cb GraphCoder");
  assert.ok(matches(screen, "\x1b[?25l\x1b[?25h", { contains: ["\u25cb GraphCoder"], cursor: true }));
  assert.ok(!matches(screen, "\x1b[?25h\x1b[?25l", { contains: ["\u25cb GraphCoder"], cursor: true }));
  assert.ok(!matches(screen, "\x1b[?25h", { contains: ["\u00e2\u2014\u2039"] }));
  screen.dispose();
});

test("real children are killed and reaped on scope cleanup", { timeout: 5000 }, async () => {
  const processes = new Processes();
  const running = processes.start(process.execPath, ["-e", "process.stdout.write(JSON.stringify({event:'ready'})+'\\n');process.stdin.resume()"]);
  const events = new Events(running.child.stdout, running.closed);
  await events.wait(event => event.event === "ready");
  await processes.close();
  assert.equal(processes.children.size, 0);
  assert.throws(() => processes.start(process.execPath, []), /scope is closed/);
});

test("real process captures preserve UTF-8 at every byte boundary", { timeout: 5000 }, async () => {
  const processes = new Processes();
  const child = processes.start(process.execPath, ["-e", "require('node:readline').createInterface({input:process.stdin}).on('line',line=>process.stdout.write(Buffer.from(line,'hex')))"]);
  const text = "hé λ 🙂", bytes = Buffer.from(text);
  try {
    for (let cut = 0; cut <= bytes.length; cut++) for (const fragment of [bytes.subarray(0, cut), bytes.subarray(cut)]) {
      if (!fragment.length) continue;
      const received = once(child.child.stdout, "data");
      child.child.stdin.write(fragment.toString("hex") + "\n");
      await received;
    }
    child.child.stdin.end();
    assert.equal((await child.closed).stdout, text.repeat(bytes.length + 1));
  } finally { await processes.close(); }
});
