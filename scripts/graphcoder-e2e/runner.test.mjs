import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import { PassThrough } from "node:stream";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import { Events, Processes } from "./process.mjs";
import { requests, subset, toolResults } from "./assertions.mjs";
import { matches } from "./pty.mjs";
import { Terminal } from "../graphcoder-pty/screen.mjs";
import { TerminalFrames, FailureHistory } from "./frames.mjs";

test("screen assertions observe complete synchronized frames at every fragmentation boundary", async () => {
  const bytes = "\x1b[?2026h\x1b[Hdraft\x1b[Hfinal\x1b[?2026l";
  for (let cut = 0; cut <= bytes.length; cut++) {
    const screen = new Terminal({ cols: 80, rows: 24, allowProposedApi: true });
    const frames = new TerminalFrames(screen), seen = [];
    const capture = () => seen.push(screen.buffer.active.getLine(0).translateToString(true));
    await frames.write(bytes.slice(0, cut), capture);
    await frames.write(bytes.slice(cut), capture);
    assert.ok(seen.includes("final"));
    assert.ok(seen.every(text => text === "" || text === "final"), JSON.stringify(seen));
    frames.dispose(); screen.dispose();
  }
  // Negative control: callbacks after arbitrary writes disclose the half-painted draft.
  const screen = new Terminal({ cols: 80, rows: 24, allowProposedApi: true });
  await new Promise(resolve => screen.write(bytes.slice(0, bytes.indexOf("\x1b[Hfinal")), resolve));
  assert.equal(screen.buffer.active.getLine(0).translateToString(true), "draft");
  screen.dispose();
});

test("handled failures retain exact task and reason identity across subsequent actions", () => {
  const history = new FailureHistory();
  const old = "task 00000000-0000-0000-0000-000000000001 failed: cancelled";
  const next = old.replace("0001 failed", "0002 failed");
  assert.equal(history.unexpected(old, {}), old, "old stateless guard rejects the handled result again");
  history.accept(old, { task_failure: "cancelled" });
  assert.equal(history.unexpected(old, {}), undefined);
  assert.equal(history.unexpected(next, {}), next, "another task's failure must still fail immediately");
  const changed = old.replace("cancelled", "unauthorized");
  assert.equal(history.unexpected(changed, {}), changed, "another reason is not an accepted replay");
});

test("owner exit permits recovery before a live descendant releases inherited pipes", async () => {
  const processes = new Processes();
  let descendant;
  try {
    const leaf = "require('node:net').createServer().listen(0,'127.0.0.1',()=>console.log(JSON.stringify({pid:process.pid})))";
    const parent = `const {spawn}=require('node:child_process');spawn(process.execPath,['-e',${JSON.stringify(leaf)}],{stdio:'inherit',detached:true,windowsHide:true})`;
    const owner = processes.start(process.execPath, ["-e", parent]);
    descendant = (await new Events(owner.child.stdout, owner.closed).wait(event => event.pid)).pid;
    let pipesClosed = false;
    owner.closed.then(() => { pipesClosed = true; });
    owner.child.kill("SIGKILL");
    const exit = await owner.exited;
    assert.ok(exit.signal || exit.code !== 0);
    process.kill(descendant, 0);
    assert.equal(pipesClosed, false, "negative control: waiting for close would prevent recovery");
    process.kill(descendant, "SIGKILL");
    await owner.closed;
    descendant = undefined;
  } finally {
    if (descendant) { try { process.kill(descendant, "SIGKILL"); } catch (error) { if (error.code !== "ESRCH") throw error; } }
    await processes.close();
  }
});

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
  const rejected = record(1, [{ type: "function_call_output", output: "tool executor rejected the admitted call (conflict): workspace generation changed" }]);
  assert.throws(() => requests({ ...fixture, assert: { requests: 2 } }, [valid[0], rejected]), /unexpected tool error/);
  requests({ ...fixture, assert: { requests: 2, tool_errors: ["workspace generation changed"] } }, [valid[0], rejected]);
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

test("expected interrupted recovery still requires its retained transcript and exact failure", async () => {
  const screen = new Terminal({ cols: 80, rows: 24, allowProposedApi: true });
  const write = data => new Promise(resolve => screen.write(data, resolve));
  const expected = { task_failure: "model provider unavailable (co", contains: ["RECOVERED_E2E"] };
  const failure = "task 00000000-0000-0000-0000-000000000001 failed: ";
  await write("RECOVERED_E2E");
  assert.ok(!matches(screen, "", expected), "replayed prefix alone is not settled recovery");
  await write("\r\n" + failure + "unauthorized");
  assert.ok(!matches(screen, "", expected), "another error cannot satisfy interrupted recovery");
  await write("\r\x1b[2K" + failure + "model provider unavailable (connection interrupted)");
  assert.ok(matches(screen, "", expected));
  await write("\x1b[H\x1b[2K");
  assert.ok(!matches(screen, "", expected), "settlement must preserve the partial transcript");
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
      assert.ok(!child.stdout.includes("\uFFFD"), "live PTY capture must share the process streaming decoder");
    }
    child.child.stdin.end();
    assert.equal((await child.closed).stdout, text.repeat(bytes.length + 1));
    assert.equal(child.stdout, text.repeat(bytes.length + 1));
  } finally { await processes.close(); }
});
