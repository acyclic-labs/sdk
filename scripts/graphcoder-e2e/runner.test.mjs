import assert from "node:assert/strict";
import test from "node:test";
import { appendFileSync, close as fsClose, createWriteStream, mkdtempSync, open as fsOpen, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PassThrough, Writable } from "node:stream";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import { Spool, SpoolEnded, OutputWatch, Processes } from "./process.mjs";
import { requests, subset, toolResults } from "./assertions.mjs";
import { matches, SHOW_CURSOR, HIDE_CURSOR } from "./pty.mjs";
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
    descendant = (await new Spool(owner.child.stdout, owner.closed).next(event => event.pid)).pid;
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

test("spool waits handle every fragmentation boundary without timers or history", async () => {
  const bytes = JSON.stringify({ event: "request", case: "λ", step: 0 }) + "\n";
  for (let boundary = 0; boundary <= bytes.length; boundary++) {
    const stream = new PassThrough();
    const events = new Spool(stream, new Promise(() => {}), { key: event => [event.case] });
    const awaited = events.next(event => event.case === "λ");
    stream.write(bytes.slice(0, boundary)); stream.write(bytes.slice(boundary));
    assert.equal((await awaited).step, 0);
    // A keyed wait also answers from the index; `next` sees only later events.
    assert.equal((await events.keyed("λ")).step, 0);
    stream.end();
  }
});

test("a spool ends with the exact typed cause", async t => {
  const ended = async (events, feed) => { const waiting = events.next(() => false); feed(); return waiting.then(() => assert.fail("resolved"), error => error); };
  await t.test("malformed line", async () => {
    const stream = new PassThrough();
    const error = await ended(new Spool(stream, new Promise(() => {})), () => stream.end("broken JSON\n"));
    assert.ok(error instanceof SpoolEnded); assert.equal(error.kind, "malformed"); assert.match(error.message, /broken JSON/);
  });
  await t.test("source EOF carries the source result", async () => {
    const stream = new PassThrough();
    let close; const closed = new Promise(resolve => { close = resolve; });
    const error = await ended(new Spool(stream, closed), () => { stream.end(); close({ code: 3, signal: null }); });
    assert.equal(error.kind, "eof"); assert.equal(error.detail.code, 3);
  });
  await t.test("duplicate unique key", async () => {
    const stream = new PassThrough();
    const events = new Spool(stream, new Promise(() => {}), { key: event => [event.id], unique: true });
    const error = await ended(events, () => stream.write('{"id":1}\n{"id":1}\n'));
    assert.equal(error.kind, "duplicate");
    assert.equal(events.sequence, 2);
  });
  await t.test("early event: next never sees it, and EOF ends the wait exactly", async () => {
    const stream = new PassThrough();
    let close; const closed = new Promise(resolve => { close = resolve; });
    const events = new Spool(stream, closed);
    stream.write('{"ready":true}\n');
    await new Promise(resolve => setImmediate(resolve));
    const late = events.next(event => event.ready);
    stream.end(); close({ code: 0, signal: null });
    await assert.rejects(late, error => error.kind === "eof");
  });
  await t.test("transcript failure", async () => {
    const stream = new PassThrough();
    const error = await ended(new Spool(stream, new Promise(() => {}), { transcript: join(tmpdir(), "missing-directory-" + process.pid, "events.jsonl") }), () => {});
    assert.equal(error.kind, "spool");
  });
});

test("history reads only completed lines while a later write is in flight", async () => {
  const path = join(mkdtempSync(join(tmpdir(), "e2e-history-")), "events.jsonl");
  const stream = new PassThrough();
  const events = new Spool(stream, new Promise(() => {}), { transcript: path });
  const second = events.next(event => event.n === 2);
  stream.write('{"n":1}\n{"n":2}\n');
  await second;
  assert.deepEqual(await events.history(), [{ n: 1 }, { n: 2 }]);
  // A started write: part of a large line is already in the file.
  appendFileSync(path, '{"n":3,"pad":"' + "x".repeat(1 << 16));
  assert.deepEqual(await events.history(), [{ n: 1 }, { n: 2 }]);
  // Negative control: reading to EOF takes the partial line.
  assert.throws(() => readFileSync(path, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line)), SyntaxError);
});

test("a transcript write that fails or is lost ends history and waits with the spool cause, never a hang", async t => {
  const directory = mkdtempSync(join(tmpdir(), "e2e-history-"));
  // A file stream whose writes fail, or (the ignored-error control) report
  // success without writing anything.
  const stream = (name, write) => createWriteStream(join(directory, name), { fs: {
    open: fsOpen, close: fsClose,
    write: (_fd, buffer, _offset, length, _position, done) => write(length, done),
    writev: (_fd, buffers, _position, done) => write(buffers.reduce((total, buffer) => total + buffer.length, 0), done),
  } });
  await t.test("the write error is latched", async () => {
    const source = new PassThrough();
    const events = new Spool(source, new Promise(() => {}), { transcript: stream("failed.jsonl", (_length, done) => done(new Error("injected EIO"))) });
    const waiting = events.next(() => false);
    source.write('{"n":1}\n');
    await assert.rejects(waiting, error => error.kind === "spool" && /injected EIO/.test(error.message));
    await assert.rejects(events.history(), error => error.kind === "spool" && /injected EIO/.test(error.message));
    await assert.rejects(events.next(() => true), error => error.kind === "spool");
  });
  await t.test("negative control: an ignored error leaves a short file, read as a typed short read", async () => {
    const source = new PassThrough();
    const events = new Spool(source, new Promise(() => {}), { transcript: stream("lost.jsonl", (length, done) => done(null, length)) });
    const first = events.next(event => event.n === 1);
    source.write('{"n":1}\n');
    await first;
    await assert.rejects(events.history(), error => error.kind === "spool" && /ends at byte 0 of 8/.test(error.message));
  });
});

test("a write that fails with a chunk queued rejects the pending waiter; the queued chunk is dropped", async () => {
  const directory = mkdtempSync(join(tmpdir(), "e2e-history-"));
  // A one-byte buffer holds the first chunk for drain; its write then fails.
  const transcript = createWriteStream(join(directory, "failed.jsonl"), { highWaterMark: 1, fs: {
    open: fsOpen, close: fsClose,
    write: (_fd, _buffer, _offset, _length, _position, done) => setImmediate(() => done(new Error("injected EIO"))),
  } });
  const source = new PassThrough();
  const events = new Spool(source, new Promise(() => {}), { transcript });
  const second = events.next(event => event.n === 2);
  source.write('{"n":1}\n');
  source.write('{"n":2}\n');
  await assert.rejects(second, error => error.kind === "spool" && /injected EIO/.test(error.message));
  assert.equal(events.sequence, 1, "the queued chunk was dropped after the transcript failed");
});

test("a source closed without ending still ends the spool and its transcript", async () => {
  const source = new PassThrough();
  let close; const closed = new Promise(resolve => { close = resolve; });
  const events = new Spool(source, closed, { transcript: join(mkdtempSync(join(tmpdir(), "e2e-history-")), "events.jsonl") });
  const waiting = events.next(() => false);
  source.destroy(); close({ code: 0, signal: null });
  await assert.rejects(waiting, error => error.kind === "eof");
});

test("a transcript failure while the child runs ends it with the spool cause and reaps the child", async () => {
  const processes = new Processes({ transcripts: join(tmpdir(), `missing-transcripts-${process.pid}`, "nested") });
  // The child would run until its stdin ends; nothing here ends it.
  const running = processes.start(process.execPath, ["-e", "process.stdin.resume()"]);
  await assert.rejects(running.closed, error => error instanceof SpoolEnded && error.kind === "spool" && error.detail.code === "ENOENT");
  await processes.close();
  assert.equal(processes.children.size, 0);
  assert.ok(running.child.exitCode !== null || running.child.signalCode !== null, "the child was reaped");
});

test("a slow transcript holds the source back, so queued bytes stay bounded by a chunk", async () => {
  const lines = 64, chunks = 200;
  const chunk = index => Array.from({ length: lines }, (_, line) => JSON.stringify({ n: index * lines + line, pad: "x".repeat(100) }) + "\n").join("");
  const size = chunk(chunks - 1).length;
  // The sink completes one write per turn. Negative control: a spool that
  // ignored write()'s return queues the whole burst in the sink.
  const peak = async honest => {
    let queued = 0;
    const sink = new Writable({ highWaterMark: 1024, write(_bytes, _encoding, done) { setImmediate(done); } });
    const write = sink.write.bind(sink);
    sink.write = (...args) => {
      const room = write(...args);
      queued = Math.max(queued, sink.writableLength);
      return honest ? room : true;
    };
    const source = new PassThrough();
    const events = new Spool(source, new Promise(() => {}), { transcript: sink });
    const last = events.next(event => event.n === lines * chunks - 1);
    let held = false;
    for (let index = 0; index < chunks; index++) if (!source.write(chunk(index))) held = true;
    await last;
    // With backpressure the producer is held; the control never holds it.
    assert.equal(held, honest, "backpressure reaches the producer only through write()'s return");
    return queued;
  };
  const bounded = await peak(true), burst = await peak(false);
  assert.ok(bounded <= size + 1024, `queued ${bounded} bytes, more than one ${size}-byte chunk`);
  assert.ok(burst > size * chunks / 2, `the ignore-write-return control queued only ${burst} bytes`);
});

test("spool cost is per event and waiter, not per event retained", async () => {
  // N events; each wait registers for the next event, as a turn-by-turn
  // driver does. The spool compares each event once per pending wait.
  // Negative control: the retained-history wait scanned every earlier event.
  for (const count of [100, 10_000, 100_000]) {
    const stream = new PassThrough();
    const events = new Spool(stream, new Promise(() => {}));
    let comparisons = 0;
    const retained = [];
    let scans = 0;
    for (let index = 0; index < count; index++) {
      const waiting = events.next(event => { comparisons++; return event.index === index; });
      stream.write(JSON.stringify({ index }) + "\n");
      const event = await waiting;
      retained.push(event);
      scans += retained.findIndex(seen => seen.index === index) + 1;
    }
    assert.equal(comparisons, count);
    assert.equal(scans, count * (count + 1) / 2, "the retained-history control scans quadratically");
    assert.ok(scans > comparisons * (count / 4), "the control exceeds the per-event budget");
  }
});

test("output watches cost each chunk once and survive split needles", () => {
  const needles = ["\x1b[?25h", "\x1b[?25l", "overflowed"];
  const text = "a\x1b[?25lb\x1b[?25hc overflowed d\x1b[?25l";
  for (let cut = 0; cut <= text.length; cut++) {
    const watch = new OutputWatch(needles).write(text.slice(0, cut)).write(text.slice(cut));
    assert.ok(watch.has("overflowed"));
    assert.ok(watch.later("\x1b[?25l", "\x1b[?25h"));
    assert.ok(!watch.later("\x1b[?25h", "\x1b[?25l"));
  }
  assert.ok(!OutputWatch.of("over flowed", needles).has("overflowed"));
});

const raw = text => OutputWatch.of(text, ["\x07", SHOW_CURSOR, HIDE_CURSOR]);

test("screen conditions require rendered styles and reject literal markdown", async () => {
  const screen = new Terminal({ cols: 80, rows: 24, allowProposedApi: true });
  const write = data => new Promise(resolve => screen.write(data, resolve));
  await write("**answer**");
  assert.ok(!matches(screen, raw(""), { bold: "answer", absent: ["**answer**"] }));
  await write("\r\x1b[2K\x1b[1manswer\x1b[0m");
  assert.ok(matches(screen, raw("\x07"), { contains: ["answer"], bold: "answer", absent: ["**answer**"], raw: ["\x07"] }));
  assert.ok(!matches(screen, raw(""), { contains: ["answer"], raw: ["\x07"] }));
  await write("\r\x1b[2K\u25cb GraphCoder");
  assert.ok(matches(screen, raw("\x1b[?25l\x1b[?25h"), { contains: ["\u25cb GraphCoder"], cursor: true }));
  assert.ok(!matches(screen, raw("\x1b[?25h\x1b[?25l"), { contains: ["\u25cb GraphCoder"], cursor: true }));
  assert.ok(!matches(screen, raw("\x1b[?25h"), { contains: ["\u00e2\u2014\u2039"] }));
  screen.dispose();
});

test("expected interrupted recovery still requires its retained transcript and exact failure", async () => {
  const screen = new Terminal({ cols: 80, rows: 24, allowProposedApi: true });
  const write = data => new Promise(resolve => screen.write(data, resolve));
  const expected = { task_failure: "model provider unavailable (co", contains: ["RECOVERED_E2E"] };
  const failure = "task 00000000-0000-0000-0000-000000000001 failed: ";
  await write("RECOVERED_E2E");
  assert.ok(!matches(screen, raw(""), expected), "replayed prefix alone is not settled recovery");
  await write("\r\n" + failure + "unauthorized");
  assert.ok(!matches(screen, raw(""), expected), "another error cannot satisfy interrupted recovery");
  await write("\r\x1b[2K" + failure + "model provider unavailable (connection interrupted)");
  assert.ok(matches(screen, raw(""), expected));
  await write("\x1b[H\x1b[2K");
  assert.ok(!matches(screen, raw(""), expected), "settlement must preserve the partial transcript");
  screen.dispose();
});

test("real children are killed and reaped on scope cleanup", { timeout: 5000 }, async () => {
  const processes = new Processes();
  const running = processes.start(process.execPath, ["-e", "process.stdout.write(JSON.stringify({event:'ready'})+'\\n');process.stdin.resume()"]);
  const events = new Spool(running.child.stdout, running.closed);
  await events.next(event => event.event === "ready");
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
