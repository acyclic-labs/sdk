import assert from "node:assert/strict";
import test from "node:test";
import { once } from "node:events";
import { mkdirSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PassThrough, Writable } from "node:stream";
import { fileURLToPath } from "node:url";
import { Spool, SpoolEnded, OutputWatch, Processes } from "./process.mjs";
import { matches, pty, SHOW_CURSOR, HIDE_CURSOR } from "./pty.mjs";
import { Terminal } from "../graphcoder-pty/screen.mjs";
import { TerminalFrames } from "./frames.mjs";
import { fuzz as fuzzTerminalFrames } from "./frames.fuzz.mjs";
import { failureReport } from "./report.mjs";

test("terminal action failure survives original native completion and capture rejection", {
  skip: process.platform === "win32",
}, async () => {
  const root = mkdtempSync(join(tmpdir(), "terminal-failure-"));
  try {
    for (const observationFails of [false, true]) {
      const processes = new Processes({ onProgress(event) {
        if (observationFails && event.phase === "node-spawn") readFileSync(join(root, "missing-terminal-observer"));
      } });
      let captures = 0;
      try {
        await assert.rejects(pty(processes, process.execPath, {
          workspace: root,
          args: ["-e", "process.stdout.write('has overflowed its stack');process.stdin.resume()"],
          onRequest() { return () => {}; },
        }, { actions: [{ when: { contains: ["unreachable"] } }] }, () => {
          captures++;
          readFileSync(join(root, "missing-terminal-capture"));
        }), error => {
          assert.match(error.message, /shipping executable failed.*has overflowed its stack/);
          assert.ok(error.cause instanceof AggregateError);
          const diagnostic = failureReport(error);
          assert.match(diagnostic.cause, /has overflowed its stack/);
          assert.match(diagnostic.cause, observationFails ? /missing-terminal-observer/ : /missing-terminal-capture/);
          return true;
        });
        assert.equal(captures, observationFails ? 0 : 1);
        assert.equal(processes.children.size, 0, "the original terminal owner must be reaped before rejection");
      } finally { await processes.close().catch(error => {
        if (!observationFails || error.name !== "ProcessObservationFailed") throw error;
      }); }
    }
  } finally { rmSync(root, { recursive: true }); }
});

test("terminal setup rejection releases parser and screen resources before returning its original error", async () => {
  const root = mkdtempSync(join(tmpdir(), "terminal-setup-")), processes = new Processes();
  const disposeFrames = TerminalFrames.prototype.dispose, disposeScreen = Terminal.prototype.dispose;
  let frames = 0, screens = 0, original;
  TerminalFrames.prototype.dispose = function () { frames++; return disposeFrames.call(this); };
  Terminal.prototype.dispose = function () { screens++; return disposeScreen.call(this); };
  try {
    await assert.rejects(pty(processes, process.execPath, {
      get args() {
        try { readFileSync(join(root, "missing-terminal-input")); }
        catch (error) { original = error; throw error; }
      },
    }, { actions: [] }, () => assert.fail("rejected setup cannot produce a capture")), error => error === original);
    assert.equal(original.code, "ENOENT");
    assert.equal(processes.children.size, 0, "setup rejection must precede executable effects");
    assert.equal(frames, 1, "actual terminal parser handlers remain owned through setup failure");
    assert.equal(screens, 1, "actual terminal screen must be disposed before rejection");
  } finally {
    TerminalFrames.prototype.dispose = disposeFrames;
    Terminal.prototype.dispose = disposeScreen;
    await processes.close();
    rmSync(root, { recursive: true });
  }
});

test("terminal completion admits the final frame after its last input action", async () => {
  // Exercise the real child, screen parser and action driver over stdio. Native
  // PTY adapters are qualified separately; these are internal frame controls.
  class TerminalProcess extends Processes {
    startTerminal(binary, args, { cwd, env }) { return this.start(binary, args, { cwd, env }); }
  }
  const root = mkdtempSync(join(tmpdir(), "terminal-completion-"));
  try {
    const tails = ["clean completion", "task 00000000-0000-0000-0000-000000000001 failed: cancelled",
      "has overflowed its stack", "has overflowed its stack\x1b[2J\x1b[Hready",
      "\x1b[?2026huncommitted frame"];
    for (const [tail, killed] of [...tails.map(tail => [tail, false]), [tails.at(-1), true]]) {
      const processes = new TerminalProcess();
      let capture;
      try {
        const running = pty(processes, process.execPath, {
          id: "terminal-completion", workspace: root,
          args: ["-e", `process.stdout.write('ready');process.stdin.once('data',()=>{process.stdout.write(${JSON.stringify("\n" + tail)},()=>{${killed ? "process.kill(process.pid,'SIGTERM')" : "process.stdin.destroy()"}})})`],
          onRequest() { return () => {}; },
        }, { actions: [{ when: { contains: ["ready"] }, send: "finish\n" }], killed }, result => { capture = result; });
        if (tail === tails[0] || killed) {
          const result = await running;
          assert.equal(Boolean(result.signal || result.code !== 0), killed);
        }
        else await assert.rejects(running, tail === tails.at(-1)
          ? /admit completed terminal transaction/ : error => {
            assert.equal(error.code, "terminal_frame_failed");
            assert.ok(error.message.includes(tail.includes("has overflowed its stack")
              ? "has overflowed its stack" : tail), "retain the detected failure even after a terminal redraw");
            return true;
          });
        assert.equal(capture.actions, 1, "the original input action must complete before the tail");
        assert.ok(capture.stdout.endsWith(tail), "retain the original final bytes before admission");
        assert.equal(processes.children.size, 0, "terminal admission follows original child retirement");
      } finally { await processes.close(); }
    }
  } finally { rmSync(root, { recursive: true }); }
});

test("screen assertions observe complete synchronized frames at every fragmentation boundary", async () => {
  const bytes = "\x1b[?2026h\x1b[Hdraft\x1b[Hfinal\x1b[?2026l";
  for (let cut = 0; cut <= bytes.length; cut++) {
    const screen = new Terminal({ cols: 80, rows: 24, allowProposedApi: true });
    const seen = [];
    const capture = () => seen.push(screen.buffer.active.getLine(0).translateToString(true));
    const frames = new TerminalFrames(screen, capture);
    await frames.write(bytes.slice(0, cut));
    await frames.write(bytes.slice(cut));
    assert.ok(seen.includes("final"));
    assert.ok(seen.every(text => text === "" || text === "final"), JSON.stringify(seen));
    frames.dispose(); screen.dispose();
  }
});

test("shipping terminal refusals survive coalesced and fragmented redraws", async () => {
  for (const schedule of [[], [0], [63], [255, 0, 7, 1]]) await fuzzTerminalFrames(Buffer.from(schedule));
});

const queryCapturePath = fileURLToPath(new URL("./fixtures/live-terminal-query-refusal.json", import.meta.url));
const queryCapture = JSON.parse(readFileSync(queryCapturePath, "utf8"));
test("observed producer frame asserts captured content with one screen projection", async () => {
  class TerminalProcess extends Processes {
    startTerminal(binary, args, { cwd, env }) { return this.start(binary, args, { cwd, env }); }
  }
  const root = mkdtempSync(join(tmpdir(), "observed-frame-"));
  const buffer = Object.getOwnPropertyDescriptor(Terminal.prototype, "buffer");
  const write = TerminalFrames.prototype.write;
  let reads = 0;
  const projections = [];
  Object.defineProperty(Terminal.prototype, "buffer", { ...buffer, get() { reads++; return buffer.get.call(this); } });
  const observed = new WeakSet();
  TerminalFrames.prototype.write = function (bytes) {
    if (!observed.has(this)) {
      observed.add(this);
      const frame = this.frame;
      this.frame = () => {
        const before = reads;
        try { return frame(); } finally { projections.push(reads - before); }
      };
    }
    return write.call(this, bytes);
  };
  try {
    const raw = queryCapture.stdout.slice(-32);
    for (const [rejects, assertion] of [
      [false, { contains: ["workspace diff query is u"] }],
      [true, { contains: ["INTENTIONALLY_ABSENT_DIFF_CONTENT"] }],
      [false, { raw: [raw] }],
      [true, { raw: [raw + "\0"] }],
    ]) {
      const processes = new TerminalProcess();
      let capture;
      try {
        const running = pty(processes, process.execPath, {
          id: "observed-frame", workspace: root,
          args: ["-e", "process.stdin.once('data',()=>process.stdin.destroy());process.stdout.write(JSON.parse(require('node:fs').readFileSync(process.argv[1],'utf8')).stdout)", queryCapturePath],
          onRequest() { return () => {}; },
        }, { actions: [{ when: { query_failure: "storage failure: diff command:" },
          assert: assertion, send: "finish\n" }] },
        result => { capture = result; });
        if (rejects) await assert.rejects(running, /step=assert observed terminal frame/);
        else assert.equal((await running).code, 0);
        assert.equal(capture.stdout, queryCapture.stdout, "retain the original captured bytes");
        assert.equal(processes.children.size, 0);
      } finally { await processes.close(); }
    }
    assert.ok(projections.includes(1), "the original frame must be inspected");
    assert.ok(projections.every(count => count <= 1), "one screen projection serves matching and assertion");
  } finally {
    TerminalFrames.prototype.write = write;
    Object.defineProperty(Terminal.prototype, "buffer", buffer);
    rmSync(root, { recursive: true });
  }
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

test("a spool ends with the exact typed cause", async t => {
  const ended = async (events, feed) => { const waiting = events.next(() => false); feed(); return waiting.then(() => assert.fail("resolved"), error => error); };
  await t.test("source EOF carries the source result", async () => {
    const stream = new PassThrough();
    let close; const closed = new Promise(resolve => { close = resolve; });
    const events = new Spool(stream, closed), results = [];
    const waits = [events.next(() => false), events.next(() => false),
      events.keyed("same"), events.keyed("same"), events.keyed("other")];
    for (const wait of waits) wait.catch(error => { results.push(error); });
    const error = await ended(events, () => { stream.end(); close({ code: 3, signal: null }); });
    assert.equal(error.kind, "eof"); assert.equal(error.detail.code, 3);
    assert.equal(results.length, waits.length, "the original EOF settles every ordinary wait, repeated key and distinct key");
    assert.ok(results.every(result => result === error), "all consumers retain the same terminal cause");
  });
  await t.test("early event: next never sees it, and EOF ends the wait exactly", async () => {
    const stream = new PassThrough();
    let close; const closed = new Promise(resolve => { close = resolve; });
    const events = new Spool(stream, closed);
    const published = events.next(event => event.ready);
    stream.write('{"ready":true}\n');
    await published;
    const late = events.next(event => event.ready);
    stream.end(); close({ code: 0, signal: null });
    await assert.rejects(late, error => error.kind === "eof");
  });
});

test("transcript admission refuses before the executable starts", async t => {
  const root = mkdtempSync(join(tmpdir(), "transcript-admission-"));
  const second = join(root, "second");
  mkdirSync(second); mkdirSync(join(second, "0.stderr"));
  for (const transcripts of [join(root, "missing"), second]) await t.test(transcripts, async () => {
    const processes = new Processes({ transcripts });
    try {
      // A real executable would wait forever if launched without input custody.
      await assert.rejects(processes.run(process.execPath, ["-e", "process.stdin.resume()"]),
        error => error instanceof SpoolEnded && error.kind === "spool" && /ENOENT|EEXIST/.test(error.detail.code));
      assert.equal(processes.started, 0);
      assert.equal(processes.children.size, 0);
      if (transcripts.endsWith("missing")) mkdirSync(transcripts);
      const recovered = await processes.run(process.execPath, ["-e", "process.stdout.write('recovered')"]);
      assert.equal(recovered.code, 0);
      assert.equal(recovered.stdout, "recovered");
      assert.equal(processes.started, 1);
    } finally { await processes.close(); }
  });
});

test("a synchronous launch refusal preserves transcripts and allows a corrected launch", async () => {
  const processes = new Processes();
  try {
    assert.throws(() => processes.start(process.execPath, ["\0"]), { code: "ERR_INVALID_ARG_VALUE" });
    assert.equal(processes.started, 0);
    assert.equal(processes.children.size, 0);
    const recovered = await processes.run(process.execPath, ["-e", "process.stdout.write('recovered')"]);
    assert.equal(recovered.code, 0);
    assert.equal(recovered.stdout, "recovered");
    assert.equal(readFileSync(join(processes.transcripts, "0.stdout"), "utf8"), "");
    assert.equal(readFileSync(join(processes.transcripts, "0.stderr"), "utf8"), "");
    assert.equal(processes.started, 1);
  } finally { await processes.close(); }
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

test("captured interrupted recovery admits its wrapped diagnostic through original completion", async () => {
  class TerminalProcess extends Processes {
    startTerminal(binary, args, { cwd, env }) { return this.start(binary, args, { cwd, env }); }
  }
  const fixture = fileURLToPath(new URL("./fixtures/interrupted-terminal.json", import.meta.url));
  const retained = JSON.parse(readFileSync(fixture, "utf8"));
  const root = mkdtempSync(join(tmpdir(), "terminal-recovery-"));
  try {
    for (const width of [0, 1, 63]) {
      const processes = new TerminalProcess();
      try {
        let captured;
        const result = await pty(processes, process.execPath, {
          id: "retained-interrupted-recovery", workspace: root,
          // Replay original native bytes, with the final diagnostic released
          // by the same input action as the real kill-and-resume journey.
          args: ["-e", `
            const captured = JSON.parse(require('node:fs').readFileSync(process.argv[1], 'utf8'));
            const bytes = Buffer.from(captured.stdout), marker = Buffer.from('\\x1b[?2026l');
            const end = bytes.lastIndexOf(marker) + marker.length;
            process.stdout.write(bytes.subarray(0, end));
            process.stdin.once('data', () => {
              let offset = end;
              function write() {
                if (offset === bytes.length) { process.exitCode = 1; process.stdin.destroy(); return; }
                const next = Math.min(bytes.length, offset + (Number(process.argv[2]) || bytes.length));
                const chunk = bytes.subarray(offset, next); offset = next;
                process.stdout.write(chunk, write);
              }
              write();
            });`, fixture, String(width)],
          onRequest() { return () => {}; },
        }, { actions: [{ when: retained.condition, send: "\x04" }], exit: 1 }, value => { captured = value; });
        assert.equal(result.code, 1, "the admitted failure must reach its original nonzero exit");
        assert.equal(captured.actions, 1);
        assert.equal(captured.stdout, retained.stdout, "retain every original terminal byte");
      } finally { await processes.close(); }
    }
  } finally { rmSync(root, { recursive: true }); }
});

test("real children are killed and reaped on scope cleanup", async t => {
  const processes = new Processes();
  t.after(() => processes.close());
  const running = processes.start(process.execPath, ["-e", "process.stdout.write(JSON.stringify({event:'ready'})+'\\n');process.stdin.resume()"]);
  const events = new Spool(running.child.stdout, running.closed);
  await events.next(event => event.event === "ready");
  await processes.close();
  assert.equal(processes.children.size, 0);
  assert.throws(() => processes.start(process.execPath, []), /scope is closed/);
});

test("real process captures preserve UTF-8 at every byte boundary", async t => {
  const processes = new Processes();
  t.after(() => processes.close());
  const child = processes.start(process.execPath, ["-e", "require('node:readline').createInterface({input:process.stdin}).on('line',line=>process.stdout.write(Buffer.from(line,'hex')))"]);
  const text = "hé λ 🙂", bytes = Buffer.from(text);
  const output = child.child.stdout.iterator({ destroyOnReturn: false });
  const receive = async (output, remaining) => {
    while (remaining > 0) {
      const next = await output.next();
      assert.equal(next.done, false, 'step=fragment-output; recovery=node --test --test-name-pattern "real process captures preserve UTF-8 at every byte boundary" scripts/graphcoder-e2e/runner.test.mjs');
      remaining -= next.value.length;
      assert.ok(remaining >= 0, "original child output exceeded the selected fragment");
    }
  };
  try {
    for (let cut = 0; cut <= bytes.length; cut++) for (const fragment of [bytes.subarray(0, cut), bytes.subarray(cut)]) {
      if (!fragment.length) continue;
      const received = receive(output, fragment.length);
      child.child.stdin.write(fragment.toString("hex") + "\n");
      await received;
    }
    const ended = output.next();
    child.child.stdin.end();
    assert.equal((await ended).done, true, "original child output must reach EOF");
    assert.equal((await output.next()).done, true, "late reads retain original EOF");
    assert.equal((await child.closed).stdout, text.repeat(bytes.length + 1));
  } finally { await output.return(); }
  // Original producer loss must fail the same fragment consumer without a timer.
  const missing = processes.start(process.execPath, ["-e", "process.exit(3)"]);
  const lost = missing.child.stdout.iterator({ destroyOnReturn: false });
  try {
    await assert.rejects(receive(lost, 1), error => error.code === "ERR_ASSERTION" && error.message.includes("step=fragment-output"));
    assert.equal((await missing.closed).code, 3);
  } finally { await lost.return(); }
});
