import assert from "node:assert/strict";
import test from "node:test";
import { getEventListeners, once } from "node:events";
import { appendFileSync, close as fsClose, createWriteStream, mkdtempSync, open as fsOpen, readFileSync, rmSync, truncateSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { PassThrough } from "node:stream";
import { finished } from "node:stream/promises";
import { Processes, Spool, SpoolEnded } from "./process.mjs";
import { failureReport } from "./report.mjs";
import { fuzz as fuzzSpool } from "./process.fuzz.mjs";

test("Spool byte fuzz preserves captured native replies and refusal terminals", async () => {
  const shipped = JSON.parse(readFileSync(new URL("./fixtures/shipping-output.json", import.meta.url), "utf8"));
  const killed = JSON.parse(readFileSync(new URL("./fixtures/rejected-conversation-reply.json", import.meta.url), "utf8"));
  for (const original of [shipped.captures["jsonl-reopen"], killed.capture]) {
    const bytes = Buffer.from(original.stdout);
    const terminal = { code: original.code, signal: original.signal };
    await fuzzSpool(bytes, terminal);
    for (const at of [0, 1, Math.floor(bytes.length / 2), bytes.length - 1]) {
      const mutation = Buffer.from(bytes);
      mutation[at] ^= 255;
      await fuzzSpool(mutation, terminal);
      await fuzzSpool(bytes.subarray(0, at), terminal);
    }
  }
});

test("native event callback failures end unmatched waits with the original cause", async () => {
  const root = mkdtempSync(join(tmpdir(), "spool-consumer-"));
  try {
    for (const stage of ["key", "listener", "match", "eof"]) {
      const processes = new Processes();
      try {
        let original, calls = 0;
        const fail = () => {
          calls++;
          try { readFileSync(join(root, "original-missing-callback-input")); }
          catch (error) { original = error; throw error; }
        };
        const producer = processes.start(process.execPath, ["-e", stage === "eof" ? "" :
          "console.log(JSON.stringify({id:'first'}));console.log(JSON.stringify({id:'later'}))"]);
        producer.input.end();
        const events = new Spool(producer.child.stdout, producer.closed, {
          key: stage === "key" ? fail : event => [event.id],
        });
        if (stage === "listener") events.subscribe(fail);
        const next = events.next(stage === "match" ? fail : () => false).catch(error => error);
        const keyed = events.keyed("unpublished").catch(error => error);
        const laterNext = events.next(() => false).catch(error => error);
        const laterKeyed = events.keyed("unpublished-other").catch(error => error);
        const failure = await next;
        assert.ok(failure instanceof SpoolEnded);
        assert.equal(await keyed, failure);
        assert.equal(await laterNext, failure);
        assert.equal(await laterKeyed, failure);
        assert.equal(failure.kind, stage === "eof" ? "eof" : "consumer");
        if (stage !== "eof") {
          assert.equal(failure.cause, original);
          assert.equal(failure.detail, original);
          assert.equal(original.code, "ENOENT");
          assert.match(failureReport(failure).cause, /ENOENT.*original-missing-callback-input/);
          assert.match(failure.message, /step=dispatch event 1.*recovery=/);
        }
        await assert.rejects(events.next(() => true), error => error === failure);
        await assert.rejects(events.keyed("late"), error => error === failure);
        assert.equal((await producer.closed).code, 0, "native exit zero cannot erase consumer failure");
        assert.equal(calls, stage === "eof" ? 0 : 1, "a failed callback cannot consume later events");
        assert.equal(processes.children.size, 0, "original producer retirement precedes completion");
      } finally { await processes.close(); }
    }
  } finally { rmSync(root, { recursive: true }); }
});

test("retired native event observations release their captured payloads", async () => {
  const processes = new Processes();
  const code = `
    import assert from 'node:assert/strict';
    import { Processes, Spool } from ${JSON.stringify(new URL("./process.mjs", import.meta.url).href)};
    const processes = new Processes();
    try {
      const producer = processes.start(process.execPath, ['-e', "console.log(JSON.stringify({id:'retained'}))"]);
      producer.input.end();
      const events = new Spool(producer.child.stdout, producer.closed, {key: event => [event.id]});
      function observe() {
        const payload = new Uint8Array(1024 * 1024);
        events.subscribe(() => payload.byteLength);
        return new WeakRef(payload);
      }
      const active = observe();
      await events.next(() => false).catch(error => assert.equal(error.kind, 'eof'));
      assert.equal((await producer.closed).code, 0);
      const late = observe();
      await new Promise(resolve => setImmediate(resolve));
      global.gc();
      assert.equal(active.deref() === undefined, true, 'terminal observation retained its capture');
      assert.equal(late.deref() === undefined, true, 'late observation retained its capture');
      assert.deepEqual(await events.keyed('retained'), {id:'retained'});
      assert.equal(processes.children.size, 0);
    } finally { await processes.close(); }
  `;
  try {
    const result = await processes.run(process.execPath, ["--expose-gc", "--input-type=module", "-e", code]);
    assert.equal(result.code, 0, result.stderr);
  } finally { await processes.close(); }
});

test("keyed transcript lookups read exactly one original event, including after EOF", async () => {
  const source = new PassThrough();
  let close;
  const closed = new Promise(resolve => { close = resolve; });
  const path = join(mkdtempSync(join(tmpdir(), "keyed-spool-")), "events.jsonl");
  const events = new Spool(source, closed, { transcript: path, key: event => [event.id, `alias:${event.id}`], unique: true });
  const first = { id: "first", text: "λ\n終" }, second = { id: "second", text: "z".repeat(128 * 1024) };
  const seen = events.next(event => event.id === "second");
  source.end(JSON.stringify(first) + "\r\n" + JSON.stringify(second));
  await seen;
  // EOF may win the next microtask, but cannot erase a prior indexed event.
  const early = events.keyed("first");
  close({ code: 0, signal: null });
  assert.deepEqual(await early, first);
  assert.deepEqual(await events.keyed("alias:second"), second);
  await events.history();
  truncateSync(path, 1);
  await assert.rejects(events.keyed("first"), error => error.kind === "spool" && /indexed event/.test(error.message));
});

test("abandoned next and keyed waits release their subscriptions without ending the source", async () => {
  const source = new PassThrough(), controller = new AbortController();
  const events = new Spool(source, new Promise(() => {}), { key: event => [event.id] });
  const reason = new Error("case no longer owns these waits");
  let comparisons = 0;
  const next = events.next(() => { comparisons++; return false; }, { signal: controller.signal }).catch(error => error);
  const keyed = events.keyed("abandoned", { signal: controller.signal }).catch(error => error);
  assert.equal(getEventListeners(controller.signal, "abort").length, 2);
  controller.abort(reason);
  assert.equal(getEventListeners(controller.signal, "abort").length, 0);
  const live = events.keyed("live");
  source.write('{"id":"live"}\n');
  assert.deepEqual(await live, { id: "live" });
  assert.equal(comparisons, 0, "cancelled predicates must never see subsequent events");
  for (const error of [await next, await keyed]) {
    assert.equal(error.kind, "cancelled");
    assert.equal(error.detail, reason);
  }
  await assert.rejects(events.keyed("live", { signal: controller.signal }), error => error.kind === "cancelled");
  source.end();
});

test("a file-backed key index releases parsed payloads as the number of unique keys grows", async () => {
  const processes = new Processes();
  const code = `
    import assert from 'node:assert/strict';
    import { mkdtempSync } from 'node:fs';
    import { tmpdir } from 'node:os';
    import { join } from 'node:path';
    import { PassThrough } from 'node:stream';
    import { Spool } from ${JSON.stringify(new URL("./process.mjs", import.meta.url).href)};
    const source = new PassThrough(), references = [];
    const events = new Spool(source, new Promise(() => {}), {
      transcript: join(mkdtempSync(join(tmpdir(), 'indexed-payloads-')), 'events.jsonl'),
      key: event => { references.push(new WeakRef(event)); return [event.id]; }, unique: true,
    });
    const total = 1024;
    const completed = events.next(event => event.id === total - 1);
    for (let id = 0; id < total; id++) {
      const line = JSON.stringify({id, payload: String(id).padEnd(16 * 1024, 'x')}) + '\\n';
      if (!source.write(line)) await new Promise(resolve => source.once('drain', resolve));
    }
    await completed;
    await events.history(() => false);
    // One event-loop turn ends WeakRef's mandatory current-job retention.
    await new Promise(resolve => setImmediate(resolve));
    global.gc();
    const retained = references.filter(reference => reference.deref() !== undefined).length;
    assert.ok(retained <= 1, 'index retained ' + retained + ' full event payloads');
    const selected = await events.keyed(511);
    assert.equal(selected.id, 511);
    assert.equal(selected.payload.length, 16 * 1024);
    source.end();
    console.log(JSON.stringify({keys: total, retained_parsed_payloads: retained}));
  `;
  try {
    const result = await processes.run(process.execPath, ["--expose-gc", "--input-type=module", "-e", code]);
    assert.equal(result.code, 0, result.stderr);
    assert.equal(JSON.parse(result.stdout).keys, 1024);
  } finally { await processes.close(); }
});

test("native transcript cursors retain one record and close the original range on return", async t => {
  const processes = new Processes();
  const producerSource = "const fs = require('node:fs'); for (let id = 0; id < 512; id++) fs.writeSync(1, JSON.stringify({id, text: 'λ終'.repeat(id === 255 ? 131073 : 1024)}) + '\\n');";
  const code = `
    import assert from 'node:assert/strict';
    import fs from 'node:fs';
    import { syncBuiltinESMExports } from 'node:module';
    import { tmpdir } from 'node:os';
    import { join } from 'node:path';
    import { Processes, Spool } from ${JSON.stringify(new URL("./process.mjs", import.meta.url).href)};
    const directory = fs.mkdtempSync(join(tmpdir(), 'spool-cursor-'));
    const path = join(directory, 'events.jsonl'), processes = new Processes();
    const opened = new Set();
    let readBytes = 0, bufferBytes = 0, opens = 0, parsed = 0;
    const open = fs.openSync, read = fs.readSync, close = fs.closeSync;
    const parse = JSON.parse;
    JSON.parse = (...args) => { parsed++; return parse(...args); };
    fs.openSync = (name, ...args) => {
      const fd = open(name, ...args);
      if (name === path) { opened.add(fd); opens++; }
      return fd;
    };
    fs.readSync = (fd, buffer, ...args) => {
      const size = read(fd, buffer, ...args);
      if (opened.has(fd)) { readBytes += size; bufferBytes = Math.max(bufferBytes, buffer.length); }
      return size;
    };
    fs.closeSync = fd => { opened.delete(fd); return close(fd); };
    syncBuiltinESMExports();
    const reset = () => { assert.equal(opened.size, 0); readBytes = bufferBytes = opens = parsed = 0; };
    const bounded = () => assert.ok(bufferBytes <= 64 * 1024, 'reader retained aggregate transcript buffer: ' + bufferBytes);
    try {
      // Actual native Node stdout is the fixture: UTF-8, many records and one
      // record larger than the cursor's chunk, captured by the original Spool.
      const producer = processes.start(process.execPath, ['-e', ${JSON.stringify(producerSource)}]);
      producer.input.end();
      const events = new Spool(producer.child.stdout, producer.closed, {transcript: path});
      await events.next(() => false).catch(error => assert.equal(error.kind, 'eof'));
      const result = await producer.closed;
      assert.equal(result.code, 0, result.stderr);
      const complete = fs.statSync(path).size;

      reset();
      let count = 0;
      for await (const event of events.records()) {
        assert.equal(event.id, count++);
        assert.equal(event.text, 'λ終'.repeat(event.id === 255 ? 131073 : 1024));
      }
      assert.equal(count, 512);
      assert.equal(readBytes, complete, 'one pass over the completed prefix');
      assert.equal(opens, 1);
      assert.equal(parsed, 512);
      bounded();

      reset();
      for await (const event of events.records()) { assert.equal(event.id, 0); break; }
      assert.equal(opens, 1);
      assert.equal(readBytes, 64 * 1024, 'return must not read the remaining history');
      assert.equal(parsed, 1, 'return must not decode later records');
      assert.equal(opened.size, 0, 'return retires the original descriptor');

      reset();
      const selected = await events.history(event => event.id === 511);
      assert.deepEqual(selected, [{id: 511, text: 'λ終'.repeat(1024)}]);
      assert.equal(readBytes, complete);
      bounded();

      // Independent cost negative: restore the old full-prefix read against
      // this SAME native transcript. The range oracle must reject it.
      reset();
      const bytes = Buffer.alloc(complete), fd = fs.openSync(path, 'r');
      try { fs.readSync(fd, bytes, 0, bytes.length, 0); }
      finally { fs.closeSync(fd); }
      bytes.toString('utf8').split('\\n').filter(Boolean).map(JSON.parse);
      assert.equal(parsed, 512, 'restored aggregate path decodes every later record');
      assert.throws(bounded, /aggregate transcript buffer/);

      const cursor = events.records();
      assert.equal((await cursor.next()).value.id, 0);
      fs.appendFileSync(path, JSON.stringify({id: 'uncommitted-tail'}) + '\\n');
      let remaining = 0;
      for await (const event of cursor) { assert.equal(event.id, ++remaining); }
      assert.equal(remaining, 511, 'the original completed prefix excludes later bytes');
      assert.equal(opened.size, 0);

      const cause = new Error('original predicate failure');
      await assert.rejects(events.history(() => { throw cause; }), error => error.kind === 'spool' && error.cause === cause);
      assert.equal(opened.size, 0, 'predicate failure retires the descriptor');

      const truncated = events.records();
      await truncated.next();
      fs.truncateSync(path, 64 * 1024);
      await assert.rejects(async () => { for await (const event of truncated) void event; },
        error => error.kind === 'spool' && /ends at byte 65536/.test(error.message));
      assert.equal(opened.size, 0, 'lost original bytes cause a typed failure and retirement');
      fs.renameSync(path, path + '.retired');
      await assert.rejects(events.history(), error => error.kind === 'spool' && error.cause?.code === 'ENOENT');
      assert.equal(opened.size, 0, 'missing transcript preserves its original open failure');
      console.log(JSON.stringify({records: count, transcript_bytes: complete, read_buffer_bytes: 64 * 1024}));
    } finally {
      await processes.close();
      fs.rmSync(directory, {recursive: true});
    }
  `;
  try {
    const result = await processes.run(process.execPath, ["--input-type=module", "-e", code]);
    assert.equal(result.code, 0, result.stderr);
    assert.equal(JSON.parse(result.stdout).records, 512);
    t.diagnostic(result.stdout.trim());
  } finally { await processes.close(); }
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
  assert.equal(transcript.listenerCount("drain"), 0, "the terminal spool releases its obsolete drain observer");
});

test("transcript withdrawal ends active and late spool waits with the original cause", async t => {
  const directory = mkdtempSync(join(tmpdir(), "e2e-transcript-loss-"));
  for (const alreadyClosed of [false, true]) await t.test(alreadyClosed ? "retained close" : "active close", async () => {
    const transcript = createWriteStream(join(directory, `${alreadyClosed}.jsonl`));
    const closed = once(transcript, "close");
    if (alreadyClosed) { transcript.destroy(); await closed; }
    const source = new PassThrough();
    const events = new Spool(source, new Promise(() => {}), { transcript });
    const waiting = assert.rejects(events.next(() => false), error => error.kind === "spool" && error.detail.code === "ERR_STREAM_PREMATURE_CLOSE");
    if (!alreadyClosed) { transcript.destroy(); await closed; }
    // The original close is the finite boundary: no process exit or timer
    // may supply a substitute failure when transcript ownership is lost.
    await finished(transcript, { readable: false, cleanup: true }).catch(() => {});
    assert.equal(events.ended?.kind, "spool");
    await waiting;
    await assert.rejects(events.next(() => true), error => error === events.ended);
    await assert.rejects(events.history(), error => error === events.ended);
    source.end();
  });
});
test("transcript withdrawal drops the queued source chunk with its original close error", async () => {
  // Its full readable buffer holds the original write callback. Destroying
  // this real stream closes it without injecting a competing write error.
  const transcript = new PassThrough({ highWaterMark: 1 });
  const source = new PassThrough();
  const events = new Spool(source, new Promise(() => {}), { transcript });
  const waiting = events.next(event => event.n === 2);
  waiting.catch(() => {});
  source.write('{"n":1}\n');
  source.write('{"n":2}\n');
  assert.equal(events.sequence, 1, "the second chunk is held behind the original transcript drain");
  const closed = once(transcript, "close");
  transcript.destroy();
  await closed;
  await assert.rejects(waiting, error => error.kind === "spool" && error.detail.code === "ERR_STREAM_PREMATURE_CLOSE");
  assert.equal(events.sequence, 1, "transcript loss must not publish the queued chunk");
  assert.equal(transcript.listenerCount("drain"), 0, "transcript loss releases its obsolete drain observer");
  source.end();
});
test("a source closed without ending still ends the spool and its transcript", async () => {
  const source = new PassThrough();
  let close; const closed = new Promise(resolve => { close = resolve; });
  const events = new Spool(source, closed, { transcript: join(mkdtempSync(join(tmpdir(), "e2e-history-")), "events.jsonl") });
  const waiting = events.next(() => false);
  source.destroy(); close({ code: 0, signal: null });
  await assert.rejects(waiting, error => error.kind === "eof");
});
