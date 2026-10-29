import { spawn } from "node:child_process";
import { closeSync, createWriteStream, mkdtempSync, openSync, readFileSync, readSync } from "node:fs";
import { tmpdir } from "node:os";
import { Writable } from "node:stream";
import { StringDecoder } from "node:string_decoder";
import { stripVTControlCharacters } from "node:util";
import assert from "node:assert/strict";
import { isAbsolute, join } from "node:path";
import { fileURLToPath } from "node:url";
import { ProcessReport } from "./process-report.mjs";

export async function schedule(items, platform, execute) {
  if (platform !== "win32") return Promise.all(items.map(execute));
  const results = [];
  for (const item of items) results.push(await execute(item));
  return results;
}

// Event-driven subprocesses: no polling or readiness delays. Every child is
// owned by the suite and is reaped even when another case fails.
//
// Output is not retained in memory: each stream goes to a transcript file in
// the scope's transcript directory, and a settled result reads it on demand.
export class Processes {
  children = new Map();
  stopping = false;
  started = 0;
  #transcripts;
  constructor({ transcripts } = {}) { this.#transcripts = transcripts; }
  get transcripts() { return this.#transcripts ??= mkdtempSync(join(tmpdir(), "e2e-transcripts-")); }
  start(program, args, options = {}) {
    if (this.stopping) throw new Error("suite process scope is closed (deadline or cleanup)");
    const { scopeKillSignal = "SIGKILL", scopeTreeControl = false, scopeReport = false, ...spawnOptions } = options;
    const report = scopeReport ? new ProcessReport() : undefined;
    const child = spawn(program, args, { windowsHide: true, stdio: ["pipe", "pipe", "pipe"], ...spawnOptions,
      ...(report ? { stdio: ["pipe", "pipe", "pipe", "pipe"], env: {
        ...(spawnOptions.env ?? process.env), ACYCLIC_REPORT_FD: "3", ACYCLIC_REPORT_GENERATION: report.generation,
      } } : {}),
    });
    report?.attach(child);
    child.stdin.on("error", error => { if (error.code !== "EPIPE") child.emit("error", error); });
    const kill = () => {
      if (scopeTreeControl) { child.stdin.end(); return; }
      child.kill(scopeKillSignal);
    };
    const index = this.started++;
    const stdoutPath = join(this.transcripts, `${index}.stdout`), stderrPath = join(this.transcripts, `${index}.stderr`);
    const written = Promise.all([transcript(child.stdout, stdoutPath), transcript(child.stderr, stderrPath)]);
    // Exit is the owner-death barrier. Close may wait for inherited pipes in
    // surviving descendants, which recovery must be allowed to fence first.
    const exited = new Promise((resolve, reject) => {
      child.once("error", reject);
      child.once("exit", (code, signal) => resolve({ code, signal }));
    });
    exited.catch(() => {});
    // Reaped once the child and its pipes are gone, however `closed` settles.
    const reaped = new Promise(resolve => {
      child.once("error", resolve);
      child.once("close", () => { this.children.delete(child); resolve(); });
    });
    const closed = new Promise((resolve, reject) => {
      // A transcript that cannot be opened or written ends the run now, with
      // the typed cause, instead of an unhandled rejection at exit.
      written.catch(error => {
        kill();
        reject(new SpoolEnded("spool", `transcript: ${error.message}`, error));
      });
      child.on("error", reject);
      child.on("close", (code, signal) => {
        // Settle only after both transcripts are complete on disk.
        written.then(() => {
          const result = settled(code, signal, stdoutPath, stderrPath);
          if (report?.termination) result.termination = report.termination;
          resolve(result);
        }, () => {});
      });
    });
    // The caller may be waiting for a ready/screen event instead of exit.
    closed.catch(() => {});
    this.children.set(child, { kill, reaped, report });
    return { child, exited, closed, reportReady: report?.ready,
      requestReport: report ? deadline => report.request(deadline) : undefined,
      killExecutable: () => child.kill("SIGKILL") };
  }
  async run(program, args, options, input = "") {
    const process = this.start(program, args, options);
    process.child.stdin.end(input);
    return process.closed;
  }
  // Windows Job custody kills the whole tree with its host; POSIX custody
  // kills only the executable on SIGUSR1 and still drains and reports it.
  startOwned(program, args, options = {}) {
    const { scopeMountRoot, scopeInheritInput = false, scopePty = false, ...spawnOptions } = options;
    if (process.platform === "win32") {
      const host = options.env?.E2E_PROCESS_HOST ?? process.env.E2E_PROCESS_HOST;
      assert.ok(host && isAbsolute(host), "E2E_PROCESS_HOST must name the cross-built process-tree host");
      return this.start(host, [...(scopeInheritInput ? ["--inherit-input"] : []), program, ...args],
        { ...spawnOptions, scopeTreeControl: !scopeInheritInput });
    }
    const mountLease = process.platform === "linux" && scopeMountRoot
      ? ["--mount-root", scopeMountRoot, "--"] : [];
    const owned = this.start("python3", [fileURLToPath(new URL("./owned-process.py", import.meta.url)),
      ...(scopePty ? ["--pty"] : []), ...mountLease, program, ...args], { ...spawnOptions, scopeTreeControl: true });
    return { ...owned, killExecutable: () => owned.child.kill("SIGUSR1") };
  }
  // The one real-terminal transport: ConPTY inside native Job custody on
  // Windows, or a PTY whose root leads its own session under the POSIX owner.
  startTerminal(binary, args, options = {}) {
    const env = { ...(options.env ?? process.env), TERM: "xterm-256color" };
    if (process.platform !== "win32") return this.startOwned(binary, args, { scopeReport: true, ...options, env, scopePty: true });
    const host = env.E2E_CONPTY_HOST;
    assert.ok(host && isAbsolute(host), "cross-build the ConPTY host and set E2E_CONPTY_HOST");
    return this.startOwned(host, conptyArgs(binary, args), { ...options, env, scopeInheritInput: true });
  }
  async runOwned(program, args, options, input = "") {
    assert.equal(input, "", "external acceptance commands receive empty stdin");
    const running = this.startOwned(program, args, options);
    const [, result] = await Promise.all([running.exited, running.closed]);
    return result;
  }
  async close() {
    this.stopping = true;
    const children = [...this.children.values()];
    const reporting = children.filter(child => child.report);
    if (reporting.length) {
      // The active deadline already failed. The existing drain reservation
      // permits diagnostics before the same physical termination barrier.
      let force;
      const forced = new Promise(resolve => { force = setTimeout(resolve, 500); });
      const deadline = Date.now() + 500;
      try { await Promise.race([Promise.all(reporting.map(child => child.report.request(deadline))), forced]); }
      finally { clearTimeout(force); }
      for (const child of reporting) child.report.expire();
    }
    for (const child of children) child.kill();
    await Promise.all(children.map(child => child.reaped));
  }
}

// The ConPTY host admits the launch paths plus typed options; the case owner holds the only deadline.
export function conptyArgs(binary, args) {
  const flags = new Map();
  let resume = false;
  for (let index = 0; index < args.length; index++) {
    if (args[index] === "--resume" && !resume) { resume = true; continue; }
    assert.ok(["--workspace", "--settings-root", "--catalog", "--model", "--operation"].includes(args[index])
      && !flags.has(args[index]) && index + 1 < args.length, `ConPTY host cannot launch argument ${args[index]}`);
    flags.set(args[index], args[++index]);
  }
  const [workspace, catalog, settings, model] = ["--workspace", "--catalog", "--settings-root", "--model"].map(flag => flags.get(flag));
  assert.ok(workspace && catalog && settings && model, "ConPTY host requires --workspace, --settings-root, --catalog and --model");
  const operation = flags.get("--operation");
  return [binary, workspace, catalog, settings, model, ...(resume ? ["--resume"] : []),
    ...(operation === undefined ? [] : ["--operation", operation])];
}

// A stream's bytes go straight to its transcript file; nothing is decoded or
// kept in memory. Resolves once the file holds every byte. On a file failure
// the source keeps draining so the child is never blocked on a full pipe.
function transcript(stream, path) {
  const file = createWriteStream(path);
  stream.pipe(file);
  // A source destroyed without an end never ends its pipe: end the file so
  // its finish still arrives.
  stream.once("close", () => { if (!stream.readableEnded) file.end(); });
  return new Promise((resolve, reject) => {
    file.once("finish", resolve);
    file.once("error", error => { stream.unpipe(file); stream.resume(); reject(error); });
    stream.once("error", reject);
  });
}

// A settled process result. Its output is the explicit historical query of
// the transcript on disk, read when a caller asks for it.
function settled(code, signal, stdoutPath, stderrPath) {
  return {
    code, signal,
    get stdout() { return readFileSync(stdoutPath, "utf8"); },
    get stderr() { return readFileSync(stderrPath, "utf8"); },
    transcripts: { stdout: stdoutPath, stderr: stderrPath },
  };
}

// Why an event spool ended: its source reached EOF or failed, a line was
// not JSON, a unique key repeated, or the transcript could not be written.
export class SpoolEnded extends Error {
  constructor(kind, message, detail) {
    super(`event spool ${kind}: ${message}`);
    this.name = "SpoolEnded";
    this.kind = kind;
    this.detail = detail;
  }
}

// A JSONL event source that keeps no history in memory. Each event gets a
// sequence number and is appended to the transcript. A waiter registers
// before the event it waits for: `next` sees only later events, and `keyed`
// also answers from the index of the keys an event was recorded under. Each
// event costs one comparison per pending `next` and one index entry per
// key. `history` is the explicit scan of the transcript.
//
// The spool is a pipe destination: it takes the next chunk only once the
// transcript has drained, so queued bytes are bounded by one source chunk
// plus the stream buffers, not by the size of a burst. The transcript is a
// path, or any writable sink; a file stream's own path serves `history`.
//
// Every wait has a producer: each write's callback settles it, a transcript
// failure latches a typed `spool` cause that every current and later
// caller receives, and EOF follows the source's end or its close.
//
// The spool is open until one `#settle(cause)` ends it. Settling sets the
// terminal state first, then rejects every waiter, then releases a chunk
// held for drain; that chunk, and all later work, sees the terminal state
// and is dropped.
export class Spool {
  sequence = 0;
  #state = { open: true };
  #pending = new Set();
  #listeners = new Set();
  #index = new Map();
  #waiting = new Map();
  #key;
  #unique;
  #file;
  #path;
  #written = Promise.resolve();
  // Bytes of whole lines the transcript has finished writing. Writes finish
  // in order, so [0, completed) is always a run of complete lines.
  #completed = 0;
  #broken;
  #blocked;
  constructor(stream, closed, { transcript, key, unique = false } = {}) {
    this.#key = key; this.#unique = unique;
    if (typeof transcript === "string") { this.#path = transcript; this.#file = createWriteStream(transcript); }
    else { this.#file = transcript; this.#path = typeof transcript?.path === "string" ? transcript.path : undefined; }
    this.#file?.on("error", error => this.#break(error));
    const decoder = new StringDecoder("utf8");
    let partial = "";
    const lines = (text, done) => {
      const parts = (partial + text).split("\n");
      partial = parts.pop();
      let full = false;
      for (const line of parts) if (!this.#line(line.endsWith("\r") ? line.slice(0, -1) : line)) full = true;
      if (!full || !this.#state.open) { done(); return; }
      // Lines already in this chunk are written; the next chunk waits for drain.
      let released = false;
      this.#blocked = () => { if (!released) { released = true; this.#blocked = undefined; done(); } };
      // `#settle` releases it too, after the spool is terminal.
      this.#file.once("drain", this.#blocked);
    };
    const sink = new Writable({
      write: (chunk, _encoding, done) => lines(decoder.write(chunk), done),
      final: done => { const rest = partial + decoder.end(); partial = ""; rest ? lines(rest + "\n", done) : done(); },
    });
    // Every line taken, or a source that closed without ending.
    const drained = new Promise(resolve => {
      sink.once("finish", resolve);
      stream.once("close", () => { if (!stream.readableEnded) resolve(); });
    });
    stream.pipe(sink);
    stream.once("error", error => this.#settle(new SpoolEnded("source", error.message, error)));
    // EOF only after every line the source produced has been taken.
    closed.then(result => drained.then(() => this.#settle(new SpoolEnded("eof", `source exited: ${JSON.stringify({ code: result.code, signal: result.signal })}`, result))),
      error => this.#settle(new SpoolEnded("source", error.message, error)));
  }
  // Why the spool ended, or undefined while it is open.
  get ended() { return this.#state.cause; }
  // Takes one line; false when the transcript asks the source to wait.
  #line(line) {
    if (!this.#state.open) return true;
    let event;
    try { event = JSON.parse(line); } catch (error) {
      this.#settle(new SpoolEnded("malformed", `line ${this.sequence + 1} is not JSON: ${line.slice(0, 200)}`, error));
      return true;
    }
    this.sequence++;
    let room = true;
    if (this.#file) {
      const file = this.#file;
      const bytes = Buffer.from(line + "\n");
      this.#written = new Promise(resolve => {
        room = file.write(bytes, error => {
          // Only written bytes count; a failed write latches the cause.
          if (error) this.#break(error); else this.#completed += bytes.length;
          resolve();
        });
      });
    }
    for (const key of this.#key?.(event) ?? []) {
      if (this.#index.has(key)) {
        if (this.#unique) { this.#settle(new SpoolEnded("duplicate", `key ${key} repeated at event ${this.sequence}`, event)); return true; }
        continue;
      }
      this.#index.set(key, event);
      for (const wait of this.#waiting.get(key) ?? []) wait.resolve(event);
      this.#waiting.delete(key);
    }
    for (const listener of this.#listeners) listener(event);
    for (const wait of this.#pending) if (wait.match(event)) {
      this.#pending.delete(wait); wait.resolve(event);
    }
    return room;
  }
  // A transcript failure: latched for history even after another ending,
  // and the end of the spool.
  #break(error) {
    this.#broken ??= new SpoolEnded("spool", `transcript ${this.#path ?? "sink"}: ${error.message}`, error);
    this.#settle(this.#broken);
  }
  // The one transition out of open; later calls do nothing.
  #settle(cause) {
    if (!this.#state.open) return;
    this.#state = { open: false, cause };
    const pending = [...this.#pending, ...[...this.#waiting.values()].flat()];
    this.#pending.clear();
    this.#waiting.clear();
    for (const wait of pending) wait.reject(cause);
    const blocked = this.#blocked;
    this.#blocked = undefined;
    blocked?.();
    this.#file?.end();
  }
  fail(cause) { this.#settle(cause); }
  // The first event after this call that `match` accepts.
  next(match) {
    if (this.ended) return Promise.reject(this.ended);
    return new Promise((resolve, reject) => this.#pending.add({ match, resolve, reject }));
  }
  // The first event recorded under `key`, earlier or later.
  keyed(key) {
    if (this.#index.has(key)) return Promise.resolve(this.#index.get(key));
    if (this.ended) return Promise.reject(this.ended);
    return new Promise((resolve, reject) => {
      const waits = this.#waiting.get(key) ?? [];
      waits.push({ resolve, reject });
      this.#waiting.set(key, waits);
    });
  }
  has(key) { return this.#index.has(key); }
  subscribe(listener) {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }
  // Every event so far that `match` accepts, read from the transcript.
  async history(match = () => true) {
    assert.ok(this.#path, "event history requires a transcript");
    await this.#written;
    if (this.#broken) throw this.#broken;
    // Exactly the completed lines: a later write may already have put part
    // of its line in the file.
    const bytes = Buffer.alloc(this.#completed), fd = openSync(this.#path, "r");
    try {
      for (let at = 0; at < bytes.length;) {
        const read = readSync(fd, bytes, at, bytes.length - at, at);
        if (read === 0) throw new SpoolEnded("spool", `transcript ${this.#path} ends at byte ${at} of ${bytes.length} written`, { at, completed: bytes.length });
        at += read;
      }
    } finally { closeSync(fd); }
    return bytes.toString("utf8").split("\n").filter(Boolean).map(line => JSON.parse(line)).filter(match);
  }
}

// An incremental watch for fixed needles in a stream's text. It remembers
// which needles have appeared and where each last appeared, and keeps only
// the tail a needle could straddle, so each chunk costs its own length.
export class OutputWatch {
  #needles; #tail = ""; #offset = 0; #last = new Map(); #decoder = new StringDecoder("utf8"); #keep;
  constructor(needles) {
    this.#needles = [...new Set(needles)].filter(Boolean);
    this.#keep = Math.max(0, ...this.#needles.map(needle => needle.length - 1));
  }
  static of(text, needles) { const watch = new OutputWatch(needles); watch.write(text); return watch; }
  // Feeds bytes or text; returns this watch.
  write(chunk) {
    const text = typeof chunk === "string" ? chunk : this.#decoder.write(chunk);
    const combined = this.#tail + text, base = this.#offset - this.#tail.length;
    for (const needle of this.#needles) {
      for (let at = combined.indexOf(needle); at >= 0; at = combined.indexOf(needle, at + 1)) {
        if (base + at + needle.length > this.#offset) this.#last.set(needle, base + at);
      }
    }
    this.#offset += text.length;
    this.#tail = this.#keep ? combined.slice(-this.#keep) : "";
    return this;
  }
  has(needle) {
    assert.ok(this.#needles.includes(needle), `unwatched needle ${JSON.stringify(needle)}`);
    return this.#last.has(needle);
  }
  // Whether `needle` last appeared after `other` last did.
  later(needle, other) { return (this.#last.get(needle) ?? -1) > (this.#last.get(other) ?? -1); }
}

export function waitDetails(stderr) {
  return Array.from(stripVTControlCharacters(stderr).matchAll(/^waiting: (\S+) (\S+) since (\d+(?:\.\d+)?) \(.*\)\r?$/gm),
    match => ({ primitive: match[1], key: match[2], since: Number(match[3]) }));
}
