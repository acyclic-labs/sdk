import { spawn } from "node:child_process";
import { close, closeSync, createWriteStream, mkdtempSync, openSync, readFileSync, readSync, write, writev } from "node:fs";
import { tmpdir } from "node:os";
import { Writable } from "node:stream";
import { StringDecoder } from "node:string_decoder";
import assert from "node:assert/strict";
import { isAbsolute, join } from "node:path";
import { fileURLToPath } from "node:url";
import { ProcessReport } from "./process-report.mjs";
import { listenReportPipe } from "./report-pipe.mjs";

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
// onProgress is a synchronous metadata sink. Its first thrown failure is
// retained until closed, after the original transcript and physical joins.
// Node exit describes the launched process, which may itself be a host;
// the selected root's native completion receipt remains authoritative.
export class Processes {
  children = new Map();
  stopping = false;
  started = 0;
  #transcripts;
  #closing;
  #onReport;
  #onProgress;
  #progressFailure;
  #progressJoins;
  #admissions = new Set();
  constructor({ transcripts, onReport, onProgress } = {}) {
    this.#transcripts = transcripts; this.#onReport = onReport; this.#onProgress = onProgress;
    if (onProgress) this.#progressJoins = new Set();
  }
  get transcripts() { return this.#transcripts ??= mkdtempSync(join(tmpdir(), "e2e-transcripts-")); }
  start(program, args, options = {}) {
    if (process.platform === "win32" && options.scopeReport) {
      throw new Error("step=report-admission; recovery=await Processes.startReported before launching the selected Windows executable");
    }
    return this.#start(program, args, options);
  }
  startReported(program, args, options = {}) {
    const admission = this.#startReported(program, args, options);
    this.#admissions.add(admission);
    const settled = () => this.#admissions.delete(admission);
    admission.then(settled, settled);
    return admission;
  }
  async #startReported(program, args, options) {
    if (process.platform !== "win32") return this.start(program, args, { ...options, scopeReport: true });
    const report = new ProcessReport();
    const endpoint = await listenReportPipe(report.generation);
    try { return this.#start(program, args, options, report, endpoint); }
    catch (error) { await endpoint.close(); throw error; }
  }
  #start(program, args, options = {}, selectedReport, endpoint) {
    if (this.stopping) throw new Error("suite process scope is closed (deadline or cleanup)");
    const { scopeKillSignal = "SIGKILL", scopeTreeControl = false, scopeReport = false, scopeOperation, ...spawnOptions } = options;
    if (scopeOperation !== undefined && (typeof scopeOperation !== "string"
      || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(scopeOperation))) {
      throw new Error("step=process-progress-operation; recovery=pass the original opaque operation UUID as scopeOperation");
    }
    const report = selectedReport ?? (scopeReport ? new ProcessReport() : undefined);
    const env = { ...(spawnOptions.env ?? process.env) };
    for (const key of ["ACYCLIC_REPORT_FD", "ACYCLIC_REPORT_PIPE", "ACYCLIC_REPORT_OWNER_PID",
      "ACYCLIC_REPORT_GENERATION", "ACYCLIC_REPORT_REQUEST_ID"]) delete env[key];
    if (report) Object.assign(env, endpoint?.environment ?? { ACYCLIC_REPORT_FD: "3" }, {
      ACYCLIC_REPORT_GENERATION: report.generation, ACYCLIC_REPORT_REQUEST_ID: report.requestId,
    });
    const index = this.started, origin = this.#onProgress ? performance.now() : undefined;
    let progressFailure;
    const observe = this.#onProgress ? (phase, detail) => {
      const event = { schema: "acyclic.process-progress.v1", ordinal: index, operation: scopeOperation ?? null,
        report_generation: report?.generation ?? null, report_request_id: report?.requestId ?? null,
        phase, elapsed_seconds: (performance.now() - origin) / 1000, ...detail };
      try { this.#onProgress(event); }
      catch (cause) {
        progressFailure ??= new ProcessObservationFailed(event, cause);
        this.#progressFailure ??= progressFailure;
      }
    } : undefined;
    // Admit both original sinks before a child can act. The writers consume
    // these same handles; an admission failure cannot strand an unowned child.
    let stdoutPath, stderrPath, stdoutFd, stderrFd, child;
    try {
      stdoutPath = join(this.transcripts, `${index}.stdout`);
      stderrPath = join(this.transcripts, `${index}.stderr`);
      stdoutFd = openSync(stdoutPath, "wx", 0o600);
      stderrFd = openSync(stderrPath, "wx", 0o600);
    } catch (error) {
      if (stdoutFd !== undefined) closeSync(stdoutFd);
      throw new SpoolEnded("spool", `transcript admission: ${error.message}; recovery command: provide Processes({transcripts: writableFreshDirectory}) before launching`, error);
    }
    try {
      child = spawn(program, args, { windowsHide: true, stdio: ["pipe", "pipe", "pipe"], ...spawnOptions,
        env, ...(report && !endpoint ? { stdio: ["pipe", "pipe", "pipe", "pipe"] } : {}),
      });
    } catch (error) {
      closeSync(stdoutFd); closeSync(stderrFd);
      throw error;
    }
    report?.attach(child, endpoint?.connected ?? child.stdio[3]);
    const input = observe ? observedInput(child.stdin, observe) : child.stdin;
    if (observe) {
      child.once("spawn", () => observe("node-spawn"));
      child.stdin.once("finish", () => observe("stdin-finish"));
      child.stdin.once("close", () => observe("stdin-close"));
    }
    child.stdin.on("error", error => { if (error.code !== "EPIPE") child.emit("error", error); });
    const kill = () => {
      if (scopeTreeControl) { input.end(); return; }
      child.kill(scopeKillSignal);
    };
    this.started++;
    const written = Promise.all([transcript(child.stdout, stdoutFd, observe, "stdout"), transcript(child.stderr, stderrFd, observe, "stderr")]);
    // Exit is the owner-death barrier. Close may wait for inherited pipes in
    // surviving descendants, which recovery must be allowed to fence first.
    const exited = new Promise((resolve, reject) => {
      child.once("error", reject);
      child.once("exit", (code, signal) => { observe?.("node-exit", { code, signal }); resolve({ code, signal }); });
    });
    exited.catch(() => {});
    // Reaped once the child and its pipes are gone, however `closed` settles.
    const reaped = new Promise((resolve, reject) => {
      child.once("close", () => {
        observe?.("node-close");
        const finish = () => { this.children.delete(child); observe?.("reaped"); resolve(); };
        if (endpoint) report.closeTransport(endpoint.close).then(finish, error => {
          this.children.delete(child); reject(error);
        });
        else finish();
      });
    });
    reaped.catch(() => {});
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
        Promise.all([written, reaped]).then(() => {
          observe?.("closed");
          if (progressFailure) { reject(progressFailure); return; }
          const result = settled(code, signal, stdoutPath, stderrPath);
          if (report?.termination) result.termination = report.termination;
          resolve(result);
        }, reject);
      });
    });
    // The caller may be waiting for a ready/screen event instead of exit.
    closed.catch(() => {});
    if (this.#progressJoins) {
      // Retain only active original terminal joins. A child can be physically
      // reaped while its transcript and final observer are still pending.
      const joined = Promise.allSettled([written, reaped, closed]);
      this.#progressJoins.add(joined);
      joined.then(() => this.#progressJoins.delete(joined));
    }
    this.children.set(child, { kill, reaped, report,
      cancel: process.platform === "win32" ? undefined : () => child.kill("SIGTERM") });
    return { child, input, exited, closed, reportReady: report?.ready,
      requestReport: report ? deadline => report.request(deadline) : undefined,
      killExecutable: () => child.kill("SIGKILL") };
  }
  async run(program, args, options, input = "") {
    const process = options?.scopeReport ? await this.startReported(program, args, options) : this.start(program, args, options);
    process.input.end(input);
    return process.closed;
  }
  // Windows Job custody kills the whole tree with its host; POSIX custody
  // kills only the executable on SIGUSR1 and still drains and reports it.
  startOwned(program, args, options = {}) {
    return this.#startOwned(program, args, options, this.start.bind(this));
  }
  startReportedOwned(program, args, options = {}) {
    return this.#startOwned(program, args, options, this.startReported.bind(this));
  }
  #startOwned(program, args, options, start) {
    const { scopeMountRoot, scopeInheritInput = false, scopePty = false, ...spawnOptions } = options;
    if (process.platform === "win32") {
      const host = options.env?.E2E_PROCESS_HOST ?? process.env.E2E_PROCESS_HOST;
      assert.ok(host && isAbsolute(host), "E2E_PROCESS_HOST must name the cross-built process-tree host");
      return start(host, [...(scopeInheritInput ? ["--inherit-input"] : []), program, ...args],
        { ...spawnOptions, scopeTreeControl: !scopeInheritInput });
    }
    const mountLease = process.platform === "linux" && scopeMountRoot
      ? ["--mount-root", scopeMountRoot, "--"] : [];
    const owned = start("python3", [fileURLToPath(new URL("./owned-process.py", import.meta.url)),
      ...(scopePty ? ["--pty"] : []), ...mountLease, program, ...args], { ...spawnOptions, scopeTreeControl: true });
    if (owned instanceof Promise) return owned.then(running => ({ ...running, killExecutable: () => running.child.kill("SIGUSR1") }));
    return { ...owned, killExecutable: () => owned.child.kill("SIGUSR1") };
  }
  // The one real-terminal transport: ConPTY inside native Job custody on
  // Windows, or a PTY whose root leads its own session under the POSIX owner.
  startTerminal(binary, args, options = {}) {
    const env = { ...(options.env ?? process.env), TERM: "xterm-256color" };
    const start = options.scopeReport ? this.startReportedOwned.bind(this) : this.startOwned.bind(this);
    if (process.platform !== "win32") return start(binary, args, { ...options, env, scopePty: true });
    const host = env.E2E_CONPTY_HOST;
    assert.ok(host && isAbsolute(host), "cross-build the ConPTY host and set E2E_CONPTY_HOST");
    return start(host, conptyArgs(binary, args), { ...options, env, scopeInheritInput: true });
  }
  async runOwned(program, args, options, input = "") {
    assert.equal(input, "", "external acceptance commands receive empty stdin");
    const running = this.startOwned(program, args, options);
    const [, result] = await Promise.all([running.exited, running.closed]);
    return result;
  }
  close() {
    return this.#closing ??= this.#close();
  }
  async #close() {
    this.stopping = true;
    await Promise.allSettled(this.#admissions);
    const children = [...this.children.values()];
    const reporting = children.filter(child => child.report);
    let observationError = this.#progressFailure;
    const observed = (report, outcome) => {
      try {
        this.#onReport?.({ request_id: report.requestId, generation: report.generation,
          outcome, acknowledged: report.termination?.report_acknowledged === true,
          cause: report.termination?.report_cause });
      } catch (error) { observationError ??= this.#progressFailure ?? error; }
    };
    try {
      if (reporting.length) {
        // The active deadline already failed. The existing drain reservation
        // permits diagnostics before the same physical termination barrier.
        let force;
        const forced = new Promise(resolve => { force = setTimeout(resolve, 500); });
        const deadline = Date.now() + 500;
        try {
          const acknowledgements = reporting.map(child => {
            const completion = child.report.request(deadline);
            if (!this.#onReport) return completion;
            observed(child.report, "requested");
            return completion.then(() => observed(child.report, child.report.termination.report_outcome));
          });
          await Promise.race([Promise.all(acknowledgements), forced]);
          for (const child of reporting) { child.report.expire(); child.cancel?.(); }
          await Promise.race([Promise.all(reporting.map(child => child.reaped)), forced]);
        } finally { clearTimeout(force); }
      }
    } finally {
      for (const child of children) child.kill();
      await Promise.all(children.map(child => child.reaped));
      if (this.#progressJoins) await Promise.all(this.#progressJoins);
    }
    if (observationError ?? this.#progressFailure) throw observationError ?? this.#progressFailure;
  }
}

// Only this launch's original write/end calls are observed. A completed Node
// write is pipe acceptance, not child consumption; finish/close are not proof
// that a nested child received EOF. Raw child.stdin calls bypass this port.
function observedInput(stream, observe) {
  let first = true, ended = false;
  const completed = callback => error => {
    observe(error ? "stdin-write-failed" : "stdin-write-complete", error ? { error_code: error.code ?? null } : undefined);
    callback?.(error);
  };
  return {
    write(chunk, encoding, callback) {
      if (typeof encoding === "function") { callback = encoding; encoding = undefined; }
      const initial = first && chunk?.length > 0;
      const result = stream.write(chunk, encoding, initial ? completed(callback) : callback);
      if (initial) { first = false; observe("stdin-write-admitted"); }
      return result;
    },
    end(chunk, encoding, callback) {
      if (typeof chunk === "function") { callback = chunk; chunk = undefined; encoding = undefined; }
      else if (typeof encoding === "function") { callback = encoding; encoding = undefined; }
      const initial = first && chunk?.length > 0;
      const result = stream.end(chunk, encoding, initial ? completed(callback) : callback);
      if (initial) { first = false; observe("stdin-write-admitted"); }
      if (!ended) { ended = true; observe("stdin-end-submitted"); }
      return result;
    },
  };
}

export class ProcessObservationFailed extends Error {
  constructor(event, cause) {
    super(`process observation failed at ${event.phase}`, { cause });
    this.name = "ProcessObservationFailed";
    this.code = "PROCESS_OBSERVATION_FAILED";
    this.ordinal = event.ordinal; this.operation = event.operation; this.phase = event.phase;
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
function transcript(stream, fd, observe, channel) {
  let first = true;
  const completed = observe ? callback => (error, count, buffers) => {
    if (!error && count > 0 && first) { first = false; observe(`${channel}-first-write`); }
    callback(error, count, buffers);
  } : undefined;
  // Observe the SAME file writer's first successful byte transfer. This is
  // transcript write completion, not the OS pipe-read timestamp. Later writes
  // use the original callback directly; no reader, payload copy or history.
  const file = createWriteStream(null, { fd, ...(observe ? { fs: {
    close,
    write(fd, buffer, offset, length, position, callback) {
      return write(fd, buffer, offset, length, position, first ? completed(callback) : callback);
    },
    writev(fd, buffers, position, callback) {
      return writev(fd, buffers, position, first ? completed(callback) : callback);
    },
  } } : {}) });
  stream.pipe(file);
  if (observe) stream.once("end", () => observe(`${channel}-eof`));
  // A source destroyed without an end never ends its pipe: end the file so
  // its finish still arrives.
  stream.once("close", () => { observe?.(`${channel}-close`); if (!stream.readableEnded) file.end(); });
  return new Promise((resolve, reject) => {
    file.once("finish", observe ? () => { observe(`${channel}-finish`); resolve(); } : resolve);
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
  #reads = new Set();
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
  #position = 0;
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
    let indexed = event;
    if (this.#file) {
      const file = this.#file;
      const bytes = Buffer.from(line + "\n");
      const at = this.#position;
      this.#position += bytes.length;
      this.#written = new Promise(resolve => {
        room = file.write(bytes, error => {
          // Only written bytes count; a failed write latches the cause.
          if (error) this.#break(error); else this.#completed += bytes.length;
          resolve();
        });
      });
      // The transcript already owns the payload. Keep only its exact range;
      // a late keyed lookup reads that event after its original write finishes.
      if (this.#path) indexed = { at, length: bytes.length, written: this.#written };
    }
    for (const key of this.#key?.(event) ?? []) {
      if (this.#index.has(key)) {
        if (this.#unique) { this.#settle(new SpoolEnded("duplicate", `key ${key} repeated at event ${this.sequence}`, event)); return true; }
        continue;
      }
      this.#index.set(key, indexed);
      for (const wait of this.#waiting.get(key) ?? []) wait.resolve(event);
      this.#waiting.delete(key);
    }
    for (const listener of this.#listeners) listener(event);
    for (const wait of this.#pending) if (wait.match?.(event)) {
      this.#pending.delete(wait); wait.resolve(event);
    }
    return room;
  }
  // A transcript failure: latched for history even after another ending,
  // and the end of the spool.
  #break(error) {
    this.#broken ??= new SpoolEnded("spool", `transcript ${this.#path ?? "sink"}: ${error.message}`, error);
    for (const wait of this.#reads) wait.reject(this.#broken);
    this.#settle(this.#broken);
  }
  // The one transition out of open; later calls do nothing.
  #settle(cause) {
    if (!this.#state.open) return;
    this.#state = { open: false, cause };
    const pending = [...this.#pending, ...[...this.#waiting.values()].flatMap(waits => [...waits])];
    this.#pending.clear();
    this.#waiting.clear();
    for (const wait of pending) wait.reject(cause);
    const blocked = this.#blocked;
    this.#blocked = undefined;
    blocked?.();
    this.#file?.end();
  }
  fail(cause) { this.#settle(cause); }
  #wait(waits, signal, match, start, cleanup) {
    if (signal?.aborted) return Promise.reject(new SpoolEnded("cancelled", "wait cancelled", signal.reason));
    return new Promise((resolve, reject) => {
      let active = true;
      const finish = (settle, value) => {
        if (!active) return;
        active = false;
        waits.delete(wait); cleanup?.();
        signal?.removeEventListener("abort", abort);
        settle(value);
      };
      const wait = { match, resolve: value => finish(resolve, value), reject: error => finish(reject, error) };
      const abort = () => wait.reject(new SpoolEnded("cancelled", "wait cancelled", signal.reason));
      waits.add(wait);
      signal?.addEventListener("abort", abort, { once: true });
      start?.(wait);
    });
  }
  // The first event after this call that `match` accepts.
  next(match, { signal } = {}) {
    if (this.ended) return Promise.reject(this.ended);
    return this.#wait(this.#pending, signal, match);
  }
  // The first event recorded under `key`, earlier or later.
  keyed(key, { signal } = {}) {
    if (signal?.aborted) return Promise.reject(new SpoolEnded("cancelled", "wait cancelled", signal.reason));
    if (this.#index.has(key)) {
      const indexed = this.#index.get(key);
      if (!this.#path) return Promise.resolve(indexed);
      return this.#wait(this.#reads, signal, undefined, wait => {
        indexed.written.then(() => wait.resolve());
      }).then(() => this.#read(indexed));
    }
    if (this.ended) return Promise.reject(this.ended);
    const waits = this.#waiting.get(key) ?? new Set();
    this.#waiting.set(key, waits);
    return this.#wait(waits, signal, undefined, undefined, () => {
      if (waits.size === 0) this.#waiting.delete(key);
    });
  }
  #read({ at, length }) {
    if (this.#broken) throw this.#broken;
    const bytes = Buffer.allocUnsafe(length), fd = openSync(this.#path, "r");
    try {
      for (let read = 0; read < length;) {
        const size = readSync(fd, bytes, read, length - read, at + read);
        if (size === 0) throw new SpoolEnded("spool", `transcript ${this.#path} ends before indexed event at byte ${at}`, { at, length });
        read += size;
      }
    } finally { closeSync(fd); }
    try { return JSON.parse(bytes.toString("utf8")); }
    catch (error) { throw new SpoolEnded("spool", `transcript ${this.#path} has an invalid indexed event at byte ${at}`, error); }
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
