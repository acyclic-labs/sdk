import { spawn } from "node:child_process";
import { createInterface } from "node:readline";
import { stripVTControlCharacters } from "node:util";
import { StringDecoder } from "node:string_decoder";
import assert from "node:assert/strict";
import { isAbsolute } from "node:path";
import { fileURLToPath } from "node:url";

export async function schedule(items, platform, execute) {
  if (platform !== "win32") return Promise.all(items.map(execute));
  const results = [];
  for (const item of items) results.push(await execute(item));
  return results;
}

// Event-driven subprocesses: no polling or readiness delays. Every child is
// owned by the suite and is reaped even when another case fails.
export class Processes {
  children = new Map();
  stopping = false;
  start(program, args, options = {}) {
    if (this.stopping) throw new Error("suite process scope is closed (deadline or cleanup)");
    const { scopeKillSignal = "SIGKILL", scopeTreeControl = false, scopeReportSignal, ...spawnOptions } = options;
    const child = spawn(program, args, { windowsHide: true, stdio: ["pipe", "pipe", "pipe"], ...spawnOptions });
    child.stdin.on("error", error => { if (error.code !== "EPIPE") child.emit("error", error); });
    const kill = () => {
      if (scopeTreeControl) { child.stdin.end(); return; }
      child.kill(scopeKillSignal);
    };
    let stdout = "", stderr = "", reportRequested = false;
    const outputDecoder = new StringDecoder("utf8"), errorDecoder = new StringDecoder("utf8");
    child.stdout.on("data", bytes => { stdout += outputDecoder.write(bytes); });
    child.stderr.on("data", bytes => { stderr += errorDecoder.write(bytes); });
    // Exit is the owner-death barrier. Close may wait for inherited pipes in
    // surviving descendants, which recovery must be allowed to fence first.
    const exited = new Promise((resolve, reject) => {
      child.once("error", reject);
      child.once("exit", (code, signal) => resolve({ code, signal }));
    });
    exited.catch(() => {});
    const closed = new Promise((resolve, reject) => {
      child.on("error", reject);
      child.on("close", (code, signal) => {
        this.children.delete(child);
        stdout += outputDecoder.end(); stderr += errorDecoder.end();
        const stuck_on = reportRequested ? waitDetails(stderr + "\n" + stdout) : [];
        resolve({ code, signal, stdout, stderr, ...(reportRequested ? { termination: { report_requested: true, stuck_on, missing_wait_report: stuck_on.length === 0 } } : {}) });
      });
    });
    // The caller may be waiting for a ready/screen event instead of exit.
    closed.catch(() => {});
    this.children.set(child, { kill, closed, report: scopeReportSignal ? () => { reportRequested = true; child.kill(scopeReportSignal); } : undefined });
    return { child, exited, closed, killExecutable: () => child.kill("SIGKILL"), get stdout() { return stdout; } };
  }
  async run(program, args, options, input = "") {
    const process = this.start(program, args, options);
    process.child.stdin.end(input);
    return process.closed;
  }
  // Windows Job custody kills the whole tree with its host; POSIX custody
  // kills only the executable on SIGUSR1 and still drains and reports it.
  startOwned(program, args, options = {}) {
    const { scopeMountRoot, scopeInheritInput = false, scopePty = false, scopeRows = 24, scopeCols = 80, ...spawnOptions } = options;
    if (process.platform === "win32") {
      const host = options.env?.E2E_PROCESS_HOST ?? process.env.E2E_PROCESS_HOST;
      assert.ok(host && isAbsolute(host), "E2E_PROCESS_HOST must name the cross-built process-tree host");
      return this.start(host, [...(scopeInheritInput ? ["--inherit-input"] : []), program, ...args],
        { ...spawnOptions, scopeTreeControl: !scopeInheritInput });
    }
    const mountLease = process.platform === "linux" && scopeMountRoot
      ? ["--mount-root", scopeMountRoot, "--"] : [];
    const owned = this.start("python3", [fileURLToPath(new URL("./owned-process.py", import.meta.url)),
      ...(scopePty ? ["--pty", String(scopeRows), String(scopeCols)] : []), ...mountLease, program, ...args], { ...spawnOptions, scopeTreeControl: true });
    return { ...owned, killExecutable: () => owned.child.kill("SIGUSR1"), get stdout() { return owned.stdout; } };
  }
  // The one real-terminal transport: ConPTY inside native Job custody on
  // Windows, or a PTY whose root leads its own session under the POSIX owner.
  startTerminal(binary, args, options = {}) {
    const { rows = 24, cols = 80, ...spawnOptions } = options;
    const env = { ...(spawnOptions.env ?? process.env), TERM: "xterm-256color" };
    if (process.platform !== "win32") {
      terminalSize(rows, cols);
      return this.startOwned(binary, args, { scopeReportSignal: "SIGTERM", ...spawnOptions, env, scopePty: true, scopeRows: rows, scopeCols: cols });
    }
    const host = env.E2E_CONPTY_HOST;
    assert.ok(host && isAbsolute(host), "cross-build the ConPTY host and set E2E_CONPTY_HOST");
    return this.startOwned(host, conptyArgs(binary, args, { rows, cols }), { ...spawnOptions, env, scopeInheritInput: true });
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
      for (const child of reporting) child.report();
      try { await Promise.race([Promise.all(reporting.map(child => child.closed)), forced]); }
      finally { clearTimeout(force); }
    }
    for (const child of children) child.kill();
    await Promise.all(children.map(child => child.closed));
  }
}

// The ConPTY host admits the launch paths plus typed options; the case owner holds the only deadline.
export function conptyArgs(binary, args, { rows = 24, cols = 80 } = {}) {
  terminalSize(rows, cols);
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
    ...(operation === undefined ? [] : ["--operation", operation]), "--rows", String(rows), "--cols", String(cols)];
}

function terminalSize(rows, cols) {
  for (const [name, value] of [["rows", rows], ["cols", cols]]) {
    assert.ok(Number.isInteger(value) && value > 0 && value <= 32767,
      `step=conpty-arguments; invalid ${name}; recovery=use a ${name} integer in 1..32767`);
  }
}

export class Events {
  seen = [];
  pending = new Set();
  listeners = new Set();
  ended;
  constructor(stream, closed) {
    createInterface({ input: stream }).on("line", line => {
      try {
        const event = JSON.parse(line);
        this.seen.push(event);
        for (const listener of this.listeners) listener(event);
        for (const wait of [...this.pending]) if (wait.match(event)) {
          this.pending.delete(wait); wait.resolve(event);
        }
      } catch (error) { this.fail(error); }
    });
    closed.then(result => this.fail(new Error(`event source exited: ${JSON.stringify(result)}`)), error => this.fail(error));
  }
  fail(error) {
    this.ended = error;
    for (const wait of this.pending) wait.reject(error);
    this.pending.clear();
  }
  wait(match) {
    const event = this.seen.find(match);
    if (event) return Promise.resolve(event);
    if (this.ended) return Promise.reject(this.ended);
    return new Promise((resolve, reject) => this.pending.add({ match, resolve, reject }));
  }
  subscribe(listener) {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
}

export function waitDetails(stderr) {
  return Array.from(stripVTControlCharacters(stderr).matchAll(/^waiting: (\S+) (\S+) since (\d+(?:\.\d+)?) \(.*\)\r?$/gm),
    match => ({ primitive: match[1], key: match[2], since: Number(match[3]) }));
}
