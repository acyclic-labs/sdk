import { spawn } from "node:child_process";
import { createInterface } from "node:readline";

// Event-driven subprocesses: no polling or readiness delays. Every child is
// owned by the suite and is reaped even when another case fails.
export class Processes {
  children = new Map();
  stopping = false;
  start(program, args, options = {}) {
    if (this.stopping) throw new Error("suite process scope is closed (deadline or cleanup)");
    const { scopeKillSignal = "SIGKILL", ...spawnOptions } = options;
    const child = spawn(program, args, { windowsHide: true, stdio: ["pipe", "pipe", "pipe"], ...spawnOptions });
    child.stdin.on("error", error => { if (error.code !== "EPIPE") child.emit("error", error); });
    this.children.set(child, scopeKillSignal);
    let stdout = "", stderr = "";
    child.stdout.on("data", bytes => { stdout += bytes; });
    child.stderr.on("data", bytes => { stderr += bytes; });
    const closed = new Promise((resolve, reject) => {
      child.on("error", reject);
      child.on("close", (code, signal) => {
        this.children.delete(child);
        resolve({ code, signal, stdout, stderr });
      });
    });
    // The caller may be waiting for a ready/screen event instead of exit.
    closed.catch(() => {});
    return { child, closed };
  }
  async run(program, args, options, input = "") {
    const process = this.start(program, args, options);
    process.child.stdin.end(input);
    return process.closed;
  }
  async close() {
    this.stopping = true;
    const children = [...this.children.keys()];
    const exits = children.map(child => new Promise(resolve => child.once("close", resolve)));
    for (const child of children) child.kill(this.children.get(child));
    await Promise.all(exits);
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
