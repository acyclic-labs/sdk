#!/usr/bin/env node
// Installed process/presentation checks. Pipes are never claimed as PTY evidence.
import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { isAbsolute, join } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";

const [binary, session, ...extra] = process.argv.slice(2);
if (!binary || extra.length || !isAbsolute(binary) || !statSync(binary).isFile()
    || (session && (!isAbsolute(session) || !statSync(session).isFile()))) {
  throw new Error("usage: node scripts/test-graphcoder.mjs ABSOLUTE_INSTALLED_BINARY [ABSOLUTE_NATIVE_CONFIG]");
}
const digest = path => createHash("sha256").update(readFileSync(path)).digest("hex");
console.log(`binary=${binary}`);
console.log(`binary_sha256=${digest(binary)}`);
console.log(`suite_sha256=${digest(fileURLToPath(import.meta.url))}`);
// The installed binary receives no tool search path or environment credentials.
const env = { PATH: "" };
for (const key of ["SystemRoot", "WINDIR", "TEMP", "TMP"]) {
  if (process.env[key] !== undefined) env[key] = process.env[key];
}
function run(name, args, input, success) {
  const result = spawnSync(binary, args, {
    input, encoding: "utf8", env, windowsHide: true,
    timeout: 5_000, killSignal: "SIGKILL", maxBuffer: 1_048_576,
  });
  console.log(JSON.stringify({ case: name, argv: args, exit: result.status,
    signal: result.signal, stdout: result.stdout, stderr: result.stderr }));
  // Timeouts, launch failures and signal deaths never satisfy a negative case.
  if (result.error) throw result.error;
  assert.equal(result.signal, null, `${name}: terminated by signal`);
  assert.notEqual(result.status, null, `${name}: missing exit status`);
  assert.equal(result.status === 0, success, `${name}: unexpected exit`);
  return result;
}
const unavailableHost = run("default-unavailable", [], "", false);
assert.match(unavailableHost.stderr, /durable terminal host is unavailable/);
const args = ["--fixture", "wire", "--headless"];
const playback = run("explicit-playback", args, "/next\n/next\n/reset\n/next\n/quit\n", true);
assert.match(playback.stdout, /UI playback only, no durable effects/);
assert.equal(playback.stdout.split("activity ").length - 1, 2);
assert.equal(playback.stdout.split("end of fixture activity").length - 1, 1);
assert.match(playback.stdout, /fixture display reset; no session restored/);
assert.doesNotMatch(playback.stdout, /fixture> /);
const unavailableInput = run("input-not-queued", args, "hello\n/next\n", false);
assert.match(unavailableInput.stderr, /fixture input is not queued/);
assert.doesNotMatch(unavailableInput.stdout, /activity /);
const oversized = run("input-bound", args, "x".repeat(65_537) + "\n/next\n", false);
assert.match(oversized.stderr, /terminal input exceeds bounds/);
assert.doesNotMatch(oversized.stdout, /activity /);

if (session) {
  const config = JSON.parse(readFileSync(session, "utf8"));
  assert.ok(config.deterministic_delay_ms >= 1000 && config.deterministic_delay_ms <= 5000,
    "native fault qualification requires an explicitly bound 1000..5000ms dispatch delay");
  console.log(`native_config_sha256=${digest(session)}`);
  const temporary = mkdtempSync(join(tmpdir(), "graphcoder-installed-"));

  // Each finite child is the actual installed binary with real local providers.
  // This harness supplies no reducer, journal, provider outcome or fake terminal.
  class InstalledSession {
    constructor(name) {
      this.name = name;
      this.stdout = "";
      this.stderr = "";
      this.waiters = new Set();
      this.failure = null;
      this.finished = null;
      this.child = spawn(binary, ["--session", session, "--headless"], {
        env, windowsHide: true, stdio: ["pipe", "pipe", "pipe"],
      });
      this.child.stdout.setEncoding("utf8");
      this.child.stderr.setEncoding("utf8");
      const capture = (stream, chunk) => {
        this[stream] += chunk;
        if (this.stdout.length + this.stderr.length > 1_048_576) {
          this.failure = new Error(`${name}: captured output exceeds bounds`);
          this.child.kill("SIGKILL");
        }
        this.notify();
      };
      this.child.stdout.on("data", chunk => capture("stdout", chunk));
      this.child.stderr.on("data", chunk => capture("stderr", chunk));
      this.child.stdin.on("error", error => { this.failure = error; this.notify(); });
      this.exited = new Promise(resolve => {
        this.child.on("error", error => { this.failure = error; this.notify(); });
        this.child.on("close", (code, signal) => {
          clearTimeout(this.deadline);
          this.finished = { code, signal };
          console.log(JSON.stringify({ case: name, argv: ["--session", session, "--headless"],
            exit: code, signal, stdout: this.stdout, stderr: this.stderr }));
          this.notify();
          resolve(this.finished);
        });
      });
      this.deadline = setTimeout(() => {
        this.failure = new Error(`${name}: finite installed process deadline exceeded`);
        this.child.kill("SIGKILL");
        this.notify();
      }, 60_000);
    }
    notify() { for (const waiter of [...this.waiters]) waiter(); }
    expect(pattern, stream = "stdout", from = 0) {
      return new Promise((resolve, reject) => {
        const check = () => {
          if (this.failure) { this.waiters.delete(check); reject(this.failure); return; }
          const match = pattern.exec(this[stream].slice(from));
          if (match) { this.waiters.delete(check); resolve(match); return; }
          if (this.finished) {
            this.waiters.delete(check);
            reject(new Error(`${this.name}: exited before ${pattern}`));
          }
        };
        this.waiters.add(check);
        check();
      });
    }
    command(line, pattern) {
      const pending = this.expect(pattern, "stdout", this.stdout.length);
      this.child.stdin.write(`${line}\n`);
      return pending;
    }
    async quit() {
      this.child.stdin.end("/quit\n");
      const exit = await this.exited;
      if (this.failure) throw this.failure;
      assert.deepEqual(exit, { code: 0, signal: null });
    }
    async stop() {
      if (!this.finished) this.child.kill("SIGKILL");
      await this.exited;
    }
  }

  let active;
  try {
    active = new InstalledSession("native-recursive-queued-and-model-fault");
    await active.expect(/actual local Harness runtime/);
    const queue = text => active.command(text,
      /queued task=([0-9a-f-]+) admission-operation=([0-9a-f-]+)/);
    const root = await queue("root operation from actual installed stdin");
    await active.command(`/parent ${root[1]}`, /selected parent-task=/);
    const left = await queue("left child");
    const right = await queue("right child");
    await active.command(`/parent ${left[1]}`, /selected parent-task=/);
    const grandchild = await queue("grandchild");
    const tasks = [root[1], left[1], right[1], grandchild[1]];
    const operations = [root[2], left[2], right[2], grandchild[2]];
    assert.equal(new Set(tasks).size, 4, "distinct retained task identities");
    assert.equal(new Set(operations).size, 4, "distinct original admission identities");
    await active.command("/reopen", /providers reopened; no admission or effect resent/);
    for (const task of tasks) {
      await active.command(`/outcome ${task}`, /outcome=None/);
    }
    const children = join(temporary, "children.json");
    writeFileSync(children, JSON.stringify({ task: root[1], revision: null, after: null }));
    const childrenPage = await active.command(`/children ${children}`, /children parent-task=[^\n]+\n/);
    assert.ok(childrenPage[0].includes(left[1]) && childrenPage[0].includes(right[1]));
    assert.ok(!childrenPage[0].includes(grandchild[1]), "direct-child demand cannot hydrate grandchildren");

    const claim = await active.command(`/run ${root[1]}`, /worker-claim task=[^\n]+ lease-json-hex=([0-9a-f]+)/);
    // The provider barrier occurs after ordinary model-start journaling, before
    // delayed model content. Only the real installed process emits this barrier.
    await active.expect(/deterministic-dispatch operation=[0-9a-f-]+ step=0 request=[0-9a-f]+/, "stderr");
    // The prompt must remain responsive while an ordinary worker/provider is
    // active. Cancel a queued sibling through the real owner-bound task host.
    await active.command(`/cancel-task ${right[1]}`, /cancellation requested task=/);
    await active.command(`/outcome ${right[1]}`, /outcome=Some\(Cancelled\)/);
    await active.stop();
    if (active.failure) throw active.failure;
    assert.ok(!active.stdout.includes(`worker completed task=${root[1]}`),
      "fault must interrupt actual dispatch, not kill an already completed run");
    const lease = join(temporary, "original-lease.json");
    const originalLease = Buffer.from(claim[1], "hex");
    JSON.parse(originalLease.toString("utf8"));
    writeFileSync(lease, originalLease);

    active = new InstalledSession("native-original-lease-reconcile-and-complete");
    await active.expect(/actual local Harness runtime/);
    await active.command(`/reconcile ${root[2]}`, /retained=Some/);
    await active.command(`/resume ${lease}`, /worker completed task=/);
    assert.doesNotMatch(active.stderr, /deterministic-dispatch/,
      "interrupted original model attempt must reconcile, not generate again");
    for (const task of [left[1], grandchild[1]]) {
      await active.command(`/run ${task}`, /worker completed task=/);
    }
    await active.command(`/run ${right[1]}`, /worker idle task=/);
    const outcomes = [];
    for (const task of tasks) {
      const outcome = await active.command(`/outcome ${task}`, /outcome=Some\([^\n]+\n/);
      if (task === right[1]) assert.match(outcome[0], /Cancelled/);
      else assert.match(outcome[0], /Succeeded.*deterministic request=/);
      outcomes.push(outcome[0]);
    }
    await active.command("/reopen", /providers reopened; no admission or effect resent/);
    for (let index = 0; index < tasks.length; index++) {
      const after = await active.command(`/outcome ${tasks[index]}`, /outcome=Some\([^\n]+\n/);
      assert.equal(after[0], outcomes[index]);
      await active.command(`/run ${tasks[index]}`, /worker idle task=/);
    }
    await active.quit();

    active = new InstalledSession("native-process-reopen-does-not-repeat-effects");
    await active.expect(/actual local Harness runtime/);
    for (let index = 0; index < tasks.length; index++) {
      const after = await active.command(`/outcome ${tasks[index]}`, /outcome=Some\([^\n]+\n/);
      assert.equal(after[0], outcomes[index]);
      await active.command(`/run ${tasks[index]}`, /worker idle task=/);
    }
    assert.doesNotMatch(active.stderr, /deterministic-dispatch/);
    await active.quit();
  } finally {
    if (active) await active.stop();
    rmSync(temporary, { recursive: true, force: true });
  }
}
