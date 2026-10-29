import assert from "node:assert/strict";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { Processes, Spool, OutputWatch, conptyArgs } from "./process.mjs";
import { alive } from "./orphan-check.mjs";

const fixture = fileURLToPath(new URL("./terminal-fixture.py", import.meta.url));
const posix = { skip: process.platform === "win32", timeout: 5000 };
// Output after this call that contains `text`; registered in the same tick as
// the input that prompts it.
const shown = (running, text) => new Promise((resolve, reject) => {
  const watch = new OutputWatch([text]);
  const check = bytes => { if (watch.write(bytes).has(text)) { running.child.stdout.off("data", check); resolve(); } };
  running.child.stdout.on("data", check);
  running.closed.then(result => reject(new Error(`closed before ${text}: ${JSON.stringify({ code: result.code, signal: result.signal })}`)));
});

test("ConPTY launch contract carries the shared terminal arguments and refuses others", () => {
  const args = ["--workspace", "W", "--settings-root", "S", "--catalog", "C", "--model", "m/x"];
  assert.deepEqual(conptyArgs("B", args), ["B", "W", "C", "S", "m/x"]);
  assert.deepEqual(conptyArgs("B", ["--operation", "id", ...args, "--resume"]).slice(5), ["--resume", "--operation", "id"]);
  // Negative controls: the native host has no channel for these.
  assert.throws(() => conptyArgs("B", [...args, "--operation", "a", "--operation", "b"]), /cannot launch argument --operation/);
  assert.throws(() => conptyArgs("B", [...args, "--operation"]), /cannot launch argument --operation/);
  assert.throws(() => conptyArgs("B", [...args, "-p", "x"]), /cannot launch argument -p/);
  assert.throws(() => conptyArgs("B", args.slice(2)), /requires --workspace/);
  assert.throws(() => conptyArgs("B", [...args, "--model", "n/y"]), /cannot launch argument --model/);
});

const conpty = {
  skip: process.platform !== "win32" || !process.env.E2E_CONPTY_HOST || !process.env.E2E_PROCESS_HOST,
  timeout: 10000,
};
const cmd = `${process.env.SystemRoot}\\System32\\cmd.exe`;
const launch = ["--workspace", process.cwd(), "--settings-root", process.cwd(), "--catalog", process.cwd(), "--model", "m/x"];

test("real ConPTY carries typed options, raw input, output and the exit code", conpty, async t => {
  const processes = new Processes();
  try {
    const running = processes.startTerminal(cmd, [...launch, "--resume", "--operation", "op 1"]);
    await shown(running, ">");
    running.child.stdin.write("echo po^ng %CMDCMDLINE%\r\nexit 7\r\n");
    const result = await bounded(t, running.closed);
    assert.equal(result.code, 7, result.stderr);
    assert.match(result.stdout.replace(/\x1b\[[0-9;?]*[A-Za-z]|\r|\n/g, ""), /pong .*--resume --operation "op 1"/);
  } finally { await processes.close(); }
});

test("ConPTY refuses launches outside admitted native custody", conpty, async t => {
  const processes = new Processes();
  const host = process.env.E2E_CONPTY_HOST;
  try {
    const direct = await bounded(t, processes.start(host, conptyArgs(cmd, launch)).closed);
    assert.equal(direct.code, 125);
    assert.match(direct.stderr, /step=conpty-native-custody/);
    const untyped = await bounded(t, processes.startOwned(host, [...conptyArgs(cmd, launch), "-p", "x"], { scopeInheritInput: true }).closed);
    assert.equal(untyped.code, 125);
    assert.match(untyped.stderr, /step=conpty-arguments/);
    const binary = await bounded(t, processes.startTerminal(`${host}.missing.exe`, launch).closed);
    assert.equal(binary.code, 125);
    assert.match(binary.stderr, /step=conpty-terminal-create/);
    process.env.E2E_CONPTY_HOST = `${host}.missing.exe`;
    const admission = await bounded(t, processes.startTerminal(cmd, launch).closed);
    assert.equal(admission.code, 125);
    assert.match(admission.stderr, /step=process-host-admission/);
  } finally { process.env.E2E_CONPTY_HOST = host; await processes.close(); }
});

const witnessed = { stdio: ["pipe", "pipe", "pipe", "pipe"] };
const released = running => { running.child.stdio[3].resume(); return once(running.child.stdio[3], "end"); };
// A regression fails at the test deadline and still reaches cleanup.
const bounded = (t, promise) => Promise.race([promise, new Promise((_, reject) =>
  t.signal.addEventListener("abort", () => reject(t.signal.reason), { once: true }))]);

test("terminal root leads a sized session, takes raw input and drains output after exit", posix, async t => {
  const processes = new Processes();
  try {
    const running = processes.startTerminal("python3", [fixture, "probe"]);
    const probe = await new Spool(running.child.stdout, running.closed).next(event => "tty" in event);
    // Owner budget: exactly one owner process between the test and the executable.
    assert.deepEqual(probe, { tty: true, size: [80, 24], leader: true, foreground: true, parent: running.child.pid });
    // The interrupt reaches the root's foreground group, not its owner.
    running.child.stdin.write("\x03");
    await shown(running, "interrupted");
    running.child.stdin.write("ping\r");
    const result = await bounded(t, running.closed);
    assert.equal(result.code, 3, result.stderr);
    assert.match(result.stdout, /got ping/);
    assert.ok(result.stdout.includes("x".repeat(100000)) && result.stdout.includes("end"), "terminal output was lost at exit");
  } finally { await processes.close(); }
});

test("a second launcher inside the terminal fails the owner budget", posix, async t => {
  const processes = new Processes();
  try {
    // Negative control: a forking shell between owner and executable, like the old script(1) path.
    const running = processes.startTerminal("/bin/sh", ["-c", `python3 '${fixture}' probe; exit $?`]);
    const probe = await bounded(t, new Spool(running.child.stdout, running.closed).next(event => "tty" in event));
    assert.notEqual(probe.parent, running.child.pid);
  } finally { await processes.close(); }
});

for (const escape of ["group", ...(process.platform === "linux" ? ["session"] : [])]) {
  test(`killed executable fails only after its escaped ${escape} is drained`, posix, async t => {
    const processes = new Processes();
    let pids = [];
    try {
      // Negative control: without custody the escaped holder outlives the kill and pins the pipe.
      const raw = processes.start("python3", [fixture, escape], witnessed);
      pids = (await new Spool(raw.child.stdout, raw.closed).next(event => event.pids)).pids;
      let closed = false;
      raw.closed.then(() => { closed = true; });
      raw.killExecutable();
      await raw.exited;
      assert.equal(alive(pids[1]), true);
      assert.equal(closed, false, "a held output pipe must not look drained");
      process.kill(pids[1], "SIGKILL");
      await raw.closed;

      const running = processes.startTerminal("python3", [fixture, escape], witnessed);
      const holders = released(running);
      pids = (await new Spool(running.child.stdout, running.closed).next(event => event.pids)).pids;
      running.killExecutable();
      const result = await bounded(t, running.closed);
      assert.equal(result.code, 128 + 9, "the owner reports the executable's SIGKILL after draining");
      await bounded(t, holders);
      pids = [];
    } finally {
      for (const pid of pids) try { process.kill(pid, "SIGKILL"); } catch (error) { if (error.code !== "ESRCH") throw error; }
      await processes.close();
    }
  });
}

test("terminal owner death fails the run and hangs up its executable", posix, async t => {
  const processes = new Processes();
  try {
    const running = processes.startTerminal("python3", [fixture, "probe"], witnessed);
    const executable = released(running);
    await new Spool(running.child.stdout, running.closed).next(event => "tty" in event);
    running.child.kill("SIGKILL");
    const result = await running.closed;
    assert.equal(result.signal, "SIGKILL", "wrapper death must not report the executable's success");
    await bounded(t, executable);
  } finally { await processes.close(); }
});
