import assert from "node:assert/strict";
import { finished } from "node:stream/promises";
import { fileURLToPath } from "node:url";
import { createServer } from "node:net";
import { once } from "node:events";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { Processes, Spool, OutputWatch } from "./process.mjs";
import { alive } from "./orphan-check.mjs";
import { Terminal } from "../graphcoder-pty/screen.mjs";

const fixture = fileURLToPath(new URL("./terminal-fixture.py", import.meta.url));
const posix = { skip: process.platform === "win32", timeout: 5000 };
// Output after this call that contains `text`; registered in the same tick as
// the input that prompts it.
const shown = async (running, text, source = running.child.stdout) => {
  const watch = new OutputWatch([text]);
  const output = source.iterator({ destroyOnReturn: false });
  try {
    for (;;) {
      const next = await output.next();
      if (next.done) throw Object.assign(new Error(
        `terminal output ended before ${JSON.stringify(text)}; step=terminal-output; recovery=node --test scripts/graphcoder-e2e/terminal.test.mjs`,
      ), { code: "TERMINAL_OUTPUT_EOF", expected: text });
      if (watch.write(next.value).has(text)) return;
    }
  } finally { await output.return(); }
};

test("terminal output consumes the original source and refuses early EOF", async t => {
  const processes = new Processes();
  t.after(() => processes.close());
  const running = processes.start(process.execPath, ["-e",
    'process.stdin.once("data", () => process.stdout.write("interrupted")); process.stdin.resume();']);
  const visible = shown(running, "interrupted");
  running.child.stdin.write("show");
  await visible;
  running.child.stdin.end();
  assert.equal((await running.closed).code, 0);

  const lost = processes.start(process.execPath, ["-e", "process.exit(3)"]);
  await assert.rejects(shown(lost, "interrupted"),
    error => error.code === "TERMINAL_OUTPUT_EOF" && error.expected === "interrupted");
  assert.equal((await lost.closed).code, 3);
});

const conpty = {
  skip: process.platform !== "win32" || !process.env.E2E_CONPTY_HOST || !process.env.E2E_PROCESS_HOST,
  timeout: 10000,
};
const cmd = `${process.env.SystemRoot}\\System32\\cmd.exe`;
const launch = ["/D", "/Q"];

test("real ConPTY forwards native argv, raw input, output and the exit code", conpty, async t => {
  const processes = new Processes();
  try {
    const args = ["--resume", "--operation", "op 1", "", 'embedded"quote', "trailing\\", "日本語 é"];
    const program = `
      process.stdin.setRawMode(true);
      let input = '';
      process.stdin.on('data', bytes => {
        input += bytes.toString('utf8');
        if (!input.includes('\\r')) return;
        console.log(JSON.stringify({args: process.argv.slice(1), input}));
        process.exit(7);
      });
      process.stdin.resume();
      process.stdout.write('ready>');
    `;
    const running = await processes.startTerminal(process.execPath, ["-e", program, "--", ...args]);
    await shown(running, ">");
    running.child.stdin.write("pong\r");
    const result = await bounded(t, running.closed);
    assert.equal(result.code, 7, result.stderr);
    const plain = result.stdout.replace(/\x1b\[[0-9;?]*[A-Za-z]|\r|\n/g, "");
    const record = JSON.parse(plain.slice(plain.indexOf('{'), plain.lastIndexOf('}') + 1));
    assert.deepEqual(record.args, args);
    assert.equal(record.input, "pong\r");
  } finally { await processes.close(); }
});

test("ConPTY joins idle and partial resize reads when the child exits before its control peer", conpty, async t => {
  for (const partial of [false, true]) {
    const processes = new Processes(), server = createServer();
    const endpoint = `\\\\.\\pipe\\acyclic-terminal-exit-${randomUUID()}`;
    let peer;
    try {
      const connected = once(server, "connection");
      server.listen(endpoint);
      await once(server, "listening");
      const running = await processes.startOwned(process.env.E2E_CONPTY_HOST, [process.execPath, "-e", `
        process.stdin.setRawMode(true);
        process.stdin.once('data', () => { process.stdout.write('drained-before-exit'); process.exit(7); });
        process.stdin.resume(); process.stdout.write('ready>');
      `], { scopeInheritInput: true, env: { ...process.env, E2E_PTY_CONTROL: endpoint } });
      const ready = shown(running, "ready>");
      [peer] = await bounded(t, connected);
      // Keep the original peer connected until physical completion. Closing it
      // first would hide the synchronous-read shutdown deadlock.
      const eof = finished(peer, { readable: true, writable: false, cleanup: true });
      eof.catch(() => {});
      peer.resume();
      await bounded(t, ready);
      if (partial) peer.write(Buffer.from([80]));
      running.input.write("quit\r");
      const result = await bounded(t, running.closed);
      assert.equal(result.code, 7, result.stderr);
      assert.equal(result.completion.process_tree_retired, true);
      assert.match(result.stdout, /drained-before-exit/);
      await bounded(t, eof);
    } finally {
      peer?.destroy();
      await processes.close();
      await new Promise(resolve => server.close(resolve));
    }
  }
});

test("ConPTY refuses launches outside admitted native custody", conpty, async t => {
  const processes = new Processes();
  const host = process.env.E2E_CONPTY_HOST;
  let admissionFailure;
  try {
    const direct = await bounded(t, processes.start(host, [cmd, ...launch]).closed);
    assert.equal(direct.code, 125);
    assert.match(direct.stderr, /step=conpty-native-custody/);
    const binaryLaunch = await processes.startTerminal(`${host}.missing.exe`, launch);
    const binary = await bounded(t, binaryLaunch.closed);
    assert.equal(binary.code, 125);
    assert.match(binary.stderr, /step=conpty-terminal-create/);
    const admissionLaunch = await processes.startTerminal(cmd, launch, {
      env: { ...process.env, E2E_CONPTY_HOST: `${host}.missing.exe` },
    });
    await assert.rejects(bounded(t, admissionLaunch.closed), error => {
      admissionFailure = error;
      assert.equal(error.code, "PROCESS_HOST_FAILED");
      assert.equal(error.completion.host_exit_code, 125);
      assert.equal(error.completion.root_status, null);
      assert.equal(error.completion.failure.phase, "root-admission");
      assert.equal(error.completion.failure.error.kind, "NotFound");
      return true;
    });
  } finally {
    if (admissionFailure) await assert.rejects(processes.close(), error => error === admissionFailure);
    else await processes.close();
  }
});

const witnessed = { stdio: ["pipe", "pipe", "pipe", "pipe"] };
const released = running => {
  const source = running.child.stdio[3];
  const completion = finished(source, { readable: true, writable: false, cleanup: true });
  source.resume();
  return completion;
};

test("terminal release observes active and retained original EOF", async t => {
  const processes = new Processes();
  t.after(() => processes.close());
  const running = processes.start(process.execPath, ["-e",
    'process.stdin.once("data", () => { require("node:fs").closeSync(3); process.stdout.write("retired"); }); process.stdin.resume();'], witnessed);
  const active = released(running);
  const retired = shown(running, "retired");
  running.child.stdin.write("release");
  await Promise.all([active, retired]);
  const late = released(running).then(() => "released");
  running.child.stdin.end();
  assert.equal(await Promise.race([late, running.closed.then(() => "exited")]), "released",
    "original EOF must remain observable before the child exit");
  assert.equal((await running.closed).code, 0);

  const withdrawn = processes.start(process.execPath, ["-e", "process.stdin.resume()"], witnessed);
  const incomplete = released(withdrawn);
  withdrawn.child.stdio[3].destroy();
  await assert.rejects(incomplete, { code: "ERR_STREAM_PREMATURE_CLOSE" });
  withdrawn.child.stdin.end();
  assert.equal((await withdrawn.closed).code, 0);
});

// A regression fails at the test deadline and still reaches cleanup.
const bounded = (t, promise) => Promise.race([promise, new Promise((_, reject) =>
  t.signal.addEventListener("abort", () => reject(t.signal.reason), { once: true }))]);

test("PTY session disposal and normal exit retire every child, unlike an unowned root", {
  skip: process.platform === "win32" && (!process.env.E2E_CONPTY_HOST || !process.env.E2E_PROCESS_HOST),
  timeout: 10000,
}, async t => {
  for (const mode of ["unowned", "dispose", "exit"]) {
    const processes = new Processes(), server = createServer();
    let socket, pid;
    try {
      server.listen(0, "127.0.0.1");
      await once(server, "listening");
      const connected = once(server, "connection");
      const child = `const s=require('node:net').connect(${server.address().port},'127.0.0.1',()=>s.write(String(process.pid)+'\\n'));s.resume()`;
      const program = `require('node:child_process').spawn(process.execPath,['-e',${JSON.stringify(child)}],{stdio:'ignore'});process.stdin.once('data',()=>process.exit(0));process.stdin.resume()`;
      const running = await (mode === "unowned"
        ? processes.start(process.execPath, ["-e", program])
        : processes.startTerminal(process.execPath, ["-e", program]));
      [socket] = await bounded(t, connected);
      socket.setEncoding("utf8");
      let identity = "";
      for await (const chunk of socket.iterator({ destroyOnReturn: false })) {
        identity += chunk;
        if (identity.includes("\n")) break;
      }
      pid = Number(identity.trim());
      assert.equal(alive(pid), true, "descendant must be running before retirement");
      const retired = finished(socket, { readable: true, writable: false, cleanup: true });
      socket.resume();
      if (mode === "exit") {
        running.child.stdin.write("quit\r");
        assert.equal((await bounded(t, running.closed)).code, 0);
      }
      else await bounded(t, running[Symbol.asyncDispose]());
      if (mode === "unowned") {
        assert.equal(alive(pid), true, "negative control must expose the surviving child");
        process.kill(pid, "SIGKILL");
      }
      await bounded(t, retired);
      // This child never closes its socket while alive. EOF proves death;
      // kill(pid, 0) can still see a dead macOS child awaiting collection.
      pid = undefined;
    } finally {
      if (pid && alive(pid)) process.kill(pid, "SIGKILL");
      socket?.destroy();
      await processes.close();
      await new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
    }
  }
});

test("terminal root leads a sized session, takes raw input and drains output after exit", posix, async t => {
  const processes = new Processes();
  try {
    const running = processes.startTerminal("python3", [fixture, "probe"]);
    const probe = await new Spool(running.child.stdout, running.closed).next(event => "tty" in event);
    // Owner budget: exactly one owner process between the test and the executable.
    assert.deepEqual(probe, { tty: true, size: [80, 24], leader: true, foreground: true, parent: running.child.pid });
    // The interrupt reaches the root's foreground group, not its owner.
    const interrupted = shown(running, "interrupted");
    running.child.stdin.write("\x03");
    await interrupted;
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
      assert.equal(result.code, null);
      assert.equal(result.signal, "SIGKILL", "the owner preserves the executable's signal after draining");
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

test("live terminal resize preserves the root and changes native dimensions, unlike screen-only resize", {
  skip: process.platform === "win32" && (!process.env.E2E_CONPTY_HOST || !process.env.E2E_PROCESS_HOST),
  timeout: 10000,
}, async t => {
  const processes = new Processes(), server = createServer();
  const screen = new Terminal({ cols: 80, rows: 24 });
  let socket;
  try {
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const connected = once(server, "connection");
    const program = `
      process.stdin.setRawMode(true); process.stdin.resume();
      if (process.platform !== "win32") for (const fd of [3,4]) {
        require("node:fs").writeSync(fd, "PRESERVED_FD_" + fd);
        require("node:fs").closeSync(fd);
      }
      const socket = require('node:net').connect(${server.address().port}, '127.0.0.1');
      const report = () => socket.write(JSON.stringify({pid:process.pid, cols:process.stdout.columns, rows:process.stdout.rows})+'\\n');
      socket.on('connect', report); socket.on('data', report); process.stdout.on('resize', report);
    `;
    const running = await processes.startTerminal(process.execPath, ["-e", program],
      process.platform === "win32" ? {} : { stdio: ["pipe", "pipe", "pipe", "pipe", "pipe"] });
    if (process.platform !== "win32") await bounded(t, Promise.all([3, 4].map(fd =>
      shown(running, `PRESERVED_FD_${fd}`, running.child.stdio[fd]))));
    [socket] = await bounded(t, connected);
    const reports = new Spool(socket, running.closed);
    const first = await bounded(t, reports.next(event => event.cols === 80 && event.rows === 24));
    const owner = running.child.pid;
    // A headless-only resize cannot prove delivery to the native terminal.
    screen.resize(51, 17);
    const unchanged = reports.next(event => event.pid === first.pid);
    socket.write("probe");
    assert.deepEqual(await bounded(t, unchanged), first);
    for (const [cols, rows] of [[51, 17], [120, 40], [80, 24]]) {
      const resized = reports.next(event => event.cols === cols && event.rows === rows);
      await bounded(t, running.resize(cols, rows, screen));
      assert.deepEqual(await bounded(t, resized), { ...first, cols, rows });
      assert.equal(running.child.pid, owner);
      assert.equal(screen.cols, cols); assert.equal(screen.rows, rows);
    }
    await assert.rejects(async () => running.resize(0, 24, screen), /positive native coordinates/);
    assert.equal(screen.cols, 80); assert.equal(screen.rows, 24);
  } finally {
    socket?.destroy(); screen.dispose();
    await processes.close();
    await new Promise(resolve => server.close(resolve));
  }
});
