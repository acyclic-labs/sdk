#!/usr/bin/env node
// One persistent owner over the deterministic suite's terminal transport.
import { fork } from "node:child_process";
import { createServer, createConnection } from "node:net";
import { mkdirSync, mkdtempSync, lstatSync, chmodSync, openSync, closeSync, writeSync,
  existsSync, unlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { StringDecoder } from "node:string_decoder";
import { performance } from "node:perf_hooks";
import { parseArgs } from "node:util";
import { Terminal, screenText } from "./screen.mjs";
import { screenSnapshot } from "../graphcoder-e2e/screen-text.mjs";
import { TerminalFrames } from "../graphcoder-e2e/frames.mjs";
import { Processes } from "../graphcoder-e2e/process.mjs";

const file = fileURLToPath(import.meta.url);
const deadlineMs = 120_000; // Protective failure bound, never readiness pacing.
const named = { Enter: "\r", Esc: "\x1b", Tab: "\t", Backspace: "\x7f",
  Up: "\x1b[A", Down: "\x1b[B", Right: "\x1b[C", Left: "\x1b[D",
  Home: "\x1b[H", End: "\x1b[F", Delete: "\x1b[3~", Insert: "\x1b[2~",
  PageUp: "\x1b[5~", PageDown: "\x1b[6~", "Shift-Tab": "\x1b[Z" };
export function keys(values) {
  return values.map(value => value.startsWith("text:") ? value.slice(5)
    : named[value] ?? (/^Ctrl-[A-Z]$/i.test(value)
      ? String.fromCharCode(value.at(-1).toUpperCase().charCodeAt(0) - 64) : value)).join("");
}
function session(name) {
  if (!/^[a-zA-Z0-9_-]+$/.test(name ?? "")) throw Error("invalid session name");
  const parent = resolve(process.env.E2E_SCRATCH_ROOT ?? tmpdir(), `graphcoder-drive-${process.getuid?.() ?? "user"}`);
  for (const directory of [parent, join(parent, name)]) {
    try { mkdirSync(directory, { mode: 0o700 }); } catch (error) { if (error.code !== "EEXIST") throw error; }
    const info = lstatSync(directory);
    if (!info.isDirectory() || (process.getuid && (info.uid !== process.getuid() || (info.mode & 0o077)))) {
      throw Error("driver scratch must be an owner-only directory");
    }
  }
  const directory = join(parent, name);
  const socket = process.platform === "win32" ? `\\\\.\\pipe\\graphcoder-drive-${name}` : join(directory, "socket");
  if (process.platform !== "win32" && Buffer.byteLength(socket) >= 104) throw Error("driver socket path too long; select a shorter E2E_SCRATCH_ROOT");
  return { directory, socket };
}
function receive(socket, callback) {
  let text = "";
  socket.setEncoding("utf8");
  socket.on("data", chunk => {
    text += chunk;
    if (Buffer.byteLength(text) > 16 * 1024 * 1024) { socket.destroy(Error("control frame exceeds protective bound")); return; }
    const end = text.indexOf("\n");
    if (end < 0) return;
    socket.removeAllListeners("data");
    try { callback(JSON.parse(text.slice(0, end))); }
    catch (error) { socket.destroy(error); }
  });
}
export function request(name, command) {
  return new Promise((accept, reject) => {
    const socket = createConnection(session(name).socket);
    const timeout = setTimeout(() => socket.destroy(Error("driver command exceeded protective deadline")), deadlineMs + 1000);
    let answered = false;
    socket.on("error", reject);
    socket.on("close", () => { clearTimeout(timeout); if (!answered) reject(Error("driver closed without a reply")); });
    socket.on("connect", () => socket.write(JSON.stringify(command) + "\n"));
    receive(socket, result => {
      answered = true;
      socket.end();
      if (result.error) reject(Error(result.error)); else accept(result);
    });
  });
}
async function own(config) {
  const location = session(config.name);
  // Exclusive ownership. A dead owner's evidence is retained; stop removes its lock.
  const owner = join(location.directory, "owner");
  const lock = openSync(owner, "wx", 0o600);
  writeSync(lock, `${process.pid}\n`); closeSync(lock);
  try { await ownRun(config, location); }
  finally { if (existsSync(owner)) unlinkSync(owner); }
}
async function ownRun(config, { directory, socket }) {
  const artifacts = mkdtempSync(join(directory, "run-"));
  const terminal = new Terminal({ cols: config.cols, rows: config.rows, allowProposedApi: true });
  const transcripts = join(artifacts, "process");
  mkdirSync(transcripts, { mode: 0o700 });
  const processes = new Processes({ transcripts });
  const pending = new Set(), clients = new Set();
  const started = performance.now();
  let running, painted = false, ending = false, failure, cast, framesLog, recordingStarted, exit, retainedScreen;
  let retire;
  const retired = new Promise(resolve => { retire = resolve; });
  let sequence = 0;
  let querySequence = 0;
  const settlementQueries = new Map();
  const queryReplies = terminal.parser.registerOscHandler(777, data => {
    const match = /^acyclic;settled;(\d+);(.*)$/.exec(data);
    if (!match || !settlementQueries.has(Number(match[1]))) return false;
    try { settlementQueries.set(Number(match[1]), JSON.parse(match[2])); notify(); }
    catch (error) { fail(error); }
    return true;
  });
  const outputDecoder = new StringDecoder("utf8");
  function snapshot(raw = false) {
    if (exit) {
      const { cells, ...screen } = retainedScreen;
      return { ...screen, ...(raw ? { cells } : {}), exit };
    }
    return capture(raw);
  }
  function capture(raw = false) {
    const buffer = terminal.buffer.active, screen = screenSnapshot(terminal);
    const result = { frame: sequence, at: new Date().toISOString(),
      cursor: { x: buffer.cursorX, y: buffer.cursorY }, text: screen.logical,
      visible: screen.visible };
    if (raw) result.cells = Array.from({ length: terminal.rows }, (_, y) =>
      Array.from({ length: terminal.cols }, (_, x) => {
        const c = buffer.getLine(buffer.viewportY + y)?.getCell(x);
        return c ? { text: c.getChars(), width: c.getWidth(), fg: c.getFgColor(), bg: c.getBgColor(),
          fgMode: c.getFgColorMode(), bgMode: c.getBgColorMode(), bold: !!c.isBold(),
          italic: !!c.isItalic(), underline: !!c.isUnderline(), inverse: !!c.isInverse(),
          dim: !!c.isDim(), invisible: !!c.isInvisible(), strikethrough: !!c.isStrikethrough() } : null;
      }));
    return result;
  }
  function notify() { for (const check of [...pending]) check(); }
  function fail(error) { failure ??= error; notify(); }
  const frames = new TerminalFrames(terminal, () => {
    if (!frames.available()) return;
    sequence++;
    if (framesLog !== undefined) writeSync(framesLog, JSON.stringify(snapshot()) + "\n");
    if (!painted && screenText(terminal).includes("GraphCoder")) {
      painted = true;
      process.send?.({ name: config.name, first_paint_ms: performance.now() - started, directory, artifacts });
      process.disconnect?.();
    }
    notify();
  });
  const screenRestore = terminal.parser.registerCsiHandler({ prefix: "?", final: "l" }, parameters => {
    if (terminal.buffer.active.type === "alternate" && parameters.some(mode => [47, 1047, 1049].includes(mode))) {
      // Retain the displayed cells before terminal restoration discards them.
      // This copy happens on buffer retirement, not on every redraw.
      retainedScreen = capture(true);
    }
    return false;
  });
  function wait(condition, connection) {
    return new Promise((accept, reject) => {
      const query = condition.settled ? ++querySequence : undefined;
      if (query !== undefined) settlementQueries.set(query, undefined);
      const cleanup = () => { clearTimeout(timeout); pending.delete(check); connection.off("close", cancelled); settlementQueries.delete(query); };
      const cancelled = () => { cleanup(); reject(Error("waiting client disconnected")); };
      const check = () => {
        if (failure || ending) { cleanup(); reject(failure ?? Error("terminal exited")); return; }
        if (condition.exited) {
          if (exit) { cleanup(); accept(snapshot()); }
          return;
        }
        if (condition.settled && exit) { cleanup(); reject(Error("terminal exited; read its screen or stop the driver")); return; }
        if (!painted || (!exit && !frames.ready())) return;
        const text = snapshot().text;
        const phase = settlementQueries.get(query);
        const settled = phase !== undefined;
        if ((condition.contains === undefined || text.includes(condition.contains))
            && (condition.absent === undefined || !text.includes(condition.absent))
            && (!condition.settled || settled)) { cleanup(); accept({ ...snapshot(), ...(condition.settled ? { settled_phase: phase } : {}) }); }
      };
      const timeout = setTimeout(() => { cleanup(); reject(Error("terminal wait exceeded protective deadline")); }, deadlineMs);
      connection.on("close", cancelled); pending.add(check); check();
      if (query !== undefined && !exit) running.child.stdin.write(`\x1b[?7727;${query}n`);
    });
  }
  async function stop(replyConnection) {
    if (ending) return;
    ending = true; notify();
    running?.input.end();
    await processes.close();
    await new Promise(resolve => terminal.write("", resolve));
    screenRestore.dispose(); queryReplies.dispose(); frames.dispose(); terminal.dispose();
    for (const fd of [cast, framesLog]) if (fd !== undefined) closeSync(fd);
    for (const client of clients) if (client !== replyConnection) client.end();
    server.close();
    if (process.platform !== "win32" && existsSync(socket)) unlinkSync(socket);
    unlinkSync(join(directory, "owner"));
    retire();
  }
  const server = createServer(connection => {
    clients.add(connection);
    connection.on("error", () => {});
    connection.on("close", () => clients.delete(connection));
    receive(connection, async command => {
      try {
        let result = {};
        if (command.action === "screen") result = snapshot(command.raw);
        else if (command.action === "keys") {
          if (exit) throw Error("terminal exited; read its screen or stop the driver");
          const input = keys(command.keys);
          const begin = performance.now();
          await new Promise((resolve, reject) => running.child.stdin.write(input, error => error ? reject(error) : resolve()));
          result = { input_written_ms: performance.now() - begin };
        } else if (command.action === "wait") result = await wait(command, connection);
        else if (command.action === "record") {
          if (cast === undefined) {
            cast = openSync(join(artifacts, "screen.cast"), "wx", 0o600);
            framesLog = openSync(join(artifacts, "frames.jsonl"), "wx", 0o600);
            recordingStarted = performance.now();
            writeSync(cast, JSON.stringify({ version: 2, width: terminal.cols, height: terminal.rows, timestamp: Date.now() / 1000 }) + "\n");
            writeSync(framesLog, JSON.stringify(snapshot(true)) + "\n");
          }
          result = { recording: join(artifacts, "screen.cast"), frames: join(artifacts, "frames.jsonl") };
        } else if (command.action === "stop") await stop(connection);
        else throw Error("unknown driver command");
        connection.end(JSON.stringify(result) + "\n");
      } catch (error) { connection.end(JSON.stringify({ error: error.message }) + "\n"); }
    });
  });
  try {
    await new Promise((resolve, reject) => { server.once("error", reject); server.listen(socket, resolve); });
    if (process.platform !== "win32") chmodSync(socket, 0o600);
    running = await processes.startTerminal(config.binary, config.args,
      { cwd: config.cwd, cols: config.cols, rows: config.rows });
    terminal.onData(data => { if (!exit && !ending) running.child.stdin.write(data); });
    running.child.stdout.on("data", bytes => {
      const output = outputDecoder.write(bytes);
      if (cast !== undefined && output) writeSync(cast, JSON.stringify([(performance.now() - recordingStarted) / 1000, "o", output]) + "\n");
      frames.write(bytes).catch(fail);
    });
    running.closed.then(async result => {
      if (ending) return;
      await new Promise(resolve => terminal.write("", resolve));
      if (ending) return;
      if (terminal.buffer.active.type === "alternate" || retainedScreen === undefined) retainedScreen = capture(true);
      exit = { code: result.code, signal: result.signal };
      notify();
      if (!painted) {
        process.send?.({ error: "terminal exited before first paint" });
        await stop();
      }
    }, error => { fail(error); if (!painted) void stop(); }).catch(fail);
    process.on("SIGTERM", () => void stop());
    process.on("SIGINT", () => void stop());
    await retired;
  } catch (error) {
    process.send?.({ error: error.message });
    await stop();
    throw error;
  }
}
export async function main(argv = process.argv.slice(2)) {
  const [action, ...args] = argv;
  if (action === "_owner") return own(JSON.parse(args[0]));
  if (action === "start") {
    const split = args.indexOf("--");
    const { values } = parseArgs({ args: split < 0 ? args : args.slice(0, split), options: {
      name: { type: "string" }, cols: { type: "string", default: "120" }, rows: { type: "string", default: "40" },
      binary: { type: "string", default: process.env.GRAPHCODER ?? "graphcoder" } } });
    session(values.name);
    return new Promise((accept, reject) => {
      const child = fork(file, ["_owner", JSON.stringify({ ...values, cols: Number(values.cols), rows: Number(values.rows),
        cwd: process.cwd(), args: split < 0 ? [] : args.slice(split + 1) })], { detached: true, stdio: ["ignore", "ignore", "ignore", "ipc"], windowsHide: true });
      const timeout = setTimeout(() => { child.kill(); reject(Error("first frame exceeded protective deadline")); }, deadlineMs);
      child.once("error", reject);
      child.once("exit", code => { clearTimeout(timeout); reject(Error(`driver exited before first paint (${code})`)); });
      child.once("message", message => {
        clearTimeout(timeout); child.unref();
        if (message.error) reject(Error(message.error)); else accept(message);
      });
    });
  }
  const [name, ...rest] = args;
  const command = { action };
  if (action === "keys") command.keys = rest;
  else if (action === "screen") command.raw = rest.includes("--raw");
  else if (action === "wait") {
    const { values } = parseArgs({ args: rest, options: { contains: { type: "string" }, absent: { type: "string" }, settled: { type: "boolean" }, exited: { type: "boolean" } } });
    if (Object.keys(values).length !== 1) throw Error("wait requires exactly one of --contains, --absent, --settled, --exited");
    Object.assign(command, values);
  } else if (!["record", "stop"].includes(action)) throw Error("usage: drive start|keys|screen|wait|record|stop");
  return request(name, command);
}
if (process.argv[1] && resolve(process.argv[1]) === file) {
  main().then(value => { if (value !== undefined) console.log(JSON.stringify(value)); }, error => {
    console.error(error.message); process.exitCode = 1;
  });
}
