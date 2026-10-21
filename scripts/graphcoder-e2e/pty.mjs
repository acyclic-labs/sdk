import assert from "node:assert/strict";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Terminal, screenText } from "../graphcoder-pty/screen.mjs";

function bold(terminal, text) {
  const buffer = terminal.buffer.active;
  for (let y = buffer.viewportY; y < buffer.viewportY + terminal.rows; y++) {
    const line = buffer.getLine(y);
    if (!line) continue;
    for (let x = 0; x <= terminal.cols - text.length; x++) {
      const cells = Array.from(text, (_, index) => line.getCell(x + index));
      if (cells.map(cell => cell?.getChars() || " ").join("") === text
          && cells.every((cell, index) => text[index] === " " || cell?.isBold())) return true;
    }
  }
  return false;
}

export function matches(screen, raw, condition) {
  const visible = screenText(screen);
  return (condition.contains ?? []).every(text => visible.includes(text))
    && (condition.absent ?? []).every(text => !visible.includes(text))
    && (!condition.bold || bold(screen, condition.bold))
    && (condition.raw ?? []).every(text => raw.includes(text))
    && (!condition.cursor || raw.lastIndexOf("\x1b[?25h") > raw.lastIndexOf("\x1b[?25l"));
}

export async function pty(processes, binary, fixture, input, capture) {
  let program, args;
  if (process.platform === "win32") {
    program = join(process.env.SystemRoot, "System32/WindowsPowerShell/v1.0/powershell.exe");
    args = ["-NoProfile", "-NonInteractive", "-File", fileURLToPath(new URL("../graphcoder-conpty.ps1", import.meta.url)),
      "-Binary", binary, "-Workspace", fixture.workspace, "-Catalog", fixture.catalog,
      "-SettingsRoot", fixture.settings, "-Model", "e2e/tiny", "-DeadlineMilliseconds", "8000"];
    assert.ok(process.env.E2E_CONPTY_ASSEMBLY, "prebuild the ConPTY host with prepare-conpty.ps1 and set E2E_CONPTY_ASSEMBLY");
    args.push("-HostAssembly", process.env.E2E_CONPTY_ASSEMBLY);
    if (input.resume) args.push("-Resume");
  } else {
    // Python sets the controlling terminal on Linux and macOS and exposes a
    // kill signal targeting the executable, so wrapper death cannot fake SIGKILL.
    program = "python3";
    args = [fileURLToPath(new URL("../graphcoder-pty/transport.py", import.meta.url)), binary, ...fixture.args];
    if (input.resume) args.push("--resume");
  }
  const screen = new Terminal({ cols: 80, rows: 24, allowProposedApi: true });
  const killSignal = process.platform === "win32" ? "SIGKILL" : "SIGUSR1";
  const running = processes.start(program, args, { cwd: fixture.workspace, env: fixture.env, scopeKillSignal: killSignal });
  screen.onData(data => running.child.stdin.write(data));
  let index = 0, failure;
  const snapshots = [];
  function frame() {
    if (failure || index >= input.actions.length) return;
    try {
      const visible = screenText(screen);
      const raw = running.stdout;
      if (raw.includes("has overflowed its stack") || /task [0-9a-f-]{36} failed:/.test(visible)) {
        throw new Error(`shipping executable failed before the next screen assertion: ${visible}`);
      }
      const action = input.actions[index];
      if (action.after_request && !fixture.requests.some(event => event.path === action.after_request.path && event.step === action.after_request.step)) return;
      if (!matches(screen, raw, action.when)) return;
      snapshots.push({ action: index, visible });
      index++;
      if (action.prompt !== undefined) running.child.stdin.write(`\x1b[200~${action.prompt}\x1b[201~\r`);
      if (action.send !== undefined) running.child.stdin.write(action.send);
      if (action.kill) {
        running.child.kill(killSignal);
      }
    } catch (error) { failure = error; running.child.kill(killSignal); }
  }
  const unsubscribe = fixture.onRequest(frame);
  running.child.stdout.on("data", bytes => {
    screen.write(bytes, frame);
  });
  const result = await running.closed;
  const raw = result.stdout;
  unsubscribe();
  await new Promise(resolve => screen.write("", resolve));
  const visible = screenText(screen);
  capture({ ...result, raw, snapshots, visible, actions: index });
  screen.dispose();
  if (failure) throw failure;
  assert.equal(index, input.actions.length, `PTY completed ${index}/${input.actions.length} actions: ${result.stderr}\n${raw}`);
  if (!input.killed) assert.equal(result.code, input.exit ?? 0, result.stderr + visible);
  else assert.ok(result.signal || result.code !== 0, "PTY executable must be killed");
  return result;
}
