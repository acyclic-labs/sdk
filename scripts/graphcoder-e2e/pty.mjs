import assert from "node:assert/strict";
import { Terminal, screenText } from "../graphcoder-pty/screen.mjs";
import { TerminalFrames, FailureHistory } from "./frames.mjs";
import { OutputWatch } from "./process.mjs";

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

export const SHOW_CURSOR = "\x1b[?25h", HIDE_CURSOR = "\x1b[?25l";
const OVERFLOW = "has overflowed its stack";

// The needles a PTY input watches in its raw output.
export function rawNeedles(actions) {
  return [OVERFLOW, SHOW_CURSOR, HIDE_CURSOR, ...actions.flatMap(action => action.when?.raw ?? [])];
}

// `raw` is the OutputWatch over the executable's raw terminal output.
export function matches(screen, raw, condition) {
  const visible = screenText(screen);
  return (condition.contains ?? []).every(text => visible.includes(text))
    && (!condition.task_failure || expectedTaskFailure(visible, condition))
    && (condition.absent ?? []).every(text => !visible.includes(text))
    && (!condition.bold || bold(screen, condition.bold))
    && (condition.raw ?? []).every(text => raw.has(text))
    && (!condition.cursor || raw.later(SHOW_CURSOR, HIDE_CURSOR));
}

function expectedTaskFailure(visible, condition) {
  const message = /task [0-9a-f-]{36} failed: ([^\n]*)/.exec(visible)?.[1];
  return typeof condition.task_failure === "string" && condition.task_failure.length > 0
    && message?.startsWith(condition.task_failure);
}

export async function pty(processes, binary, fixture, input, capture) {
  const screen = new Terminal({ cols: 80, rows: 24, allowProposedApi: true });
  const frames = new TerminalFrames(screen), failures = new FailureHistory();
  const running = processes.startTerminal(binary, [...fixture.args, ...(input.resume ? ["--resume"] : [])],
    { cwd: fixture.workspace, env: fixture.env });
  screen.onData(data => running.child.stdin.write(data));
  const raw = new OutputWatch(rawNeedles(input.actions));
  let index = 0, failure;
  const snapshots = [];
  function frame() {
    if (failure || index >= input.actions.length || !frames.ready()) return;
    try {
      const visible = screenText(screen);
      const action = input.actions[index];
      if (raw.has(OVERFLOW) || failures.unexpected(visible, action.when)) {
        throw new Error(`shipping executable failed before the next screen assertion: ${visible}`);
      }
      if (action.after_request && !fixture.requested(action.after_request)) return;
      if (!matches(screen, raw, action.when)) return;
      failures.accept(visible, action.when);
      snapshots.push({ action: index, visible });
      index++;
      if (action.prompt !== undefined) running.child.stdin.write(`\x1b[200~${action.prompt}\x1b[201~\r`);
      if (action.send !== undefined) running.child.stdin.write(action.send);
      if (action.kill) running.killExecutable();
    } catch (error) { failure = error; running.killExecutable(); }
  }
  const unsubscribe = fixture.onRequest(frame);
  running.child.stdout.on("data", bytes => {
    raw.write(bytes);
    frames.write(bytes, frame);
  });
  const result = await running.closed;
  unsubscribe();
  await new Promise(resolve => screen.write("", resolve));
  const visible = screenText(screen);
  capture({ ...result, snapshots, visible, actions: index });
  frames.dispose();
  screen.dispose();
  if (failure) throw failure;
  assert.equal(index, input.actions.length, `PTY completed ${index}/${input.actions.length} actions: ${result.stderr}\n${result.stdout}`);
  if (!input.killed) assert.equal(result.code, input.exit ?? 0, result.stderr + visible);
  else assert.ok(result.signal || result.code !== 0, "PTY executable must be killed");
  for (const text of input.settled_contains ?? []) {
    assert.ok(snapshots.at(-1)?.visible.includes(text), `settled screen missing ${text}`);
  }
  return result;
}
