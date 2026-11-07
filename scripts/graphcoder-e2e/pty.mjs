import assert from "node:assert/strict";
import { Terminal, screenText } from "../graphcoder-pty/screen.mjs";
import { TerminalFrames, FailureHistory, queryFailure } from "./frames.mjs";
import { OutputWatch } from "./process.mjs";
import { executionSucceeded } from "./assertions.mjs";

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
  return [OVERFLOW, SHOW_CURSOR, HIDE_CURSOR,
    ...actions.flatMap(action => [...(action.when?.raw ?? []), ...(action.assert?.raw ?? [])])];
}

// `raw` is the OutputWatch over the executable's raw terminal output.
export function matches(screen, raw, condition, visible = screenText(screen)) {
  return (condition.contains ?? []).every(text => visible.includes(text))
    && (!condition.task_failure || expectedTaskFailure(visible, condition))
    && (!condition.query_failure || queryFailure(visible)?.startsWith(condition.query_failure))
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
  const failures = new FailureHistory();
  let unsubscribe, frames;
  try {
    frames = new TerminalFrames(screen, frame);
    const running = await processes.startTerminal(binary, [...fixture.args, ...(input.resume ? ["--resume"] : [])],
      { cwd: fixture.workspace, env: fixture.env, scopeReport: true });
    screen.onData(data => running.child.stdin.write(data));
    const raw = new OutputWatch(rawNeedles(input.actions));
    let index = 0, command, failure, errors;
    const snapshots = [];
    function frame() {
      if (failure || !frames.ready()) return;
      try {
        const visible = screenText(screen);
        const action = input.actions[index];
        const refusal = raw.has(OVERFLOW) ? OVERFLOW : failures.unexpected(visible, action?.when);
        if (refusal) {
          const error = new Error(`step=admit terminal frame: shipping executable failed before terminal completion: ${refusal}\n${visible}; recovery=node scripts/graphcoder-e2e/run.mjs GRAPHCODER MOCK_MODEL ${fixture.id}`);
          error.code = "terminal_frame_failed";
          throw error;
        }
        if (!action) return;
        const unavailable = command && `${command} is not available in this view`;
        if (unavailable && visible.includes(unavailable)
            && !(action.when.contains ?? []).some(text => text && unavailable.includes(text))) {
          const error = new Error(`command_unavailable: ${unavailable}`);
          error.code = "command_unavailable";
          throw error;
        }
        if (!frames.available()) return;
        if (action.after_request && !fixture.requested(action.after_request)) return;
        if (!matches(screen, raw, action.when, visible)) return;
        if (action.assert) assert.ok(matches(screen, raw, action.assert, visible),
          `step=assert observed terminal frame: ${visible}; recovery=node scripts/graphcoder-e2e/run.mjs GRAPHCODER MOCK_MODEL ${fixture.id}`);
        failures.accept(visible, action.when);
        if (unavailable && visible.includes(unavailable)) command = undefined;
        snapshots.push({ action: index, visible });
        index++;
        if (action.prompt !== undefined || action.send !== undefined || action.kill) frames.consume();
        if (action.prompt !== undefined) {
          command = action.prompt.trim().startsWith("/") ? action.prompt.trim() : undefined;
          running.child.stdin.write(`\x1b[200~${action.prompt}\x1b[201~\r`);
        }
        if (action.send !== undefined) running.child.stdin.write(action.send);
        if (action.kill) running.killExecutable();
      } catch (error) { failure = error; running.killExecutable(); }
    }
    unsubscribe = fixture.onRequest(frame);
    running.child.stdout.on("data", bytes => {
      raw.write(bytes);
      frames.write(bytes);
    });
    let result;
    try { result = await running.closed; }
    catch (error) { (errors ??= []).push(error); }
    finally {
      unsubscribe();
      unsubscribe = undefined;
      await new Promise(resolve => screen.write("", resolve));
    }
    const visible = screenText(screen);
    if (result) {
      try { capture({ ...result, snapshots, visible, actions: index }); }
      catch (error) { (errors ??= []).push(error); }
    }
    if (failure) {
      if (!errors) throw failure;
      errors.unshift(failure);
    }
    if (errors) {
      if (errors.length === 1) throw errors[0];
      throw new Error(errors[0].message, {
        cause: new AggregateError(errors, "terminal action and completion failures"),
      });
    }
    // A refused start reports only through its termination record.
    const cause = result.termination?.execution_failure;
    assert.equal(index, input.actions.length, `PTY completed ${index}/${input.actions.length} actions: ${cause ? JSON.stringify(cause) : ""}${result.stderr}\n${result.stdout}`);
    if (!input.killed) {
      assert.ok(frames.ready(), `step=admit completed terminal transaction; recovery=node scripts/graphcoder-e2e/run.mjs GRAPHCODER MOCK_MODEL ${fixture.id}`);
      if ((input.exit ?? 0) === 0) executionSucceeded(result);
      else assert.equal(result.code, input.exit, result.stderr + visible);
    }
    else assert.ok(result.signal || result.code !== 0, "PTY executable must be killed");
    for (const text of input.settled_contains ?? []) {
      assert.ok(snapshots.at(-1)?.visible.includes(text), `settled screen missing ${text}`);
    }
    return result;
  } finally {
    unsubscribe?.();
    frames?.dispose();
    screen.dispose();
  }
}
