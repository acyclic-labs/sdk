import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import { Terminal, screenText, styledRun } from "../graphcoder-pty/screen.mjs";
import { TerminalFrames, FailureHistory, queryFailure, expectedTaskFailure } from "./frames.mjs";
import { OutputWatch } from "./process.mjs";
import { screenSnapshot } from "./screen-text.mjs";
import { executionSucceeded } from "./assertions.mjs";

export const SHOW_CURSOR = "\x1b[?25h", HIDE_CURSOR = "\x1b[?25l";
const OVERFLOW = "has overflowed its stack";
// The idle header. A `delta` is streamed text painted while its turn is open.
const IDLE = "\u25cb GraphCoder";

// The needles a PTY input watches in its raw output.
export function rawNeedles(actions) {
  return [OVERFLOW, SHOW_CURSOR, HIDE_CURSOR,
    ...actions.flatMap(action => [...(action.when?.raw ?? []), ...(action.assert?.raw ?? []), ...(action.reject?.raw ?? [])])];
}

// `raw` is the OutputWatch over the executable's raw terminal output.
// `delta` waits, on the same frame events as every condition, for streamed
// text to paint before its turn settles.
export function matches(screen, raw, condition, visible, diagnostic = visible, logical = visible) {
  if (visible === undefined) ({ visible, diagnostic, logical } = screenSnapshot(screen));
  return (condition.contains ?? []).every(text => logical.includes(text))
    && (condition.delta === undefined || (logical.includes(condition.delta) && !logical.includes(IDLE)))
    && (!condition.task_failure || expectedTaskFailure(diagnostic, condition))
    && (!condition.query_failure || queryFailure(diagnostic)?.startsWith(condition.query_failure))
    && (condition.absent ?? []).every(text => !logical.includes(text))
    && (!condition.bold || styledRun(screen, { text: condition.bold, bold: true }))
    && (condition.styles ?? []).every(style => styledRun(screen, style))
    && (condition.raw ?? []).every(text => raw.has(text))
    && (!condition.cursor || raw.later(SHOW_CURSOR, HIDE_CURSOR));
}

export async function pty(processes, binary, fixture, input, capture) {
  const screen = new Terminal({ cols: 80, rows: 24, allowProposedApi: true });
  const failures = new FailureHistory();
  let unsubscribe, unsubscribeSignals, frames;
  let firstPaint;
  const launchStarted = performance.now();
  try {
    frames = new TerminalFrames(screen, frame);
    const running = await processes.startTerminal(binary, [...fixture.args, ...(input.args ?? []), ...(input.resume ? ["--continue"] : [])],
      { cwd: fixture.workspace, env: fixture.env, scopeReport: true });
    screen.onData(data => running.child.stdin.write(data));
    const raw = new OutputWatch(rawNeedles(input.actions));
    let index = 0, failure, errors;
    // Model requests this case made, the newest one's arrival, and the count
    // when a prompt was sent: a delta's latency runs from its turn's request
    // reaching the mock, which then streams the chunks, to their paint.
    let requests = 0, requested, prompted = 0;
    const snapshots = [], deltas = [];
    function trace(event) {
      if (process.env.E2E_PTY_FRAMES === "1") process.stderr.write(`pty-frame: ${JSON.stringify({
        case: fixture.id, ms: performance.now(), action: index, ...event,
      })}\n`);
    }
    function frame(settled = false) {
      if (failure || !frames.ready()) return;
      try {
        const { visible, diagnostic, logical } = screenSnapshot(screen, { completeLines: !settled });
        trace({ event: "frame", settled, available: frames.available(), visible });
        if (firstPaint === undefined && frames.available() && visible.includes("GraphCoder")) {
          firstPaint = performance.now() - launchStarted;
          process.stderr.write(`pty-startup: ${JSON.stringify({ case: fixture.id, first_paint_ms: firstPaint })}\n`);
        }
        const action = input.actions[index];
        const refusal = raw.has(OVERFLOW) ? OVERFLOW : failures.unexpected(diagnostic, action?.when, action?.assert, visible);
        if (refusal) {
          const error = new Error(`step=admit terminal frame: shipping executable failed before terminal completion: ${refusal}\n${visible}; recovery=node scripts/graphcoder-e2e/run.mjs GRAPHCODER MOCK_MODEL ${fixture.id}`);
          error.code = "terminal_frame_failed";
          throw error;
        }
        if (!action || settled || !frames.available()) return;
        if (action.reject && matches(screen, raw, action.reject, visible, diagnostic, logical)) {
          const error = new Error(`step=await terminal action ${index}: rejected terminal state before the required observation\n${visible}; recovery=node scripts/graphcoder-e2e/run.mjs GRAPHCODER MOCK_MODEL ${fixture.id}`);
          error.code = "terminal_action_rejected";
          throw error;
        }
        if (action.after_request && !fixture.requested(action.after_request)) return;
        if (action.after_signal && !fixture.signalled?.(action.after_signal)) return;
        if (action.when.delta !== undefined && requests <= prompted) return;
        if (!matches(screen, raw, action.when, visible, diagnostic, logical)) return;
        if (action.when.delta !== undefined) {
          const delta = { case: fixture.id, action: index, text: action.when.delta, ms: performance.now() - requested };
          deltas.push(delta);
          process.stderr.write(`pty-delta: ${JSON.stringify(delta)}\n`);
        }
        if (action.assert) assert.ok(matches(screen, raw, action.assert, visible, diagnostic, logical),
          `step=assert observed terminal frame: ${visible}; recovery=node scripts/graphcoder-e2e/run.mjs GRAPHCODER MOCK_MODEL ${fixture.id}`);
        failures.accept(diagnostic, action.when, action.assert, visible);
        snapshots.push({ action: index, logical });
        if (action.prompt !== undefined || action.send !== undefined || action.kill) frames.consume();
        trace({ event: "action", prompt_bytes: Buffer.byteLength(action.prompt ?? ""),
          send: action.send, kill: action.kill, available: frames.available() });
        index++;
        if (action.prompt !== undefined || action.send !== undefined || action.kill) frames.consume();
        if (action.prompt !== undefined) {
          prompted = requests;
          running.child.stdin.write(`\x1b[200~${action.prompt}\x1b[201~\r`);
        }
        if (action.send !== undefined) running.child.stdin.write(action.send);
        if (action.signal !== undefined) fixture.signal(action.signal);
        if (action.kill) running.killExecutable();
      } catch (error) { failure = error; running.killExecutable(); }
    }
    unsubscribeSignals = fixture.onSignal?.(() => frame());
    unsubscribe = fixture.onRequest(event => {
      trace({ event: "request", path: event.path, step: event.step });
      // Token counts precede a turn's model request; they are not its start.
      if (event.path !== "count") {
        requests++;
        requested = performance.now();
      }
      frame();
    });
    // Exercise input arriving with the first terminal bytes, before the first
    // rendered frame or terminal-query replies have been admitted.
    if (input.text !== undefined) running.child.stdout.once("data", () => running.child.stdin.write(input.text));
    running.child.stdout.on("data", bytes => {
      raw.write(bytes);
      frames.write(bytes);
    });
    let result;
    try { result = await running.closed; }
    catch (error) { (errors ??= []).push(error); }
    finally {
      unsubscribeSignals?.();
      unsubscribeSignals = undefined;
      unsubscribe();
      unsubscribe = undefined;
      await new Promise(resolve => screen.write("", () => resolve(undefined)));
      frame(true);
    }
    const visible = screenText(screen);
    if (result) {
      try { capture({ ...result, snapshots, deltas, first_paint_ms: firstPaint, visible, actions: index }); }
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
      assert.ok(snapshots.at(-1)?.logical.includes(text), `settled screen missing ${text}`);
    }
    return result;
  } finally {
    unsubscribeSignals?.();
    unsubscribe?.();
    frames?.dispose();
    screen.dispose();
  }
}
