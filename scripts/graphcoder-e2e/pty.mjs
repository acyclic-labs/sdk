import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import { Terminal, screenText, styledRun } from "../graphcoder-pty/screen.mjs";
import { TerminalFrames, FailureHistory, queryFailure, expectedTaskFailure } from "./frames.mjs";
import { OutputWatch, withCancellation } from "./process.mjs";
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
export function matches(screen, raw, condition, visible, diagnostic = visible, logical = visible, previous = "") {
  if (visible === undefined) ({ visible, diagnostic, logical } = screenSnapshot(screen));
  const rows = condition.same_row?.length || condition.separate_rows?.length ? visible.split("\n") : undefined;
  return (condition.contains ?? []).every(text => logical.includes(text))
    && (condition.same_row?.every(group => rows.some(row => group.every(text => row.includes(text)))) ?? true)
    && (condition.separate_rows?.every(group => group.every(text => rows.some(row => row.includes(text)))
      && !rows.some(row => group.every(text => row.includes(text)))) ?? true)
    && (condition.new_contains === undefined || condition.new_contains.some(group =>
      group.every(text => logical.includes(text)) && !group.every(text => previous.includes(text))))
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
  let unsubscribe, unsubscribeSignals, frames, running;
  let firstPaint, primaryFailure;
  const launchStarted = performance.now();
  try {
    frames = new TerminalFrames(screen, frame);
    running = await processes.startTerminal(binary, [...fixture.args, ...(input.args ?? []), ...(input.resume ? ["--continue"] : [])],
      { cwd: fixture.workspace, env: fixture.env, scopeReport: true });
    screen.onData(data => running.child.stdin.write(data));
    const raw = new OutputWatch(rawNeedles(input.actions));
    let index = 0, failure, errors, resizing, detaching = false;
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
      if (failure) return;
      try {
        const action = input.actions[index];
        // Provider settlement is independent of terminal parsing. An open
        // synchronized frame must not hide a bypassed required interaction.
        if (action?.before_request && fixture.requested(action.before_request)) {
          const error = new Error(`step=await terminal action ${index}: model request ${JSON.stringify(action.before_request)} overtook the required interaction; recovery=node scripts/graphcoder-e2e/run.mjs GRAPHCODER MOCK_MODEL ${fixture.id}`);
          error.code = "terminal_action_overtaken";
          throw error;
        }
        if (!frames.ready()) return;
        const { visible, diagnostic, logical } = screenSnapshot(screen, { completeLines: !settled });
        const previous = snapshots.at(-1)?.logical ?? "";
        trace({ event: "frame", settled, available: frames.available(), visible });
        if (firstPaint === undefined && frames.available() && visible.includes("GraphCoder")) {
          firstPaint = performance.now() - launchStarted;
          process.stderr.write(`pty-startup: ${JSON.stringify({ case: fixture.id, first_paint_ms: firstPaint })}\n`);
        }
        const refusal = raw.has(OVERFLOW) ? OVERFLOW : failures.unexpected(diagnostic, action?.when, action?.assert, visible);
        if (refusal) {
          const error = new Error(`step=admit terminal frame: shipping executable failed before terminal completion: ${refusal}\n${visible}; recovery=node scripts/graphcoder-e2e/run.mjs GRAPHCODER MOCK_MODEL ${fixture.id}`);
          error.code = "terminal_frame_failed";
          throw error;
        }
        // Session exit is an interaction, not two blind keystrokes. A single
        // Ctrl+D may detach directly or ask for confirmation; answer only
        // after that original dialog has actually been rendered.
        if (detaching && !settled && frames.available()
          && matches(screen, raw, { contains: ["Stop or detach?"] }, visible, diagnostic, logical)) {
          detaching = false;
          frames.consume();
          running.child.stdin.write("\u0004");
          return;
        }
        if (!action || settled || resizing || !frames.available()) return;
        if (action.reject && matches(screen, raw, action.reject, visible, diagnostic, logical, previous)) {
          const error = new Error(`step=await terminal action ${index}: rejected terminal state before the required observation\n${visible}; recovery=node scripts/graphcoder-e2e/run.mjs GRAPHCODER MOCK_MODEL ${fixture.id}`);
          error.code = "terminal_action_rejected";
          throw error;
        }
        if (action.after_request && !fixture.requested(action.after_request)) return;
        if (action.after_signal && !fixture.signalled?.(action.after_signal)) return;
        if (action.when.delta !== undefined && requests <= prompted) return;
        if (!matches(screen, raw, action.when, visible, diagnostic, logical, previous)) return;
        if (action.when.delta !== undefined) {
          const delta = { case: fixture.id, action: index, text: action.when.delta, ms: performance.now() - requested };
          deltas.push(delta);
          process.stderr.write(`pty-delta: ${JSON.stringify(delta)}\n`);
        }
        if (action.assert) assert.ok(matches(screen, raw, action.assert, visible, diagnostic, logical, previous),
          `step=assert observed terminal frame: ${visible}; recovery=node scripts/graphcoder-e2e/run.mjs GRAPHCODER MOCK_MODEL ${fixture.id}`);
        failures.accept(diagnostic, action.when, action.assert, visible);
        snapshots.push({ action: index, logical });
        if (action.prompt !== undefined || action.send !== undefined || action.resize || action.kill || action.detach) frames.consume();
        trace({ event: "action", prompt_bytes: Buffer.byteLength(action.prompt ?? ""),
          send: action.send, resize: action.resize, kill: action.kill, available: frames.available() });
        index++;
        if (action.prompt !== undefined || action.send !== undefined || action.resize || action.kill) frames.consume();
        if (action.prompt !== undefined) {
          prompted = requests;
          running.child.stdin.write(`\x1b[200~${action.prompt}\x1b[201~\r`);
        }
        if (action.send !== undefined) running.child.stdin.write(action.send);
        if (action.resize) {
          resizing = running.resize(action.resize.cols, action.resize.rows, screen);
          resizing.then(() => { resizing = undefined; frame(); }, error => {
            failure = error; running.killExecutable();
          });
        }
        if (action.detach) {
          detaching = true;
          running.child.stdin.write("\u0004");
        }
        if (action.signal !== undefined) fixture.emitSignal(action.signal);
        if (action.acknowledge !== undefined) {
          fetch(`${fixture.url}/ack`, { method: "POST", headers: { "content-type": "application/json" },
            body: JSON.stringify({ chunk: action.acknowledge }) })
            .then(reply => { if (!reply.ok) throw new Error(`chunk acknowledgment refused: ${reply.status}`); })
            .catch(error => { failure = error; running.killExecutable(); });
        }
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
    // Interactive waits must stop on native failure; ordinary commands retain
    // their original exit status so declared refusal cases can assert it.
    // A missing interaction is a failed case, never an indefinitely live PTY.
    // This is only a protective bound; every successful action waits on frames.
    const signal = AbortSignal.any([AbortSignal.timeout(120_000),
      ...(running.failure ? [running.failure] : []), ...(fixture.signal ? [fixture.signal] : [])]);
    try { result = await withCancellation(running, signal); }
    catch (error) {
      (errors ??= []).push(error);
      try { result = await running.closed; }
      catch (error) { errors.push(error); }
    }
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
  } catch (error) {
    primaryFailure = error;
    throw error;
  } finally {
    unsubscribeSignals?.();
    unsubscribe?.();
    try { await running?.[Symbol.asyncDispose](); }
    catch (error) {
      // The action/completion path already retains its original failures.
      // Joining the same rejected completion again must not replace them.
      if (!primaryFailure) throw error;
    }
    finally { frames?.dispose(); screen.dispose(); }
  }
}
