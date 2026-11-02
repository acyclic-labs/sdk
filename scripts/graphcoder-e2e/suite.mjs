import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import { failureReport } from "./report.mjs";

// The command caller owns SIGINT and SIGTERM cancellation.
export function commandCancellation() {
  const controller = new AbortController();
  const cancel = name => controller.abort(new Error(
    `step=caller cancelled command (${name}); recovery command: ${JSON.stringify(process.argv)}`));
  const interrupt = () => cancel("SIGINT"), terminate = () => cancel("SIGTERM");
  process.once("SIGINT", interrupt);
  process.once("SIGTERM", terminate);
  return { signal: controller.signal, dispose() {
    process.off("SIGINT", interrupt); process.off("SIGTERM", terminate);
  } };
}

// The caller supplies the sole process owner and joins its physical retirement.
// Preparation retains that owner but settles before the active sample begins.
// Phase observers enqueue small records; their failure cannot skip retirement.
// `origin` includes module load and preparation in the elapsed-time report.
/**
 * @template Result
 * @param {{processes: {close(): unknown}, prepare?: () => any,
 * run: (prepared: any) => Result | Promise<Result>,
 * onPhase?: (event: {phase: string, outcome: string, seconds: number, failure?: ReturnType<typeof failureReport>}) => void,
 * origin?: number, signal?: AbortSignal}} options
 */
export async function withOwnedCase({ processes, prepare, run, onPhase, origin, signal = undefined }) {
  assert.ok(processes && typeof processes.close === "function" && typeof run === "function",
    "owned-case step=admit failed; recovery command: call withOwnedCase({processes,run}) with the caller's process scope");
  const entered = origin ?? performance.now();
  let started, preparationSeconds = 0, failure;
  const phase = (name, outcome, error) => {
    if (outcome === "failed") failure ??= { cause: error };
    try {
      if (onPhase) {
        const event = { phase: name, outcome, seconds: (performance.now() - entered) / 1000 };
        if (outcome === "failed") event.failure = failureReport(error);
        onPhase(event);
      }
    } catch (error) { failure ??= { cause: error }; }
  };
  let result;
  /** @type {Promise<unknown> | undefined} */
  let pending;
  const invoke = operation => {
    signal?.throwIfAborted();
    if (!signal) return operation();
    return (async () => {
      let reject;
      const cancelled = new Promise((_, no) => { reject = no; });
      const abort = () => reject(signal.reason);
      signal.addEventListener("abort", abort, { once: true });
      try {
        pending = Promise.resolve().then(() => { signal.throwIfAborted(); return operation(); });
        return await Promise.race([pending, cancelled]);
      } finally { signal.removeEventListener("abort", abort); }
    })();
  };
  try {
    signal?.throwIfAborted();
    let prepared;
    if (prepare) {
      phase("preparation", "entered");
      try {
        prepared = await invoke(prepare);
        phase("preparation", "completed");
      }
      catch (error) { phase("preparation", "failed", error); throw error; }
      preparationSeconds = (performance.now() - entered) / 1000;
    }
    if (failure) throw failure.cause;
    phase("admitted", "completed");
    if (failure) throw failure.cause;
    started = performance.now();
    phase("run", "entered");
    if (failure) throw failure.cause;
    try { result = await invoke(() => run(prepared)); phase("run", "completed"); }
    catch (error) { phase("run", "failed", error); throw error; }
  }
  catch (error) { failure ??= { cause: error }; }
  finally {
    phase("retired", "entered");
    try { await processes.close(); phase("retired", "completed"); }
    catch (error) { phase("retired", "failed", error); }
    finally { await pending?.catch(() => {}); }
  }
  if (failure) throw failure.cause;
  return { run: result, seconds: (performance.now() - started) / 1000,
    preparation_seconds: preparationSeconds, total_seconds: (performance.now() - entered) / 1000 };
}
