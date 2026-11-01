import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";

// The caller supplies the sole process owner and joins its physical retirement.
// Preparation retains that owner but settles before the active sample begins.
// Phase observers enqueue small records; their failure cannot skip retirement.
// `origin` includes module load and preparation in the elapsed-time report.
export async function withOwnedCase({ processes, prepare, run, onPhase, origin }) {
  assert.ok(processes && typeof processes.close === "function" && typeof run === "function",
    "owned-case step=admit failed; recovery command: call withOwnedCase({processes,run}) with the caller's process scope");
  const entered = origin ?? performance.now();
  let started, preparationSeconds = 0, phaseFailure;
  const phase = (name, outcome) => {
    try { onPhase?.({ phase: name, outcome, seconds: (performance.now() - entered) / 1000 }); }
    catch (error) { phaseFailure ??= error; }
  };
  let result;
  try {
    let prepared;
    if (prepare) {
      phase("preparation", "entered");
      try {
        prepared = await prepare();
        phase("preparation", "completed");
      }
      catch (error) { phase("preparation", "failed"); throw error; }
      preparationSeconds = (performance.now() - entered) / 1000;
    }
    if (phaseFailure) throw phaseFailure;
    phase("admitted", "completed");
    if (phaseFailure) throw phaseFailure;
    started = performance.now();
    phase("run", "entered");
    if (phaseFailure) throw phaseFailure;
    try { result = await run(prepared); phase("run", "completed"); }
    catch (error) { phase("run", "failed"); throw error; }
  }
  finally {
    phase("retired", "entered");
    try { await processes.close(); phase("retired", "completed"); }
    catch (error) { phase("retired", "failed"); throw error; }
  }
  if (phaseFailure) throw phaseFailure;
  return { run: result, seconds: (performance.now() - started) / 1000,
    preparation_seconds: preparationSeconds, total_seconds: (performance.now() - entered) / 1000 };
}
