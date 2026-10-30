import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";

// The caller supplies the sole process owner. Reserve one second of the common
// ten-second case budget for drain/publication; external scenarios add no timer.
// Preparation retains that owner but settles before the active sample begins.
// Phase observers enqueue small records; their failure cannot skip retirement.
export class CaseDeadline extends Error {
  constructor(phase) {
    super(`owned-case step=${phase}-deadline; recovery command: ${JSON.stringify(process.argv)} after repairing the named phase`);
    this.name = "CaseDeadline";
    this.code = "CASE_DEADLINE_EXPIRED";
    this.phase = phase;
  }
}

// `origin` is the performance.now() instant the budget began; a suite process
// whose own start opens the envelope passes 0, so module load and preparation
// spend the same ten seconds the outer owner measures.
export async function withOwnedCase({ processes, prepare, run, onPhase, origin }) {
  assert.ok(processes && typeof processes.close === "function" && typeof run === "function",
    "owned-case step=admit failed; recovery command: call withOwnedCase({processes,run}) with the caller's process scope");
  const entered = origin ?? performance.now();
  let started, preparationSeconds = 0, phaseFailure, activePhase = prepare ? "preparation" : "run";
  const phase = (name, outcome) => {
    try { onPhase?.({ phase: name, outcome, seconds: (performance.now() - entered) / 1000 }); }
    catch (error) { phaseFailure ??= error; }
  };
  let expired = false, closing, timer;
  const close = () => {
    if (!closing) {
      phase("retired", "entered");
      try { closing = Promise.resolve(processes.close()); }
      catch (error) { closing = Promise.reject(error); }
    }
    return closing;
  };
  let result;
  timer = setTimeout(() => {
    expired = true;
    phase(activePhase, "deadline");
    close().catch(() => {}); // The original physical owner is joined below.
  }, origin === undefined ? 9000 : origin + 9000 - performance.now());
  try {
    let prepared;
    if (prepare) {
      phase("preparation", "entered");
      try {
        prepared = await prepare();
        if (expired) throw new CaseDeadline("preparation");
        phase("preparation", "completed");
      }
      catch (error) { if (!expired) phase("preparation", "failed"); throw error; }
      preparationSeconds = (performance.now() - entered) / 1000;
    }
    if (phaseFailure) throw phaseFailure;
    phase("admitted", "completed");
    if (phaseFailure) throw phaseFailure;
    started = performance.now();
    activePhase = "run";
    phase("run", "entered");
    if (phaseFailure) throw phaseFailure;
    try { result = await run(prepared); if (!expired) phase("run", "completed"); }
    catch (error) { if (!expired) phase("run", "failed"); throw error; }
  }
  finally {
    clearTimeout(timer);
    try { await close(); phase("retired", "completed"); }
    catch (error) { phase("retired", "failed"); throw error; }
  }
  if (phaseFailure) throw phaseFailure;
  return { run: result, expired, seconds: (performance.now() - started) / 1000,
    preparation_seconds: preparationSeconds, total_seconds: (performance.now() - entered) / 1000 };
}
