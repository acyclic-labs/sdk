import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";

// The caller supplies the sole process owner. Reserve one second of the common
// ten-second case budget for drain/publication; external scenarios add no timer.
export async function withOwnedCase({ processes, run }) {
  assert.ok(processes && typeof processes.close === "function" && typeof run === "function",
    "owned-case step=admit failed; recovery command: call withOwnedCase({processes,run}) with the caller's process scope");
  const started = performance.now();
  let expired = false, closing;
  const timer = setTimeout(() => {
    expired = true;
    closing = processes.close();
    closing.catch(() => {}); // Joined below even when the interrupted run rejects.
  }, 9000);
  let result;
  try { result = await run(); }
  finally {
    clearTimeout(timer);
    await (closing ?? processes.close());
  }
  return { run: result, expired, seconds: (performance.now() - started) / 1000 };
}
