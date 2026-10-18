import { createInterface } from "node:readline";

/**
 * Work one model process may do in its whole life: every served command, peer connection, received frame and queue
 * item draws from this single budget, so no input, peer or queue can renew it. Long-lived owners replace the process
 * once it has spent half of it (fuzz/driver.rs `run`) instead of resetting the budget.
 */
export const MODEL_STEPS = Number(process.env.STREAM_MODEL_STEPS ?? 4096);
/** Exit code of a process that exhausted its budget, distinct from crashes (1) and owner exit (0). */
export const BUDGET_EXHAUSTED = 70;
let remaining = MODEL_STEPS;

/** Draws one step from the process-wide budget; exhausting it ends the process with BUDGET_EXHAUSTED. */
export function spend(): void {
  remaining -= 1;
  if (remaining >= 0) return;
  process.stderr.write("model step budget exhausted\n");
  process.exit(BUDGET_EXHAUSTED);
}

/** Answers before a command settles, for commands that report a state they then stay in (a parked command). */
export function acknowledge(value: unknown): void {
  process.stdout.write(`${JSON.stringify({ ack: value, remaining })}\n`);
}

/**
 * Serves one model process for its Rust owner (fuzz/driver.rs): one JSON command per stdin line, one JSON envelope
 * per stdout line carrying the result and the remaining budget. The owner's stdin is this process's lifetime. Its end
 * means the owning test finished, panicked or was killed, so the process exits at once, even while a command is still
 * pending or client work is still scheduled; no model process can outlive the test that started it.
 */
export async function serve<Input>(command: (input: Input) => Promise<unknown>): Promise<never> {
  const lines = createInterface({ input: process.stdin });
  lines.once("close", () => process.exit(0));
  for await (const line of lines) {
    spend();
    // Test-only control: a process that dies on its first command must fail every lifetime and budget test.
    if (process.env.STREAM_SESSION_FAULT === "startupCrash") process.exit(1);
    try { process.stdout.write(`${JSON.stringify({ result: await command(JSON.parse(line) as Input), remaining })}\n`); }
    catch (error) { process.stdout.write(`${JSON.stringify({ error: String(error), remaining })}\n`); }
  }
  process.exit(0);
}
