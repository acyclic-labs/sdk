import { createInterface } from "node:readline";

/** Peer interactions (connections plus frames) one model process may spend; the largest model uses a few dozen. */
export const MODEL_STEPS = 4096;

/**
 * Serves one model process for its Rust owner (fuzz/driver.rs): one JSON command per stdin line, one JSON result per
 * stdout line. The owner's stdin is this process's lifetime. Its end means the owning test finished, panicked or was
 * killed, so the process exits at once, even while a command is still pending or client work is still scheduled; no
 * model process can outlive the test that started it.
 */
export async function serve<Input>(command: (input: Input) => Promise<unknown>): Promise<never> {
  const lines = createInterface({ input: process.stdin });
  lines.once("close", () => process.exit(0));
  for await (const line of lines) {
    try { process.stdout.write(`${JSON.stringify(await command(JSON.parse(line) as Input))}\n`); }
    catch (error) { process.stdout.write(`${JSON.stringify({ error: String(error) })}\n`); }
  }
  process.exit(0);
}
