import type { ChildProcess } from "node:child_process";

export type OwnedChildClose =
  | {
      readonly kind: "closed";
      readonly code: number | null;
      readonly signal: NodeJS.Signals | null;
      /** A launch error observed before close; close remains the exit proof. */
      readonly error?: Error;
    };

/**
 * Keeps one close observation for a host-owned child process. `killed` only
 * reports that a signal was sent, so callers use this object's close result
 * when they need evidence that the child actually terminated.
 */
export class OwnedChild {
  readonly child: ChildProcess;
  readonly #closed: Promise<OwnedChildClose>;
  #error: Error | undefined;

  constructor(child: ChildProcess) {
    this.child = child;
    this.#error = undefined;
    this.#closed = new Promise(resolve => {
      child.once("close", (code: number | null, signal: NodeJS.Signals | null) => {
        resolve({ kind: "closed", code, signal, error: this.#error });
      });
      child.once("error", error => {
        // An error can describe a failed signal or spawn attempt while the
        // child is still alive. Keep waiting for `close` as the ownership
        // proof and carry the diagnostic alongside that terminal observation.
        this.#error = error instanceof Error ? error : new Error(String(error));
      });
    });
  }

  /** Sends a termination signal; the returned boolean is not an exit proof. */
  terminate(signal: NodeJS.Signals = "SIGTERM"): boolean {
    try {
      return this.child.kill(signal);
    } catch {
      return false;
    }
  }

  /**
   * Waits for the close observation. A timeout is an explicit unknown result;
   * it never masquerades as a successful child exit.
   */
  waitForClose(): Promise<OwnedChildClose>;
  waitForClose(timeoutMs: number): Promise<OwnedChildClose | { readonly kind: "timeout" }>;
  async waitForClose(timeoutMs?: number): Promise<OwnedChildClose | { readonly kind: "timeout" }> {
    if (timeoutMs === undefined) return this.#closed;
    if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
      throw new RangeError("child close timeout must be finite and nonnegative");
    }
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([
        this.#closed,
        new Promise<{ readonly kind: "timeout" }>(resolve => {
          timer = setTimeout(() => resolve({ kind: "timeout" }), timeoutMs);
        }),
      ]);
    } finally {
      if (timer !== undefined) clearTimeout(timer);
    }
  }
}

/** Installs host signal forwarding and returns the precise uninstaller. */
export function installOwnedChildSignalCleanup(child: OwnedChild): () => void {
  const handlers = (["SIGINT", "SIGTERM"] as const).map(signal => {
    const handler = () => {
      child.terminate(signal);
    };
    process.once(signal, handler);
    return [signal, handler] as const;
  });
  return () => {
    for (const [signal, handler] of handlers) process.off(signal, handler);
  };
}
