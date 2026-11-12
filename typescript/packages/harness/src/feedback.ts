import type { UsageQuery } from "./client-query.js";

/** Display a visible agent's admitted counts. Owns and frees the supplied query.
 * Closing cancels the pending source wait; done settles after Rust releases it. */
export function mountFeedbackCounts(element: HTMLElement, query: UsageQuery): {
  close(): void; done: Promise<void>;
} {
  const cancellation = new AbortController();
  const render = () => {
    const snapshot = query.snapshot();
    element.textContent = snapshot.synchronized
      ? (["defect", "slow", "unclear", "limit", "other"] as const)
        .map(kind => kind + ": " + (snapshot.feedback[kind] ?? 0n)).join(" · ")
      : "Feedback loading";
  };
  const done = (async () => {
    try {
      render();
      while (!cancellation.signal.aborted) {
        if (await query.advance(cancellation.signal)) render();
      }
    } catch (error) {
      if (!cancellation.signal.aborted) {
        element.textContent = "Feedback unavailable";
        throw error;
      }
    } finally {
      query.free();
    }
  })();
  return { close: () => cancellation.abort(), done };
}
