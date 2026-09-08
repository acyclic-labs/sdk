import type { ModelAttempt, ModelEvent, ModelProvider, ModelRequest } from "@acyclic/harness";

/** Independently versioned adapter for public OpenCode-compatible clients. */
export function openCodeProvider(
  run: (request: ModelRequest) => AsyncIterable<ModelEvent>,
  reconcile: (attempt: ModelAttempt) => Promise<readonly ModelEvent[] | undefined>,
): ModelProvider {
  return { generate: run, reconcile };
}
