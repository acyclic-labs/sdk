import type { ModelAttempt, ModelEvent, ModelProvider, ModelRequest } from "@acyclic/harness";

/** Independently versioned adapter for public Pi-compatible streaming clients. */
export function piProvider(
  run: (request: ModelRequest) => AsyncIterable<ModelEvent>,
  reconcile: (attempt: ModelAttempt) => Promise<readonly ModelEvent[] | undefined>,
): ModelProvider {
  return { generate: run, reconcile };
}
