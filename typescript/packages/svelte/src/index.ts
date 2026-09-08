import type { ProjectionStore } from "@acyclic/harness";

/** Minimal Svelte-readable view over one authoritative harness projection. */
export function harnessReadable<State>(store: ProjectionStore<State>) {
  return {
    subscribe(run: (value: State) => void): () => void {
      run(store.getSnapshot());
      return store.subscribe(() => run(store.getSnapshot()));
    },
  };
}
