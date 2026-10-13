import type { ClientSnapshot } from "./client-snapshot.js";

/** Svelte readable contract; every subscription receives its current value now. */
export function clientReadable<Value>(store: ClientSnapshot<Value>): {
  subscribe(run: (value: Value) => void): () => void;
} {
  return {
    subscribe(run) {
      let unsubscribe: (() => void) | undefined;
      unsubscribe = store.subscribe(() => {
        if (unsubscribe !== undefined) run(store.getSnapshot());
      });
      try { run(store.getSnapshot()); }
      catch (error) { unsubscribe(); throw error; }
      return unsubscribe;
    },
  };
}
