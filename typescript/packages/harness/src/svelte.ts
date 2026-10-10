import type { ClientSnapshot } from "./client-snapshot.js";

/** Svelte readable contract; every subscription receives its current value now. */
export function clientReadable<Value>(store: ClientSnapshot<Value>): {
  subscribe(run: (value: Value) => void): () => void;
} {
  return {
    subscribe(run) {
      let initial = true;
      const unsubscribe = store.subscribe(() => {
        if (!initial) run(store.getSnapshot());
      });
      initial = false;
      try { run(store.getSnapshot()); }
      catch (error) { unsubscribe(); throw error; }
      return unsubscribe;
    },
  };
}
