import { useSyncExternalStore } from "react";
import type { ClientSnapshot } from "./client-snapshot.js";

/** Pin serverSnapshot to the same immutable value used for server rendering. */
export function useClientSnapshot<Value>(store: ClientSnapshot<Value>, serverSnapshot: Value): Value {
  return useSyncExternalStore(store.subscribe, store.getSnapshot, () => serverSnapshot);
}
