import type { ProjectionStore } from "@acyclic/harness";
import { useSyncExternalStore } from "react";

/** Reads an authoritative harness projection using React's external-store contract. */
export function useHarnessProjection<State>(store: ProjectionStore<State>): State {
  return useSyncExternalStore(store.subscribe, store.getSnapshot, store.getSnapshot);
}
