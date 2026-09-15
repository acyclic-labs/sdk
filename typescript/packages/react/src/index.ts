import type { ProjectionStore } from "@acyclic-labs/harness";
import { useRef, useSyncExternalStore } from "react";

export type ProjectionSelector<State, Selected> = (state: State) => Selected;
export function useHarnessProjection<State>(store: ProjectionStore<State>): State { return useSyncExternalStore(store.subscribe, store.getSnapshot, store.getSnapshot); }
export function useHarnessSelector<State, Selected>(store: ProjectionStore<State>, select: ProjectionSelector<State, Selected>, equal: (left: Selected, right: Selected) => boolean = Object.is): Selected {
  const cache = useRef<{ readonly selected: Selected } | undefined>(undefined);
  const snapshot = (): Selected => { const selected = select(store.getSnapshot()); if (cache.current !== undefined && equal(cache.current.selected, selected)) return cache.current.selected; cache.current = { selected }; return selected; };
  return useSyncExternalStore(store.subscribe, snapshot, snapshot);
}
