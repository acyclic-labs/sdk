import type { ProjectionStore } from "@acyclic-labs/harness";

export interface Readable<Value> { subscribe(run: (value: Value) => void): () => void }
export function harnessReadable<State>(store: ProjectionStore<State>): Readable<State> { return selectedHarnessReadable(store, value => value); }
export function selectedHarnessReadable<State, Selected>(store: ProjectionStore<State>, select: (state: State) => Selected, equal: (left: Selected, right: Selected) => boolean = Object.is): Readable<Selected> {
  return { subscribe(run) { let current = select(store.getSnapshot()); run(current); return store.subscribe(() => { const next = select(store.getSnapshot()); if (!equal(current, next)) { current = next; run(next); } }); } };
}
