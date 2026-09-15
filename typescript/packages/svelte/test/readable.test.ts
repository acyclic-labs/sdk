import { describe, expect, test } from "bun:test";
import { harnessReadable, selectedHarnessReadable } from "../src/index.js";

interface State { readonly count: number; readonly label: string }

function externalStore(initial: State) {
  let state = initial;
  const listeners = new Set<() => void>();
  return {
    getSnapshot: () => state,
    subscribe: (listener: () => void) => { listeners.add(listener); return () => listeners.delete(listener); },
    update(next: State) { state = next; for (const listener of listeners) listener(); },
  };
}

describe("Svelte bindings", () => {
  test("emits immediately, suppresses equal selections, and unsubscribes", () => {
    const store = externalStore({ count: 0, label: "initial" });
    const states: State[] = [];
    const counts: number[] = [];
    const unsubscribeState = harnessReadable(store).subscribe(value => states.push(value));
    const unsubscribeCount = selectedHarnessReadable(store, value => value.count).subscribe(value => counts.push(value));

    store.update({ count: 0, label: "changed" });
    store.update({ count: 1, label: "changed" });
    unsubscribeState();
    unsubscribeCount();
    store.update({ count: 2, label: "ignored" });

    expect(states).toEqual([{ count: 0, label: "initial" }, { count: 0, label: "changed" }, { count: 1, label: "changed" }]);
    expect(counts).toEqual([0, 1]);
  });
});
