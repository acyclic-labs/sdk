import { describe, expect, test } from "bun:test";
import { act, create } from "react-test-renderer";
import { useHarnessProjection, useHarnessSelector } from "../src/index.js";

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

describe("React bindings", () => {
  test("rerenders projections and suppresses selector-equal snapshots", () => {
    globalThis.IS_REACT_ACT_ENVIRONMENT = true;
    const store = externalStore({ count: 0, label: "initial" });
    const projectionRenders: State[] = [];
    const selectorRenders: number[] = [];
    function Projection() { projectionRenders.push(useHarnessProjection(store)); return null; }
    function Selector() { selectorRenders.push(useHarnessSelector(store, state => state.count)); return null; }

    let projection!: ReturnType<typeof create>;
    let selector!: ReturnType<typeof create>;
    act(() => { projection = create(<Projection />); selector = create(<Selector />); });
    act(() => store.update({ count: 0, label: "changed" }));
    act(() => store.update({ count: 1, label: "changed" }));

    expect(projectionRenders).toEqual([{ count: 0, label: "initial" }, { count: 0, label: "changed" }, { count: 1, label: "changed" }]);
    expect(selectorRenders).toEqual([0, 1]);
    act(() => { projection.unmount(); selector.unmount(); });
  });
});
