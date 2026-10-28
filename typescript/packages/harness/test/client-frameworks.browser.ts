import { createElement, StrictMode, startTransition, Suspense, act } from "react";
import { createRoot, hydrateRoot } from "react-dom/client";
import { renderToString } from "react-dom/server";
import { flushSync } from "react-dom";
import { mount, unmount, tick } from "svelte";
import Component from "./client-frameworks.svelte";
import { SnapshotStore } from "@acyclic-labs/harness";
import { useClientSnapshot } from "@acyclic-labs/harness/react";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
const result = document.querySelector<HTMLElement>("#result")!;
const assert = (condition: unknown, message: string): void => { if (!condition) throw new Error(message); };
type Value = Readonly<{ count: number; settled: boolean; status: string }>;
const value = (count: number, settled: boolean, status: string): Value => Object.freeze({ count, settled, status });
const label = (view: Value): string => `${view.count}:${view.settled ? "settled" : "pending"}`;
let root: ReturnType<typeof createRoot> | undefined;
let hydrated: ReturnType<typeof hydrateRoot> | undefined;
let svelte: ReturnType<typeof mount> | undefined;
try {
  const store = new SnapshotStore(value(1, false, "pending"));
  const failedStore = new SnapshotStore(value(2, false, "pending"));
  const conflictStore = new SnapshotStore(value(3, false, "pending"));
  const failedServer = failedStore.getSnapshot(), conflictServer = conflictStore.getSnapshot();
  const warmStart = performance.now();
  const warmSnapshot = store.getSnapshot();
  for (let i = 0; i < 100_000; i++) assert(store.getSnapshot() === warmSnapshot, "warm snapshot identity changed");
  const warmMainThreadMs = performance.now() - warmStart;
  assert(warmMainThreadMs < 20, "cached view exceeded 20ms/100k main-thread read budget");
  const server = store.getSnapshot();
  let renders = 0;
  let subscriptions = 0;
  const source = {
    getSnapshot: store.getSnapshot,
    subscribe(listener: () => void) {
      subscriptions++;
      const detach = store.subscribe(listener);
      return () => { subscriptions--; detach(); };
    },
  };
  function View() {
    const snapshot = useClientSnapshot(source, server);
    const failure = useClientSnapshot(failedStore, failedServer);
    const conflicting = useClientSnapshot(conflictStore, conflictServer);
    renders++;
    return createElement("section", null,
      createElement("input", { defaultValue: "draft", "aria-label": "draft" }),
      createElement("p", { "data-react": true }, label(snapshot)),
      createElement("p", { "data-outcome": true, role: "status", "aria-live": "polite", "aria-atomic": true }, snapshot.status),
      createElement("p", { "data-failed": true, role: "status", "aria-live": "polite", "aria-atomic": true }, failure.status),
      createElement("p", { "data-conflict": true, role: "status", "aria-live": "polite", "aria-atomic": true }, conflicting.status),
      createElement("ol", { "data-scroll": true, style: { height: 80, overflowY: "scroll", margin: 0 } },
        Array.from({ length: 20 }, (_, i) => createElement("li", { key: i, "data-anchor": i, style: { height: 24 } }, `${i}:${snapshot.count}`))));
  }
  const markup = renderToString(createElement(View));
  assert(subscriptions === 0, "SSR attached a subscription");
  const hydration = document.querySelector<HTMLElement>("#hydration")!;
  hydration.innerHTML = markup;
  const errors: unknown[] = [];
  const coldStart = performance.now();
  store.publish(value(1, false, "indeterminate"));
  await act(async () => { hydrated = hydrateRoot(hydration, createElement(StrictMode, null, createElement(View)), { onRecoverableError: error => errors.push(error) }); });
  assert(errors.length === 0, "hydration differed from server snapshot");
  assert(hydration.querySelector("[data-outcome]")?.textContent === "indeterminate", "pre-hydration publication was lost");
  assert(subscriptions === 1, "StrictMode leaked subscriptions");
  root = createRoot(document.querySelector("#react")!);
  await act(async () => root!.render(createElement(StrictMode, null, createElement(Suspense, { fallback: "waiting" }, createElement(View)))));
  assert(subscriptions === 2, "concurrent root subscription missing");
  const coldMountMs = performance.now() - coldStart;
  assert(coldMountMs < 1000, "cold framework interaction exceeded 1s baseline");
  const input = document.querySelector<HTMLInputElement>("#react input")!;
  input.focus(); input.value = "current user intent"; input.setSelectionRange(2, 5);
  let focusedInput = input;
  svelte = mount(Component, { target: document.querySelector("#svelte")!, props: { store: source, failedStore, conflictStore } });
  await tick();
  assert(subscriptions === 3, "Svelte mount did not attach exactly one subscription");
  assert(document.querySelector("[data-svelte]")?.textContent === "1:pending", "Svelte initial value missing");
  const anchors = ["#react", "#svelte"].map(selector => {
    const list = document.querySelector<HTMLElement>(`${selector} [data-scroll]`)!;
    list.scrollTop = 120;
    const anchor = list.querySelector<HTMLElement>('[data-anchor="5"]')!;
    return { list, anchor, top: anchor.getBoundingClientRect().top };
  });
  const retained = (): void => {
    for (const { list, anchor, top } of anchors) {
      assert(list.scrollTop === 120 && Math.abs(anchor.getBoundingClientRect().top - top) < 1, "reconciliation moved scroll anchor");
    }
    assert(document.activeElement === focusedInput && focusedInput.value === "current user intent" && focusedInput.selectionStart === 2 && focusedInput.selectionEnd === 5, "reconciliation lost focus/selection/user intent");
  };
  const accessible = (expected: string, attribute = "outcome"): void => {
    for (const selector of [`#react [data-${attribute}]`, `#svelte [data-${attribute === "outcome" ? "svelte-outcome" : attribute}]`]) {
      const status = document.querySelector(selector)!;
      assert(status.getAttribute("role") === "status" && status.getAttribute("aria-live") === "polite" &&
        status.getAttribute("aria-atomic") === "true" && status.textContent === expected, "accessible reconciliation status missing");
    }
    retained();
  };
  accessible("indeterminate");
  const before = renders;
  await act(async () => { store.publish(store.getSnapshot()); });
  assert(renders === before, "unchanged snapshot rendered");
  let previousRenders = renders;
  await act(async () => { failedStore.publish(value(2, false, "rejected")); });
  await tick();
  accessible("rejected", "failed");
  assert(renders - previousRenders <= 4, "failure exceeded two StrictMode roots' render baseline");
  previousRenders = renders;
  focusedInput = document.querySelector<HTMLInputElement>("#svelte input")!;
  focusedInput.focus(); focusedInput.value = "current user intent"; focusedInput.setSelectionRange(2, 5);
  await act(async () => startTransition(() => {
    store.publish(value(1, true, "completed"));
  }));
  await tick();
  assert(document.querySelector("#react [data-react]")?.textContent === "1:settled", "React reconciliation missing");
  assert(document.querySelector("[data-svelte]")?.textContent === "1:settled", "Svelte reconciliation differed");
  accessible("completed");
  assert(renders - previousRenders <= 4, "reconciliation exceeded two StrictMode roots' render baseline");
  previousRenders = renders;
  await act(async () => { conflictStore.publish(value(4, true, "replaced")); });
  await tick();
  accessible("replaced", "conflict");
  assert(renders - previousRenders <= 4, "conflict exceeded two StrictMode roots' render baseline");
  await act(async () => flushSync(() => root!.unmount())); root = undefined;
  await act(async () => hydrated!.unmount()); hydrated = undefined;
  await unmount(svelte); svelte = undefined;
  assert(subscriptions === 0, "unmount leaked React subscriptions");
  store.dispose(); failedStore.dispose(); conflictStore.dispose();
  result.dataset.status = "passed";
  result.textContent = JSON.stringify({ controls: "React Strict/concurrent/SSR/hydration, Svelte mount/unmount, cached snapshots, focus/selection/scroll, accessible reconciliation", coldMountMs, renders, subscriptions, warmMainThreadMs, warmReads: 100_000, warmSnapshotAllocations: 0 });
} catch (error) {
  result.dataset.status = "failed";
  result.textContent = String(error instanceof Error ? error.stack : error);
} finally {
  if (root) await act(async () => root!.unmount());
  if (hydrated) await act(async () => hydrated!.unmount());
  if (svelte) await unmount(svelte);
}
