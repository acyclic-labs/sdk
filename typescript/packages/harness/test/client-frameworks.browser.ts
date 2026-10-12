import { createElement, StrictMode, startTransition, Suspense, act } from "react";
import { createRoot, hydrateRoot } from "react-dom/client";
import { renderToString } from "react-dom/server";
import { flushSync } from "react-dom";
import { mount, unmount, tick } from "svelte";
import Component from "./client-frameworks.svelte";
import { ClientViews, type ClientDomain, type ClientObservation } from "@acyclic-labs/harness/client";
import { useClientSnapshot } from "@acyclic-labs/harness/react";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
const result = document.querySelector<HTMLElement>("#result")!;
const assert = (condition: unknown, message: string): void => { if (!condition) throw new Error(message); };
type Value = Readonly<{ count: number }>;
type Evidence = ClientObservation<Value>;
const value = (count: number): Value => Object.freeze({ count });
const domain: ClientDomain<Value, null, Evidence> = {
  identity: "1", validate: () => ({ bytes: 512, work: 1 }),
  observe: evidence => evidence,
  corresponds: (predicted, canonical) => ({ matches: predicted.count === canonical.count, work: 1 }),
};
let client: ClientViews<Value, null, Evidence> | undefined;
let root: ReturnType<typeof createRoot> | undefined;
let hydrated: ReturnType<typeof hydrateRoot> | undefined;
let svelte: ReturnType<typeof mount> | undefined;
try {
  await ClientViews.initialize();
  client = new ClientViews(domain, "9", 0n, { records: 2, branches: 4, edges: 8, bytes: 1_000_000, work: 256, retention: 10n, visible: 4 });
  client.observe("a", { fact: { key: "a", basis: "0", value: value(0), bytes: 512 }, operation: null, work: 1 });
  client.observe("b", { fact: { key: "b", basis: "0", value: value(0), bytes: 512 }, operation: null, work: 1 });
  const branch = client.begin({ key: "a", basis: "0", operation: "op", predicted: value(1), assumption: null, dependencies: [], expires: 5n });
  const failed = client.begin({ key: "a", basis: "0", operation: "failed", predicted: value(2), assumption: null, dependencies: [], expires: 5n });
  const conflict = client.begin({ key: "b", basis: "0", operation: "conflict", predicted: value(3), assumption: null, dependencies: [], expires: 5n });
  const store = client.select("a", [branch]);
  const failedStore = client.select("a", [failed]);
  const conflictStore = client.select("b", [conflict]);
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
      createElement("p", { "data-react": true }, `${snapshot.value.count}:${snapshot.branch === null ? "authoritative" : "hypothesis"}`),
      createElement("p", { "data-outcome": true, role: "status", "aria-live": "polite", "aria-atomic": true }, snapshot.hypotheses.map(status => `${status.prediction}:${status.outcome}`).join(",")),
      createElement("p", { "data-failed": true, role: "status", "aria-live": "polite", "aria-atomic": true }, failure.hypotheses.map(status => `${status.prediction}:${status.outcome}`).join(",")),
      createElement("p", { "data-conflict": true, role: "status", "aria-live": "polite", "aria-atomic": true }, conflicting.hypotheses.map(status => `${status.prediction}:${status.outcome}`).join(",")),
      createElement("ol", { "data-scroll": true, style: { height: 80, overflowY: "scroll", margin: 0 } },
        Array.from({ length: 20 }, (_, i) => createElement("li", { key: i, "data-anchor": i, style: { height: 24 } }, `${i}:${snapshot.value.count}`))));
  }
  const markup = renderToString(createElement(View));
  assert(subscriptions === 0, "SSR attached a subscription");
  const hydration = document.querySelector<HTMLElement>("#hydration")!;
  hydration.innerHTML = markup;
  const errors: unknown[] = [];
  const coldStart = performance.now();
  client.observe("a", { fact: null, operation: ["op", "Indeterminate"], work: 1 });
  await act(async () => { hydrated = hydrateRoot(hydration, createElement(StrictMode, null, createElement(View)), { onRecoverableError: error => errors.push(error) }); });
  assert(errors.length === 0, "hydration differed from server snapshot");
  assert(hydration.querySelector("[data-outcome]")?.textContent === "Pending:Indeterminate", "pre-hydration publication was lost");
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
  assert(document.querySelector("[data-svelte]")?.textContent === "1:hypothesis", "Svelte initial value missing");
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
  accessible("Pending:Indeterminate");
  const before = renders;
  await act(async () => { client!.observe("a", { fact: null, operation: null, work: 1 }); });
  assert(renders === before, "unchanged snapshot rendered");
  let previousRenders = renders;
  await act(async () => { client!.observe("a", { fact: null, operation: ["failed", "Rejected"], work: 1 }); });
  await tick();
  accessible("Removed:Rejected", "failed");
  assert(renders - previousRenders <= 4, "failure exceeded two StrictMode roots' render baseline");
  previousRenders = renders;
  focusedInput = document.querySelector<HTMLInputElement>("#svelte input")!;
  focusedInput.focus(); focusedInput.value = "current user intent"; focusedInput.setSelectionRange(2, 5);
  await act(async () => startTransition(() => {
    client!.observe("a", { fact: { key: "a", basis: "1", value: value(1), bytes: 512 }, operation: ["op", "Completed"], work: 1 });
  }));
  await tick();
  assert(document.querySelector("#react [data-react]")?.textContent === "1:authoritative", "React reconciliation missing");
  assert(document.querySelector("[data-svelte]")?.textContent === "1:authoritative", "Svelte reconciliation differed");
  accessible("Confirmed:Completed");
  assert(renders - previousRenders <= 4, "reconciliation exceeded two StrictMode roots' render baseline");
  previousRenders = renders;
  await act(async () => { client!.observe("b", { fact: { key: "b", basis: "1", value: value(4), bytes: 512 }, operation: ["conflict", "Completed"], work: 1 }); });
  await tick();
  accessible("Replaced:Completed", "conflict");
  assert(renders - previousRenders <= 4, "conflict exceeded two StrictMode roots' render baseline");
  await act(async () => flushSync(() => root!.unmount())); root = undefined;
  await act(async () => hydrated!.unmount()); hydrated = undefined;
  await unmount(svelte); svelte = undefined;
  assert(subscriptions === 0, "unmount leaked React subscriptions");
  store.dispose(); failedStore.dispose(); conflictStore.dispose();
  client.discard(branch); client.discard(failed); client.discard(conflict); client.release("a"); client.release("b");
  assert(client.residency().every(item => item === 0), "disposed demand retained kernel state");
  result.dataset.status = "passed";
  result.textContent = JSON.stringify({ controls: "React Strict/concurrent/SSR/hydration, Svelte mount/unmount, cached snapshots, provenance, focus/selection/scroll, accessible reconciliation", coldMountMs, renders, subscriptions, warmMainThreadMs, warmReads: 100_000, warmSnapshotAllocations: 0 });
} catch (error) {
  result.dataset.status = "failed";
  result.textContent = String(error instanceof Error ? error.stack : error);
} finally {
  if (root) await act(async () => root!.unmount());
  if (hydrated) await act(async () => hydrated!.unmount());
  if (svelte) await unmount(svelte);
  client?.dispose();
}
