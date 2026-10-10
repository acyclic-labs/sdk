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
  const branch = client.begin({ key: "a", basis: "0", operation: "op", predicted: value(1), assumption: null, dependencies: [], expires: 5n });
  const store = client.select("a", [branch]);
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
    renders++;
    return createElement("section", null,
      createElement("input", { defaultValue: "draft", "aria-label": "draft" }),
      createElement("p", { "data-react": true }, `${snapshot.value.count}:${snapshot.branch === null ? "authoritative" : "hypothesis"}`),
      createElement("p", { "data-outcome": true, role: "status" }, snapshot.hypotheses.map(status => status.outcome).join(",")));
  }
  const markup = renderToString(createElement(View));
  assert(subscriptions === 0, "SSR attached a subscription");
  const hydration = document.querySelector<HTMLElement>("#hydration")!;
  hydration.innerHTML = markup;
  const errors: unknown[] = [];
  client.observe("a", { fact: null, operation: ["op", "Indeterminate"], work: 1 });
  await act(async () => { hydrated = hydrateRoot(hydration, createElement(StrictMode, null, createElement(View)), { onRecoverableError: error => errors.push(error) }); });
  assert(errors.length === 0, "hydration differed from server snapshot");
  assert(hydration.querySelector("[data-outcome]")?.textContent === "Indeterminate", "pre-hydration publication was lost");
  assert(subscriptions === 1, "StrictMode leaked subscriptions");
  root = createRoot(document.querySelector("#react")!);
  await act(async () => root!.render(createElement(StrictMode, null, createElement(Suspense, { fallback: "waiting" }, createElement(View)))));
  assert(subscriptions === 2, "concurrent root subscription missing");
  const input = document.querySelector<HTMLInputElement>("#react input")!;
  input.focus(); input.value = "current user intent"; input.setSelectionRange(2, 5);
  svelte = mount(Component, { target: document.querySelector("#svelte")!, props: { store: source } });
  await tick();
  assert(subscriptions === 3, "Svelte mount did not attach exactly one subscription");
  assert(document.querySelector("[data-svelte]")?.textContent === "1:hypothesis", "Svelte initial value missing");
  assert(document.querySelector("[data-svelte-outcome]")?.textContent === "Indeterminate", "Svelte uncertainty differed from React");
  const before = renders;
  await act(async () => { client!.observe("a", { fact: null, operation: null, work: 1 }); });
  assert(renders === before, "unchanged snapshot rendered");
  await act(async () => startTransition(() => {
    client!.observe("a", { fact: { key: "a", basis: "1", value: value(1), bytes: 512 }, operation: ["op", "Completed"], work: 1 });
  }));
  await tick();
  assert(document.querySelector("#react [data-react]")?.textContent === "1:authoritative", "React reconciliation missing");
  assert(document.querySelector("[data-svelte]")?.textContent === "1:authoritative", "Svelte reconciliation differed");
  assert(document.querySelector("[data-svelte-outcome]")?.textContent === "Completed", "Svelte authoritative outcome missing");
  assert(document.activeElement === input && input.value === "current user intent" && input.selectionStart === 2 && input.selectionEnd === 5, "reconciliation lost focus/selection/user intent");
  await act(async () => flushSync(() => root!.unmount())); root = undefined;
  await act(async () => hydrated!.unmount()); hydrated = undefined;
  await unmount(svelte); svelte = undefined;
  assert(subscriptions === 0, "unmount leaked React subscriptions");
  store.dispose(); client.discard(branch); client.release("a");
  assert(client.residency().every(item => item === 0), "disposed demand retained kernel state");
  result.dataset.status = "passed";
  assert(renders <= 13, "framework rendered beyond pinned StrictMode/pre-hydration-update baseline budget");
  result.textContent = JSON.stringify({ controls: "React Strict/concurrent/SSR/hydration, Svelte mount/unmount, cached snapshots, provenance, focus/selection", renders, subscriptions, warmMainThreadMs, warmReads: 100_000, warmSnapshotAllocations: 0 });
} catch (error) {
  result.dataset.status = "failed";
  result.textContent = String(error instanceof Error ? error.stack : error);
} finally {
  if (root) await act(async () => root!.unmount());
  if (hydrated) await act(async () => hydrated!.unmount());
  if (svelte) await unmount(svelte);
  client?.dispose();
}
