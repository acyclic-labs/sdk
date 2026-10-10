import { expect, test } from "bun:test";
import { ClientViews, type ClientDomain, type ClientObservation, type ClientViewLimits } from "../src/client-views.js";
import { ProjectionStore, type ClientEvent, type ClientListener } from "../src/client.js";
import { WasmClientViews } from "../generated/wasm/acyclic_harness_wasm.js";

type Value = Readonly<{ count: number }>;
type Evidence = ClientObservation<Value>;
const limits: ClientViewLimits = { records: 4, branches: 4, edges: 8, bytes: 1_000_000, work: 256, retention: 10n, visible: 4 };
const value = (count: number): Value => Object.freeze({ count });
const evidence = (key: string, revision: number, projected: Value, operation: Evidence["operation"] = null): Evidence => ({
  fact: { key, basis: String(revision), value: projected, bytes: 512 }, operation, work: 1,
});
const domain: ClientDomain<Value, null, Evidence> = {
  identity: "1",
  validate: () => ({ bytes: 512, work: 1 }),
  observe(input, current) {
    if (current && input.fact && Number(input.fact.basis) < Number(current.basis)) throw new Error("reordered evidence");
    if (current && input.fact && input.fact.basis === current.basis && input.fact.value.count !== current.value.count) throw new Error("conflicting evidence");
    return input;
  },
  corresponds: (predicted, canonical) => ({ matches: predicted.count === canonical.count, work: 1 }),
};

await ClientViews.initialize();

test("restored hypotheses retain dependencies and require trusted outcome receipts", () => {
  const receipt = evidence("a", 1, value(1), ["op", "Completed"]);
  const recovering: ClientDomain<Value, null, Evidence> = {
    ...domain,
    restore(fact, branch) {
      if (branch.adapter !== "1") throw new Error("adapter drift");
      if (branch.prediction === "Pending" && branch.outcome === "Unknown" && branch.basis === fact.basis) {
        return { bytes: 512, work: 1 };
      }
      if (branch.prediction === "Confirmed" && branch.outcome === "Completed" &&
          branch.operation === receipt.operation?.[0] && fact.basis === receipt.fact?.basis &&
          branch.predicted.count === receipt.fact.value.count) return { bytes: 512, work: 1 };
      throw new Error("no trusted recovery receipt");
    },
  };
  const source = new ClientViews(recovering, "71", 0n, limits);
  source.observe("a", evidence("a", 0, value(0)));
  const parent = source.begin({ key: "a", basis: "0", operation: "op", predicted: value(1), assumption: null, dependencies: [], expires: 5n });
  source.observe("a", receipt);
  const child = source.begin({ key: "a", basis: "1", operation: "child", predicted: value(2), assumption: null,
    dependencies: [{ branch: parent, requirement: "Confirmed" }], expires: 5n });
  const savedParent = source.checkpoint(parent);
  const savedChild = source.checkpoint(child);
  const restored = new ClientViews(recovering, "71", source.sequence(), limits);
  restored.observe("a", receipt);
  const before = restored.residency();
  expect(() => restored.restore(savedChild)).toThrow("Missing");
  expect(restored.residency()).toEqual(before);
  expect(() => restored.restore({ ...savedChild, prediction: "Confirmed", outcome: "Completed" })).toThrow();
  expect(restored.residency()).toEqual(before);
  restored.restore(savedParent);
  restored.restore(savedChild);
  expect(restored.view("a", [child]).value).toBe(savedChild.predicted);
  expect(restored.checkpoint(child).dependencies).toEqual(savedChild.dependencies);
  expect(restored.checkpoint(parent).outcome).toBe("Completed");
  const next = restored.begin({ key: "a", basis: "1", operation: null, predicted: value(3), assumption: null,
    dependencies: [], expires: 5n });
  expect(next).toBe("71:3");
  const unsupported = new ClientViews(domain, "71", source.sequence(), limits);
  unsupported.observe("a", receipt);
  expect(() => unsupported.restore(savedParent)).toThrow("Unsupported");
  source.dispose(); restored.dispose(); unsupported.dispose();
});

test("Rust binding keeps stable immutable references, narrow notifications, honest outcome and provenance", () => {
  const client = new ClientViews(domain, "7", 0n, limits);
  const canonical = value(0);
  client.observe("a", evidence("a", 0, canonical));
  client.observe("b", evidence("b", 0, value(2)));
  const branch = client.begin({ key: "a", basis: "0", operation: "op", predicted: value(1), assumption: null, dependencies: [], expires: 5n });
  const selected = client.select("a", [branch]);
  const sibling = client.select("a", [branch]);
  const unrelated = client.select("b");
  const old = selected.getSnapshot();
  let notifications = 0;
  let unrelatedCalls = 0;
  const off = selected.subscribe(() => {
    notifications++;
    expect(sibling.getSnapshot()).toEqual(selected.getSnapshot());
  });
  const otherOff = unrelated.subscribe(() => unrelatedCalls++);
  try {
    expect(selected.getSnapshot()).toBe(old);
    expect(old.branch).toBe(branch);
    expect(old.adapter).toBe("1");
    expect(old.outcome).toBe("Unknown");
    const receipt = client.observe("a", { fact: null, operation: ["op", "Indeterminate"], work: 1 });
    expect(typeof receipt.work).toBe("number");
    expect(receipt.work).toBeGreaterThan(0);
    expect(selected.getSnapshot().outcome).toBe("Indeterminate");
    expect(notifications).toBe(1);
    client.observe("a", { fact: null, operation: ["op", "Indeterminate"], work: 1 });
    expect(notifications).toBe(1);
    const confirmed = value(1);
    client.observe("a", evidence("a", 1, confirmed, ["op", "Completed"]));
    expect(selected.getSnapshot().value).toBe(confirmed);
    expect(selected.getSnapshot().branch).toBeNull();
    expect(selected.getSnapshot().hypotheses[0]?.prediction).toBe("Confirmed");
    expect(selected.getSnapshot().hypotheses[0]?.outcome).toBe("Completed");
    expect(notifications).toBe(2);
    expect(unrelatedCalls).toBe(0);
    expect(old.value.count).toBe(1);
    expect(old.outcome).toBe("Unknown");
    client.observe("a", evidence("a", 1, value(1)));
    expect(notifications).toBe(2);
    client.discard(branch);
    expect(selected.getSnapshot().hypotheses).toEqual([]);
    expect(notifications).toBe(3);
    expect(() => client.release("a")).toThrow("dispose");
    selected.dispose();
    sibling.dispose();
    client.release("a");
    expect(client.residency().slice(0, 3)).toEqual([1, 0, 0]);
  } finally { off(); otherOff(); selected.dispose(); sibling.dispose(); unrelated.dispose(); client.dispose(); }
  client.dispose();
  expect(() => client.view("a")).toThrow("disposed");
});

test("raw generated WASM rejects negative/overflow/precision-losing ticks and costs", () => {
  const raw = new WasmClientViews("1", "2", 0n, limits, domain.validate, domain.observe, domain.corresponds);
  try {
    for (const sequence of [-1n, 1n << 64n, Number.MAX_SAFE_INTEGER + 1]) {
      expect(() => new WasmClientViews("1", "2", sequence as bigint, limits, domain.validate, domain.observe, domain.corresponds)).toThrow();
      expect(() => raw.advance(sequence as bigint)).toThrow();
    }
    for (const work of [-1, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
      expect(() => raw.observe("a", { ...evidence("a", 0, value(0)), work })).toThrow();
      expect(raw.residency()).toEqual([0, 0, 0, 0]);
    }
    raw.observe("a", evidence("a", 0, value(0)));
    const snapshot = raw.view("a", []);
    expect(() => raw.observe("a", { ...evidence("a", 1, value(1)), work: limits.work + 1 })).toThrow("Budget");
    expect(raw.view("a", []).value).toBe(snapshot.value);
    const hugeOverlays = new Array(limits.visible + 1);
    Object.defineProperty(hugeOverlays, 0, { get() { throw new Error("overlays parsed before admission"); } });
    expect(() => raw.view("a", hugeOverlays)).toThrow("Budget");
    const hugeDependencies = new Array(limits.edges + 1);
    Object.defineProperty(hugeDependencies, 0, { get() { throw new Error("dependencies parsed before admission"); } });
    expect(() => raw.begin({ dependencies: hugeDependencies }, value(1), null)).toThrow("Budget");
    const narrow = evidence("a", 1, value(1));
    Object.defineProperty(narrow, "unrelated", { enumerable: true, get() { throw new Error("unrelated field enumerated"); } });
    raw.observe("a", narrow);
    expect(raw.view("a", []).value).toBe(narrow.fact!.value);
  } finally { raw.free(); }
});

test("binding failure atomicity, ambiguity, dependencies, retention and finite selection capacity", () => {
  const client = new ClientViews(domain, "8", 0n, { ...limits, records: 1, branches: 2 });
  client.observe("a", evidence("a", 0, value(0)));
  const request = { key: "a", basis: "0", operation: "op", predicted: value(1), assumption: null, dependencies: [], expires: 5n } as const;
  try {
    expect(() => client.begin({ ...request, basis: "bad" })).toThrow("StaleBasis");
    const branch = client.begin(request);
    expect(branch).toBe("8:1");
    for (const alias of ["08:1", "8:01", "+8:1", "8:+1"] as const) {
      expect(() => client.select("a", [alias])).toThrow("Conflict");
      expect(() => client.discard(alias)).toThrow("Conflict");
    }
    expect(() => client.begin(request)).toThrow("Conflict");
    const child = client.begin({ ...request, operation: "child", dependencies: [{ branch, requirement: "Prediction" }] });
    expect(() => client.view("a", [branch, child])).toThrow("Ambiguous");
    const selected = client.select("a", [child]);
    const unchanged = selected.getSnapshot();
    expect(() => client.observe("a", evidence("a", -1, value(9)))).toThrow("Unsupported");
    expect(selected.getSnapshot()).toBe(unchanged);
    client.observe("a", { fact: null, operation: ["op", "Rejected"], work: 1 });
    expect(selected.getSnapshot().value.count).toBe(0);
    expect(selected.getSnapshot().hypotheses[0]?.prediction).toBe("Invalidated");
    const second = client.select("a");
    const third = client.select("a");
    expect(() => client.select("a")).toThrow("capacity");
    const before = client.residency();
    expect(() => client.advance(-1n)).toThrow();
    expect(client.residency()).toEqual(before);
    client.advance(5n);
    expect(selected.getSnapshot().hypotheses).toEqual([]);
    selected.dispose(); second.dispose(); third.dispose();
    client.release("a");
    expect(client.residency()).toEqual([0, 0, 0, 0]);
  } finally { client.dispose(); }
});

test("ProjectionStore explicitly starts before delivery and retains updates across subscriber churn", () => {
  const listeners = new Set<ClientListener<number>>();
  const client = { subscribe(listener: ClientListener<number>) { listeners.add(listener); return () => { listeners.delete(listener); }; } };
  const store = new ProjectionStore(client, 0, (state, event) => state + event.event);
  expect(listeners.size).toBe(0);
  store.start(); store.start();
  expect(listeners.size).toBe(1);
  let calls = 0;
  const stop = store.subscribe(() => calls++);
  const emit = (event: number): void => {
    for (const listener of listeners) listener({ authority: { kind: "conversation", id: "a" }, operationId: "op", revision: 1n, event } as ClientEvent<number>);
  };
  emit(0);
  expect(calls).toBe(0);
  emit(1);
  expect(calls).toBe(1);
  stop(); emit(2);
  expect(store.getSnapshot()).toBe(3);
  expect(calls).toBe(1);
  store.dispose(); store.dispose(); emit(4);
  expect(listeners.size).toBe(0);
  expect(store.getSnapshot()).toBe(3);
  expect(() => store.start()).toThrow("disposed");
});

test("bounded reordered/duplicate/partial outcomes preserve unrelated authoritative values", () => {
  const outcomes = ["Admitted", "Indeterminate", "Completed", "Rejected", "Cancelled"] as const;
  let explored = 0;
  for (const first of outcomes) for (const second of outcomes) for (const repeated of [false, true]) {
    const client = new ClientViews(domain, "10", 0n, limits);
    const unrelated = value(42);
    client.observe("a", evidence("a", 0, value(0)));
    client.observe("b", evidence("b", 0, unrelated));
    const branch = client.begin({ key: "a", basis: "0", operation: "original", predicted: value(1), assumption: null, dependencies: [], expires: 5n });
    const selected = client.select("a", [branch]);
    try {
      for (const outcome of repeated ? [first, second, second] : [first, second]) {
        const observation: Evidence = outcome === "Completed" ? evidence("a", 1, value(1), ["original", outcome])
          : { fact: null, operation: ["original", outcome], work: 1 };
        const before = selected.getSnapshot();
        const residency = client.residency();
        try { client.observe("a", observation); }
        catch {
          expect(selected.getSnapshot()).toBe(before);
          expect(client.residency()).toEqual(residency);
        }
        expect(client.view("b").value).toBe(unrelated);
        expect(client.residency()[1]).toBe(1);
        expect(selected.getSnapshot().hypotheses[0]?.branch).toBe(branch);
      }
      explored++;
    } finally { selected.dispose(); client.dispose(); }
  }
  expect(explored).toBe(50);
});
