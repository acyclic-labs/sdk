import { expect, test } from "bun:test";
import { ClientViews, type ClientDomain, type ClientObservation, type ClientViewLimits } from "../src/client-views.js";
import { SnapshotStore } from "../src/client-snapshot.js";

type Value = Readonly<{ ref: string }>;
type Evidence = ClientObservation<Value>;
const value = Object.freeze({ ref: "content-addressed-body" });
const limits: ClientViewLimits = { records: 16, branches: 16, edges: 16, bytes: 1_000_000, work: 256, retention: 10n, visible: 16 };
const domain: ClientDomain<Value, null, Evidence> = {
  identity: "1", validate: () => ({ bytes: 256, work: 1 }), observe: evidence => evidence,
  corresponds: (left, right) => ({ matches: left.ref === right.ref, work: 1 }),
};
await ClientViews.initialize();

test("cold reconciliation measures discarded speculation; warm snapshots and demand residency plateau", () => {
  let snapshots = 0;
  class Measured extends ClientViews<Value, null, Evidence> {
    override view(...args: Parameters<ClientViews<Value, null, Evidence>["view"]>) {
      snapshots++;
      return super.view(...args);
    }
  }
  const client = new Measured(domain, "5", 0n, limits);
  const resident: number[] = [];
  let loaded = 0;
  const timing = (read: () => unknown): number => {
    const start = performance.now();
    for (let i = 0; i < 100_000; i++) read();
    return performance.now() - start;
  };
  try {
    const equivalent = new Measured(domain, "6", 0n, limits);
    const coldStart = performance.now();
    client.observe("0", { fact: { key: "0", basis: "0", value, bytes: 256 }, operation: null, work: 1 });
    const predicted = Object.freeze({ ref: "discarded-prediction" });
    const branch = client.begin({ key: "0", basis: "0", operation: "original", predicted, assumption: null, dependencies: [], expires: 5n });
    const beforeColdSnapshots = snapshots;
    const speculative = client.select("0", [branch]);
    const coldSnapshotAllocations = snapshots - beforeColdSnapshots;
    const coldMs = performance.now() - coldStart;
    const speculativeBytes = client.residency()[3];
    const storedBytes = new TextEncoder().encode(JSON.stringify(client.checkpoint(branch), (_, item) => typeof item === "bigint" ? String(item) : item)).byteLength;
    let speculativeNotifications = 0;
    speculative.subscribe(() => speculativeNotifications++);
    const baselineStart = performance.now();
    equivalent.observe("0", { fact: { key: "0", basis: "0", value, bytes: 256 }, operation: null, work: 1 });
    const authoritative = equivalent.select("0");
    const nonSpeculativeColdMs = performance.now() - baselineStart;
    let authoritativeNotifications = 0;
    authoritative.subscribe(() => authoritativeNotifications++);
    const confirmation = { fact: { key: "0", basis: "1", value, bytes: 256 }, operation: ["original", "Completed"] as const, work: 1 };
    const reconcileStart = performance.now();
    const speculativeWork = client.observe("0", confirmation).work;
    const reconcileMs = performance.now() - reconcileStart;
    const authoritativeWork = equivalent.observe("0", confirmation).work;
    expect(speculative.getSnapshot().value).toBe(authoritative.getSnapshot().value);
    expect(speculative.getSnapshot().branch).toBeNull();
    expect(client.checkpoint(branch).prediction).toBe("Replaced");
    expect(speculativeNotifications).toBe(authoritativeNotifications);
    expect(speculativeWork).toBeGreaterThan(authoritativeWork);
    expect(coldMs + reconcileMs).toBeLessThan(Math.max(20, nonSpeculativeColdMs * 20));
    expect(storedBytes).toBeGreaterThan(0);
    expect(speculativeBytes).toBeGreaterThan(equivalent.residency()[3]);
    speculative.dispose(); client.discard(branch);
    const discardedResidentBytes = speculativeBytes - client.residency()[3];
    expect(discardedResidentBytes).toBeGreaterThan(0);
    expect(client.residency()).toEqual(equivalent.residency());
    authoritative.dispose(); equivalent.dispose(); client.release("0");
    for (const count of [1, 8, 16]) {
      for (let i = loaded; i < count; i++) {
        client.observe(String(i), { fact: { key: String(i), basis: "0", value, bytes: 256 }, operation: null, work: 1 });
      }
      loaded = count;
      resident.push(client.residency()[3]);
    }
    const selected = client.select("0");
    const initial = selected.getSnapshot();
    const baseline = new SnapshotStore(initial);
    let notifications = 0;
    const stop = selected.subscribe(() => notifications++);
    const beforeSnapshots = snapshots;
    const beforeBytes = client.residency()[3];
    timing(baseline.getSnapshot); timing(selected.getSnapshot);
    const baselineMs = timing(baseline.getSnapshot);
    const warmMs = timing(selected.getSnapshot);
    // Both read exactly the same cached accessor. This threshold permits host
    // scheduling noise but rejects accidental per-read kernel/serialization work.
    expect(warmMs).toBeLessThan(Math.max(10, baselineMs * 5));
    for (let i = 0; i < 1_000; i++) {
      client.observe("0", { fact: { key: "0", basis: "0", value, bytes: 256 }, operation: null, work: 1 });
    }
    expect(selected.getSnapshot()).toBe(initial);
    expect(snapshots).toBe(beforeSnapshots);
    expect(notifications).toBe(0);
    expect(client.residency()[3]).toBe(beforeBytes);
    stop(); selected.dispose(); baseline.dispose();
    for (let i = 0; i < 16; i++) client.release(String(i));
    expect(client.residency()).toEqual([0, 0, 0, 0]);
    console.log(JSON.stringify({ clientViewsBaseline: { records: [1, 8, 16], accountedResidentBytes: resident, coldMs, coldSnapshotAllocations, reconcileMs, nonSpeculativeColdMs, speculativeWork, authoritativeWork, speculativeBytes, storedBytes, discardedResidentBytes, discardedPredictions: 1, speculativeNotifications, authoritativeNotifications, warmReads: 100_000, baselineMs, warmMs, snapshotAllocations: snapshots - beforeSnapshots, transferredBytes: 0, duplicateNotifications: notifications } }));
  } finally { client.dispose(); }
});
