import { beforeAll, expect, test } from "bun:test";
import "fake-indexeddb/auto";
import { CredentialCache, CredentialSnapshot } from "../../auth/src/credential.js";
import { WasmExplorer, WasmStream } from "../generated/wasm/acyclic_harness_wasm.js";
import { AgentExplorer } from "../src/explorer.js";
import { ensureClientWasm } from "../src/wasm-runtime.js";

beforeAll(ensureClientWasm);

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
}

function database(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open("acyclic-reader", 1);
    request.onupgradeneeded = () => request.result.createObjectStore("snapshots");
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

function saved(db: IDBDatabase, key: string): Promise<Uint8Array | undefined> {
  return new Promise((resolve, reject) => {
    const request = db.transaction("snapshots").objectStore("snapshots").get(key);
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

function seed(db: IDBDatabase, key: string, bytes: Uint8Array): Promise<void> {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction("snapshots", "readwrite");
    transaction.objectStore("snapshots").put(bytes, key);
    transaction.oncomplete = () => resolve();
    transaction.onabort = () => reject(transaction.error);
  });
}

test("pending source admission preserves persisted bytes and disposal cancels its owner", async () => {
  const entered = deferred<void>(), cancelled = deferred<void>();
  const refresh = deferred<CredentialSnapshot>();
  let blocked = false, calls = 0;
  const cache = new CredentialCache(async (_, signal) => {
    calls++;
    if (blocked) {
      signal.addEventListener("abort", () => cancelled.resolve(), { once: true });
      entered.resolve();
      return refresh.promise;
    }
    return new CredentialSnapshot("synthetic-owner", "account/audience", 100);
  }, () => ({ nowUnixSeconds: 1, revision: "account/audience" }));
  const db = await database(), key = crypto.randomUUID();
  const stream = WasmStream.connect("https://localhost.invalid", cache.provider);
  const admitted = deferred<void>();
  let core: WasmExplorer | undefined = new WasmExplorer(stream, () => admitted.resolve(), 0);
  let explorer: AgentExplorer | undefined;
  try {
    expect(core.snapshot()).toBeUndefined();
    await admitted.promise;
    expect(core.health().credentials).toBe("current");
    const snapshot = core.snapshot();
    if (snapshot === undefined) throw new Error("admitted reader did not produce a snapshot");
    expect(snapshot.length).toBeGreaterThan(0);
    core.free(); core = undefined;
    await seed(db, key, snapshot);

    blocked = true;
    cache.invalidate();
    explorer = await AgentExplorer.open(stream, { persist: key, render() {} });
    await entered.promise;
    expect(explorer.health().credentials).toBe("pending");
    expect(explorer.health().snapshotBytes).toBe(BigInt(snapshot.length));
    explorer.save();
    explorer.dispose(); explorer = undefined;
    await cancelled.promise;
    expect(await saved(db, key)).toEqual(snapshot);
    expect(calls).toBe(2);

    // A later admission restores the bytes under the same source revision and expiry.
    blocked = false;
    const ready = deferred<void>();
    core = new WasmExplorer(stream, () => ready.resolve(), 0);
    core.restore(snapshot);
    await ready.promise;
    expect(core.health().credentials).toBe("current");
    expect(core.health().snapshotBytes).toBe(0n);
    expect(core.snapshot()).toEqual(snapshot);
  } finally {
    explorer?.dispose(); core?.free(); stream.free(); db.close();
    refresh.resolve(new CredentialSnapshot("synthetic-owner", "account/audience", 100));
  }
});

test("settled unsupported source deletes persisted bytes instead of preserving them", async () => {
  const stream = WasmStream.connect("https://localhost.invalid", () => "synthetic-raw");
  const rendered = deferred<void>();
  const db = await database(), key = crypto.randomUUID();
  let explorer: AgentExplorer | undefined;
  try {
    // Capture bytes from the real local reader, not a handwritten snapshot fixture.
    const local = new WasmStream();
    const core = new WasmExplorer(local, () => {}, 0);
    try {
      const snapshot = core.snapshot();
      if (snapshot === undefined) throw new Error("local reader did not produce a snapshot");
      await seed(db, key, snapshot);
    } finally { core.free(); local.free(); }
    explorer = await AgentExplorer.open(stream, { persist: key, render() { rendered.resolve(); } });
    await rendered.promise;
    expect(explorer.health().credentials).toBe("unsupported");
    explorer.save();
    expect(await saved(db, key)).toBeUndefined();
  } finally { explorer?.dispose(); stream.free(); db.close(); }
});
