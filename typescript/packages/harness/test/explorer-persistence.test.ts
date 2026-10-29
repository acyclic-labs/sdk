import { beforeAll, expect, spyOn, test } from "bun:test";
import "fake-indexeddb/auto";
import { IDBFactory as ModelIDBFactory } from "fake-indexeddb";
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

test("render disposal retires scheduling before returning to the frame handler", async () => {
  const rendered = deferred<void>();
  const stream = WasmStream.connect("https://localhost.invalid", () => "synthetic-raw");
  let explorer: AgentExplorer | undefined;
  try {
    explorer = await AgentExplorer.open(stream, {
      render(_, current) {
        current.dispose();
        rendered.resolve();
      },
    });
    await rendered.promise;
    const timers = spyOn(globalThis, "setTimeout");
    try {
      explorer.schedule();
      explorer.schedule();
      explorer.save();
      explorer.dispose();
      expect(timers).not.toHaveBeenCalled();
    } finally { timers.mockRestore(); }
  } finally { explorer?.dispose(); stream.free(); }
});

test("a failed persistence transaction still releases the reader", async () => {
  const stream = new WasmStream();
  const explorer = await AgentExplorer.open(stream, { persist: crypto.randomUUID(), render() {} });
  const transaction = IDBDatabase.prototype.transaction;
  const transactions = spyOn(IDBDatabase.prototype, "transaction").mockImplementation(function (
    this: IDBDatabase, stores: string | string[], mode?: IDBTransactionMode, options?: IDBTransactionOptions,
  ) {
    this.close();
    return transaction.call(this, stores, mode, options);
  });
  try {
    expect(() => explorer.dispose()).toThrow();
    // The actual WASM owner is gone even though IndexedDB refused the write.
    expect(() => explorer.health()).toThrow();
    explorer.dispose();
    explorer.schedule();
  } finally { transactions.mockRestore(); explorer.dispose(); stream.free(); }
});

test("disposal commits its saved bytes and releases the database connection", async () => {
  const previous = globalThis.indexedDB;
  globalThis.indexedDB = new ModelIDBFactory();
  const connections: IDBDatabase[] = [];
  const snapshots = spyOn(WasmExplorer.prototype, "snapshot");
  const open = IDBFactory.prototype.open;
  const opens = spyOn(IDBFactory.prototype, "open").mockImplementation(function (
    this: IDBFactory, name: string, version?: number,
  ) {
    const request = open.call(this, name, version);
    request.addEventListener("success", () => { connections.push(request.result); });
    return request;
  });
  const stream = new WasmStream(), key = crypto.randomUUID();
  let explorer: AgentExplorer | undefined;
  let deletion: Promise<boolean> | undefined;
  try {
    explorer = await AgentExplorer.open(stream, { persist: key, render() {} });
    explorer.dispose();
    explorer.dispose();
    expect(snapshots).toHaveBeenCalledTimes(1);
    const observer = await database();
    try {
      const bytes = await saved(observer, key);
      expect(bytes).toBeInstanceOf(Uint8Array);
      expect(bytes).toEqual(snapshots.mock.results[0]?.value);
    } finally { observer.close(); }

    // The database request provides both completion and blocked events.
    // Leaking the disposed explorer's connection must fail without a timeout.
    const request = indexedDB.deleteDatabase("acyclic-reader");
    const blocked = deferred<boolean>();
    deletion = new Promise((resolve, reject) => {
      request.onsuccess = () => resolve(true);
      request.onerror = () => reject(request.error);
    });
    request.onblocked = () => blocked.resolve(false);
    expect(await Promise.race([deletion, blocked.promise])).toBe(true);
  } finally {
    explorer?.dispose(); stream.free(); opens.mockRestore(); snapshots.mockRestore();
    for (const connection of connections) connection.close();
    await deletion;
    globalThis.indexedDB = previous;
  }
});
