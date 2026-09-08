import { expect, test } from "bun:test";
import { IDBFactory } from "fake-indexeddb";
import {
  HarnessClient,
  HydrationCache,
  IndexedDbClientStore,
  MemoryOutbox,
  PageWindow,
  TerminalAdmissionError,
  type ClientCommand,
  type Connection,
  type CursorStore,
  type Delivery,
  type OperationId,
  type ReplayCursor,
  type Transport,
} from "../src/index.js";

const operationId = "01010101-0101-0101-0101-010101010101" as OperationId;
const authority = { kind: "conversation" as const, id: "conversation-1" };

test("offline outbox accepts only explicitly safe non-approval commands", async () => {
  const outbox = new MemoryOutbox();
  const transport: Transport = { connect: async () => { throw new Error("offline"); } };
  const client = new HarnessClient(transport, outbox);
  const safe: ClientCommand = {
    operationId,
    authority,
    kind: "message.append",
    payload: { text: "hello" },
    offlineSafe: true,
  };
  await client.submit(safe);
  expect(await outbox.load()).toEqual([safe]);
  await expect(
    client.submit({ ...safe, kind: "interaction.resolve.approval" }),
  ).rejects.toThrow("not safe");
});

test("IndexedDB atomically persists outbox acknowledgements and replay cursors across restart", async () => {
  const indexedDB = new IDBFactory();
  const options = { indexedDB, databaseName: "durable-client", maximumCommands: 2, maximumBytes: 4_096 };
  const first = new IndexedDbClientStore(options);
  const command: ClientCommand = {
    operationId,
    authority,
    kind: "message.append",
    payload: { text: "durable" },
    offlineSafe: true,
  };
  await first.put(command);
  await first.putCursor(authority, { generation: "one", revision: 1n });

  const reopened = new IndexedDbClientStore(options);
  expect(await reopened.load()).toEqual([command]);
  expect((await reopened.loadCursors()).get("conversation:conversation-1")).toEqual({
    generation: "one",
    revision: 1n,
  });
  await reopened.commit(authority, { generation: "one", revision: 2n }, operationId);

  const verified = new IndexedDbClientStore(options);
  expect(await verified.load()).toEqual([]);
  expect((await verified.loadCursors()).get("conversation:conversation-1")?.revision).toBe(2n);
});

test("IndexedDB outbox enforces configured command and byte bounds", async () => {
  const store = new IndexedDbClientStore({
    indexedDB: new IDBFactory(),
    databaseName: "bounded-client",
    maximumCommands: 1,
    maximumBytes: 256,
  });
  await store.put({ operationId, authority, kind: "message.append", payload: {}, offlineSafe: true });
  await expect(store.put({
    operationId: "02020202-0202-0202-0202-020202020202" as OperationId,
    authority,
    kind: "message.append",
    payload: {},
    offlineSafe: true,
  })).rejects.toThrow("capacity exceeded");
  const customArray: unknown[] & { extra?: string } = [];
  customArray.extra = "x".repeat(10_000);
  await expect(store.put({ operationId, authority, kind: "message.append", payload: customArray }))
    .rejects.toThrow("custom properties");
  const accessor = {} as { value: string };
  Object.defineProperty(accessor, "value", { enumerable: true, get: () => "x".repeat(10_000) });
  await expect(store.put({ operationId, authority, kind: "message.append", payload: accessor }))
    .rejects.toThrow("accessor");
});

test("IndexedDB rejects non-canonical structured-clone payloads", async () => {
  const store = new IndexedDbClientStore({
    indexedDB: new IDBFactory(),
    databaseName: "canonical-client",
    maximumBytes: 128,
  });
  for (const payload of [new Map([["large", "x".repeat(1_024)]]), new Set(["x".repeat(1_024)])]) {
    await expect(store.put({
      operationId,
      authority,
      kind: "message.append",
      payload,
      offlineSafe: true,
    })).rejects.toThrow("non-canonical structured value");
  }
  await expect(store.put({
    operationId,
    authority,
    kind: "message.append",
    payload: Number.POSITIVE_INFINITY,
  })).rejects.toThrow("non-finite number");
  await expect(store.put({
    operationId,
    authority,
    kind: "message.append",
    payload: { toJSON: () => ({}) },
  })).rejects.toThrow("non-data value");
  await expect(store.put({
    operationId,
    authority,
    kind: "message.append",
    payload: new ArrayBuffer(1_024),
  })).rejects.toThrow("capacity exceeded");
});

test("IndexedDB preserves enqueue order across restart and isolates database namespaces", async () => {
  const indexedDB = new IDBFactory();
  const first = new IndexedDbClientStore({ indexedDB, databaseName: "ordered-client" });
  const laterKey: ClientCommand = {
    operationId: "ffffffff-ffff-ffff-ffff-ffffffffffff" as OperationId,
    authority,
    kind: "first",
    payload: {},
  };
  const earlierKey: ClientCommand = {
    operationId: "00000000-0000-0000-0000-000000000000" as OperationId,
    authority,
    kind: "second",
    payload: {},
  };
  await first.put(laterKey);
  await first.put(earlierKey);
  expect(await new IndexedDbClientStore({ indexedDB, databaseName: "ordered-client" }).load()).toEqual([
    laterKey,
    earlierKey,
  ]);
  expect(await new IndexedDbClientStore({ indexedDB, databaseName: "other-client" }).load()).toEqual([]);
});

test("IndexedDB migration keeps version-one records ahead of newly enqueued commands", async () => {
  const indexedDB = new IDBFactory();
  const opening = indexedDB.open("legacy-client", 1);
  opening.onupgradeneeded = () => {
    opening.result.createObjectStore("outbox", { keyPath: "operationId" });
    opening.result.createObjectStore("cursors", { keyPath: "authority" });
  };
  const database = await new Promise<IDBDatabase>((resolve, reject) => {
    opening.onsuccess = () => resolve(opening.result);
    opening.onerror = () => reject(opening.error);
  });
  const legacy: ClientCommand = {
    operationId: "ffffffff-ffff-ffff-ffff-ffffffffffff" as OperationId,
    authority,
    kind: "legacy",
    payload: {},
  };
  const transaction = database.transaction("outbox", "readwrite");
  transaction.objectStore("outbox").put({ operationId: legacy.operationId, bytes: 1, command: legacy });
  await new Promise<void>((resolve, reject) => {
    transaction.oncomplete = () => resolve();
    transaction.onerror = () => reject(transaction.error);
  });
  database.close();

  const migrated = new IndexedDbClientStore({ indexedDB, databaseName: "legacy-client" });
  const current: ClientCommand = {
    operationId: "00000000-0000-0000-0000-000000000000" as OperationId,
    authority,
    kind: "current",
    payload: {},
  };
  await migrated.put(current);
  expect(await migrated.load()).toEqual([legacy, current]);
});

test("rebase waits for an in-flight submit before publishing the new replay epoch", async () => {
  let sendStarted: (() => void) | undefined;
  let releaseSend: (() => void) | undefined;
  const sending = new Promise<void>(resolve => { sendStarted = resolve; });
  const released = new Promise<void>(resolve => { releaseSend = resolve; });
  let iterationStarted: (() => void) | undefined;
  const iterating = new Promise<void>(resolve => { iterationStarted = resolve; });
  const controller = new AbortController();
  const client = new HarnessClient({
    async connect() {
      return {
        async send() {
          sendStarted?.();
          await released;
        },
        close() {},
        async *[Symbol.asyncIterator]() {
          iterationStarted?.();
          await new Promise(resolve => controller.signal.addEventListener("abort", resolve));
        },
      };
    },
  });
  const running = client.run(controller.signal);
  await iterating;
  const submitted = client.submit({ operationId, authority, kind: "online-only", payload: {} });
  await sending;
  let reset = false;
  const rebased = client.rebase(authority, { generation: "new", revision: 0n }, () => { reset = true; });
  await Promise.resolve();
  expect(reset).toBe(false);
  releaseSend?.();
  await submitted;
  await rebased;
  expect(reset).toBe(true);
  controller.abort();
  await running;
});

test("rebase waits for an in-flight outbox flush and fences later stale sends", async () => {
  let sendStarted: (() => void) | undefined;
  let releaseSend: (() => void) | undefined;
  const sending = new Promise<void>(resolve => { sendStarted = resolve; });
  const released = new Promise<void>(resolve => { releaseSend = resolve; });
  const controller = new AbortController();
  const outbox = new MemoryOutbox();
  await outbox.put({ operationId, authority, kind: "queued", payload: {}, offlineSafe: true });
  let sends = 0;
  const client = new HarnessClient({
    async connect() {
      return {
        async send() {
          sends += 1;
          sendStarted?.();
          await released;
        },
        close() {},
        async *[Symbol.asyncIterator]() {
          await new Promise(resolve => controller.signal.addEventListener("abort", resolve));
        },
      };
    },
  }, outbox);
  const running = client.run(controller.signal);
  await sending;
  let reset = false;
  const rebased = client.rebase(authority, { generation: "new", revision: 0n }, () => { reset = true; });
  await Promise.resolve();
  expect(reset).toBe(false);
  releaseSend?.();
  await rebased;
  expect(reset).toBe(true);
  expect(sends).toBe(1);
  controller.abort();
  await running;
});

test("rebase fails closed and serializes the first reconnect behind durable persistence", async () => {
  let release: (() => void) | undefined;
  let writeStarted: (() => void) | undefined;
  const started = new Promise<void>(resolve => { writeStarted = resolve; });
  let rejectWrite = true;
  const cursors = new Map<string, ReplayCursor>();
  const cursorStore: CursorStore = {
    async loadCursors() { return new Map(cursors); },
    async putCursor(_authority, cursor) {
      if (rejectWrite) throw new Error("persistence failed");
      writeStarted?.();
      await new Promise<void>(resolve => { release = resolve; });
      cursors.set("conversation:conversation-1", cursor);
    },
    async deleteCursor() { cursors.delete("conversation:conversation-1"); },
  };
  const controller = new AbortController();
  let connected: ReplayCursor | undefined;
  const client = new HarnessClient({
    async connect(resume) {
      connected = resume.get("conversation:conversation-1");
      controller.abort();
      return { async send() {}, close() {}, async *[Symbol.asyncIterator]() {} };
    },
  }, new MemoryOutbox(), cursorStore);
  let resets = 0;
  await expect(client.rebase(authority, { generation: "failed", revision: 1n }, () => { resets += 1; }))
    .rejects.toThrow("persistence failed");
  expect(resets).toBe(0);

  rejectWrite = false;
  const rebase = client.rebase(authority, { generation: "durable", revision: 2n }, () => { resets += 1; });
  await started;
  const run = client.run(controller.signal);
  await Promise.resolve();
  expect(connected).toBeUndefined();
  release?.();
  await rebase;
  await run;
  expect(resets).toBe(1);
  expect(connected).toEqual({ generation: "durable", revision: 2n });
});

test("rebase rolls durable state back when projection reset fails", async () => {
  const state = new IndexedDbClientStore({ indexedDB: new IDBFactory(), databaseName: "rebase-rollback" });
  await state.putCursor(authority, { generation: "old", revision: 4n });
  const client = new HarnessClient({ connect: async () => { throw new Error("unused"); } }, state);
  await expect(client.rebase(authority, { generation: "new", revision: 0n }, () => {
    throw new Error("projection reset failed");
  })).rejects.toThrow("projection reset failed");
  expect((await state.loadCursors()).get("conversation:conversation-1")).toEqual({
    generation: "old",
    revision: 4n,
  });
});

test("rebase fences a delivery buffered by the previous replay connection", async () => {
  const controller = new AbortController();
  let releaseDelivery: (() => void) | undefined;
  let connected: (() => void) | undefined;
  let closeStarted: (() => void) | undefined;
  let iteratorFinished: (() => void) | undefined;
  const connectionStarted = new Promise<void>(resolve => { connected = resolve; });
  let iterationStarted: (() => void) | undefined;
  const iterating = new Promise<void>(resolve => { iterationStarted = resolve; });
  const closing = new Promise<void>(resolve => { closeStarted = resolve; });
  const finished = new Promise<void>(resolve => { iteratorFinished = resolve; });
  const deliveryReady = new Promise<void>(resolve => { releaseDelivery = resolve; });
  const state = new IndexedDbClientStore({ indexedDB: new IDBFactory(), databaseName: "rebase-fence" });
  const client = new HarnessClient<string>({
    async connect() {
      connected?.();
      return {
        async send() {},
        async close() {
          closeStarted?.();
          await finished;
        },
        async *[Symbol.asyncIterator]() {
          iterationStarted?.();
          await deliveryReady;
          try {
            yield {
              authority,
              generation: "old",
              fromRevision: 0n,
              throughRevision: 1n,
              live: true,
              events: [{ authority, revision: 1n, operationId, event: "stale" }],
            };
          } finally {
            iteratorFinished?.();
            controller.abort();
          }
        },
      };
    },
  }, state);
  const received: string[] = [];
  client.subscribe(event => received.push(event.event));
  const running = client.run(controller.signal);
  await connectionStarted;
  await iterating;
  const rebasing = client.rebase(authority, { generation: "new", revision: 0n }, () => {});
  await closing;
  releaseDelivery?.();
  await rebasing;
  await running;
  expect(received).toEqual([]);
  expect((await state.loadCursors()).get("conversation:conversation-1")).toEqual({
    generation: "new",
    revision: 0n,
  });
});

test("client hydrates a durable cursor before its first reconnect", async () => {
  const state = new IndexedDbClientStore({ indexedDB: new IDBFactory(), databaseName: "cursor-hydration" });
  await state.putCursor(authority, { generation: "persisted", revision: 9n });
  const controller = new AbortController();
  let observed: ReplayCursor | undefined;
  const transport: Transport = {
    async connect(cursors) {
      observed = cursors.get("conversation:conversation-1");
      controller.abort();
      return { async send() {}, close() {}, async *[Symbol.asyncIterator]() {} };
    },
  };
  await new HarnessClient(transport, state).run(controller.signal);
  expect(observed).toEqual({ generation: "persisted", revision: 9n });
});

test("reconnect delivery is contiguous and clears authoritative outbox entries", async () => {
  const controller = new AbortController();
  const delivery: Delivery<string> = {
    authority,
    generation: "one",
    fromRevision: 0n,
    throughRevision: 1n,
    live: true,
    events: [{ authority, revision: 1n, operationId, event: "committed" }],
  };
  const connection: Connection<string> = {
    async send() {},
    close() {},
    async *[Symbol.asyncIterator]() {
      yield delivery;
      controller.abort();
    },
  };
  const transport: Transport<string> = { connect: async () => connection };
  const outbox = new MemoryOutbox();
  await outbox.put({ operationId, authority, kind: "message.append", payload: {}, offlineSafe: true });
  const client = new HarnessClient(transport, outbox);
  const received: string[] = [];
  client.subscribe(event => received.push(event.event));
  await client.run(controller.signal);
  await Promise.resolve();
  expect(received).toEqual(["committed"]);
  expect(await outbox.load()).toEqual([]);
});

test("malformed delivery has no visible or outbox side effects", async () => {
  const controller = new AbortController();
  const connection: Connection<string> = {
    async send() {},
    close() {},
    async *[Symbol.asyncIterator]() {
      try {
        yield {
          authority,
          generation: "one",
          fromRevision: 0n,
          throughRevision: 2n,
          live: false,
          events: [{ authority, revision: 1n, operationId, event: "invalid" }],
        };
      } finally {
        controller.abort();
      }
    },
  };
  const outbox = new MemoryOutbox();
  await outbox.put({ operationId, authority, kind: "message.append", payload: {}, offlineSafe: true });
  const client = new HarnessClient<string>({ connect: async () => connection }, outbox);
  const received: string[] = [];
  client.subscribe(event => received.push(event.event));
  await client.run(controller.signal);
  expect(received).toEqual([]);
  expect(await outbox.load()).toHaveLength(1);
});

test("a replay generation cannot change without an explicit rebase", async () => {
  const controller = new AbortController();
  const deliveries: Delivery<string>[] = [
    {
      authority,
      generation: "one",
      fromRevision: 0n,
      throughRevision: 1n,
      live: false,
      events: [{ authority, revision: 1n, operationId, event: "first" }],
    },
    {
      authority,
      generation: "two",
      fromRevision: 1n,
      throughRevision: 2n,
      live: false,
      events: [{ authority, revision: 2n, operationId, event: "stale" }],
    },
  ];
  const connection: Connection<string> = {
    async send() {},
    close() {},
    async *[Symbol.asyncIterator]() {
      try {
        for (const delivery of deliveries) yield delivery;
      } finally {
        controller.abort();
      }
    },
  };
  const client = new HarnessClient<string>({ connect: async () => connection });
  const received: string[] = [];
  client.subscribe(event => received.push(event.event));
  await client.run(controller.signal);
  expect(received).toEqual(["first"]);
});

test("listener failure does not advance the cursor or acknowledge the outbox", async () => {
  const controller = new AbortController();
  const connection: Connection<string> = {
    async send() {},
    close() {},
    async *[Symbol.asyncIterator]() {
      try {
        yield {
          authority,
          generation: "one",
          fromRevision: 0n,
          throughRevision: 1n,
          live: true,
          events: [{ authority, revision: 1n, operationId, event: "committed" }],
        };
      } finally {
        controller.abort();
      }
    },
  };
  const outbox = new MemoryOutbox();
  await outbox.put({ operationId, authority, kind: "message.append", payload: {}, offlineSafe: true });
  const client = new HarnessClient<string>({ connect: async () => connection }, outbox);
  client.subscribe(() => { throw new Error("projection failed"); });
  await client.run(controller.signal);
  expect(await outbox.load()).toHaveLength(1);
});

test("a later listener failure does not replay earlier committed batch events", async () => {
  const secondOperation = "02020202-0202-0202-0202-020202020202" as OperationId;
  const controller = new AbortController();
  let observedResume = -1n;
  let connections = 0;
  const transport: Transport<string> = {
    async connect(cursors) {
      connections += 1;
      if (connections > 1) {
        observedResume = cursors.get("conversation:conversation-1")?.revision ?? 0n;
        controller.abort();
        return { async send() {}, close() {}, async *[Symbol.asyncIterator]() {} };
      }
      return {
        async send() {},
        close() {},
        async *[Symbol.asyncIterator]() {
          yield {
            authority,
            generation: "one",
            fromRevision: 0n,
            throughRevision: 2n,
            live: true,
            events: [
              { authority, revision: 1n, operationId, event: "first" },
              { authority, revision: 2n, operationId: secondOperation, event: "second" },
            ],
          };
        },
      };
    },
  };
  const received: string[] = [];
  const client = new HarnessClient(transport);
  client.subscribe(event => {
    received.push(event.event);
    if (event.event === "second") throw new Error("projection failed");
  });
  await client.run(controller.signal);
  expect(received).toEqual(["first", "second"]);
  expect(observedResume).toBe(1n);
});

test("terminal admission removes a safe command from the retry outbox", async () => {
  const controller = new AbortController();
  const connection: Connection = {
    async send() {
      controller.abort();
      throw new TerminalAdmissionError("rejected");
    },
    close() {},
    async *[Symbol.asyncIterator]() {},
  };
  const outbox = new MemoryOutbox();
  await outbox.put({ operationId, authority, kind: "message.append", payload: {}, offlineSafe: true });
  const client = new HarnessClient({ connect: async () => connection }, outbox);
  await client.run(controller.signal);
  expect(await outbox.load()).toEqual([]);
});

test("hydration cache obeys entry and byte bounds", () => {
  const cache = new HydrationCache<string, string>(2, 5);
  cache.set("a", "aa", 2);
  cache.set("b", "bb", 2);
  cache.get("a");
  cache.set("c", "ccc", 3);
  expect(cache.get("b")).toBeUndefined();
  expect(cache.bytes).toBe(5);
  expect(cache.size).toBe(2);
});

test("page window coalesces same-direction loads and deduplicates items", async () => {
  let calls = 0;
  const window = new PageWindow(
    (item: { id: string }) => item.id,
    async () => {
      calls += 1;
      await Promise.resolve();
      return {
        generation: "one",
        items: [{ id: "a" }, { id: "a" }, { id: "b" }],
        hasMoreBefore: false,
        hasMoreAfter: false,
      };
    },
    10,
  );
  await Promise.all([window.load("before"), window.load("before")]);
  expect(calls).toBe(1);
  expect(window.items.map(item => item.id)).toEqual(["a", "b"]);
});

test("page reset fences an older in-flight response", async () => {
  let release: ((page: {
    generation: string;
    items: readonly { id: string }[];
    hasMoreBefore: boolean;
    hasMoreAfter: boolean;
  }) => void) | undefined;
  const window = new PageWindow(
    (item: { id: string }) => item.id,
    () => new Promise(resolve => { release = resolve; }),
    10,
  );
  const stale = window.load("before");
  window.reset({
    generation: "two",
    items: [{ id: "fresh" }],
    hasMoreBefore: false,
    hasMoreAfter: false,
  });
  release?.({
    generation: "one",
    items: [{ id: "stale" }],
    hasMoreBefore: false,
    hasMoreAfter: false,
  });
  expect(await stale).toEqual([]);
  expect(window.items.map(item => item.id)).toEqual(["fresh"]);
});
