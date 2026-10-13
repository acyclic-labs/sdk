import { expect, test } from "bun:test";
import { IDBFactory } from "fake-indexeddb";
import "fake-indexeddb/auto";
import {
  HarnessClient,
  HydrationCache,
  IndexedDbClientStore,
  MemoryOutbox,
  MemoryCursorStore,
  ProjectionStore,
  ClientCheckpointConflict,
  PageWindow,
  TerminalAdmissionError,
  type ClientCommand,
  type Connection,
  type CursorStore,
  type Delivery,
  type FileRef,
  type OfflinePayload,
  type OperationId,
  type Page,
  type ReplayCursor,
  type Transport,
} from "../src/index.js";

const operationId = "01010101-0101-0101-0101-010101010101" as OperationId;
const authority = { kind: "conversation" as const, id: "conversation-1" };
const content: FileRef = {
  volume: { provider: { namespace: "test", family: "filesystem", version: "2" },
    id: "project", class: "project", owner: { kind: "project", id: "project" } },
  path: "messages/committed.txt", version: "generation-1",
  descriptor: { sha256: Array(32).fill(0), byte_length: 0, media_type: "text/plain" },
  display_name: "committed.txt",
};

function replayConnection(controller: AbortController, deliveries: readonly Delivery<string>[]): Connection<string> {
  return {
    async send() {},
    close() {},
    async *[Symbol.asyncIterator]() {
      try { yield* deliveries; }
      finally { controller.abort(); }
    },
  };
}

test("client admits finite caller work before retaining it and snapshots queued commands", async () => {
  let release!: () => void;
  const blocked = new Promise<void>(resolve => { release = resolve; });
  const persisted: ClientCommand[] = [];
  const outbox = {
    load: async () => persisted,
    delete: async () => {},
    put: async (command: ClientCommand) => { await blocked; persisted.push(command); },
  };
  const client = new HarnessClient({ connect: async () => { throw new Error("offline"); } }, outbox,
    new MemoryCursorStore(), { requests: 2, bytes: 8192, commandBytes: 4096 });
  const mutableAuthority = { ...authority };
  const command: ClientCommand = { operationId, authority: mutableAuthority, kind: "message.append", payload: {}, offlineSafe: true };
  const first = client.submit(command);
  const second = client.submit(command);
  await expect(client.submit(command)).rejects.toThrow();
  let rejectedCopies = 0;
  const rejectedBuffer = new ArrayBuffer(16);
  Object.defineProperty(rejectedBuffer, "slice", { value: () => {
    rejectedCopies++; return new ArrayBuffer(16);
  } });
  await expect(client.submit({ operationId, authority, kind: "online", payload: rejectedBuffer }))
    .rejects.toThrow();
  expect(rejectedCopies).toBe(0);
  mutableAuthority.id = "mutated-after-admission";
  release();
  await Promise.all([first, second]);
  expect(persisted.map(value => value.authority.id)).toEqual(["conversation-1", "conversation-1"]);
  mutableAuthority.id = "conversation-1";
  await expect(client.submit({ ...command, payload: { metadata: { oversized: new Array(1_000_000) } } } as unknown as ClientCommand))
    .rejects.toThrow();
  expect(persisted).toHaveLength(2);
});

test("durable projection restores the committed cut before reconnect and refuses identity drift", async () => {
  const indexedDB = new IDBFactory();
  let opened = 0;
  const originalOpen = indexedDB.open.bind(indexedDB);
  indexedDB.open = (...args) => { opened++; return originalOpen(...args); };
  const options = { indexedDB, databaseName: "projection-recovery" };
  const store = new IndexedDbClientStore(options);
  expect(opened).toBe(0);
  await store.put({ operationId, authority, kind: "message.append", payload: {}, offlineSafe: true });
  const recovery = {
    authority, identity: "tenant/reducer/schema/adapter-1", encode: (value: number) => value,
    decode: (value: unknown, cursor: ReplayCursor) => {
      if (cursor.generation !== "one" || typeof value !== "number" || value !== Number(cursor.revision)) throw new Error("invalid cut");
      return value;
    },
  };
  const controller = new AbortController();
  const connection: Connection<number> = {
    send: async () => {}, close: () => {},
    async *[Symbol.asyncIterator]() {
      yield { authority, generation: "one", fromRevision: 0n, throughRevision: 1n, live: true,
        events: [{ authority, operationId, revision: 1n, event: 1 }] };
      controller.abort();
    },
  };
  const first = new HarnessClient<number>({ connect: async () => connection }, store);
  const projection = new ProjectionStore(first, 0, (state, event) => state + event.event, recovery);
  projection.start();
  await first.run(controller.signal);
  expect(projection.getSnapshot()).toBe(1);
  expect(await store.load()).toEqual([]);
  const reopened = new IndexedDbClientStore(options);
  const stop = new AbortController();
  let restored!: ProjectionStore<number, number>;
  const second = new HarnessClient<number>({ connect: async cursors => {
    expect(restored.getSnapshot()).toBe(1);
    expect(cursors.get("conversation:conversation-1")?.revision).toBe(1n);
    stop.abort(); return { send: async () => {}, close: () => {}, async *[Symbol.asyncIterator]() {} };
  } }, reopened);
  restored = new ProjectionStore(second, 0, (state, event) => state + event.event, recovery);
  restored.start();
  await second.run(stop.signal);
  await expect(store.commit(authority, { generation: "one", revision: 2n }, undefined,
    { identity: recovery.identity, state: 2 }, null)).rejects.toBeInstanceOf(ClientCheckpointConflict);
  expect((await store.loadCursors()).get("conversation:conversation-1")?.revision).toBe(1n);
  await expect(store.putCursor(authority, { generation: "one", revision: 2n }))
    .rejects.toBeInstanceOf(ClientCheckpointConflict);
  let connections = 0;
  const third = new HarnessClient<number>({ connect: async () => { connections++; return connection; } }, reopened);
  const repaired = new ProjectionStore(third, 0, (state, event) => state + event.event, { ...recovery, identity: "changed-adapter" });
  repaired.start();
  await expect(third.run()).rejects.toThrow();
  expect(connections).toBe(0);
  await third.rebase(authority, { generation: "one", revision: 2n },
    { identity: "changed-adapter", state: 2 });
  expect((await reopened.loadCursors()).get("conversation:conversation-1")?.revision).toBe(2n);
  expect(repaired.getSnapshot()).toBe(2);
  repaired.dispose();
});

test("checkpoint failure preserves projection, cursor and original retry identity", async () => {
  const indexedDB = new IDBFactory();
  const options = { indexedDB, databaseName: "projection-failure", maximumCheckpointBytes: 128 };
  const store = new IndexedDbClientStore(options);
  const command: ClientCommand = { operationId, authority, kind: "message.append", payload: {}, offlineSafe: true };
  await store.put(command);
  const stop = new AbortController();
  const delivery: Delivery<number> = { authority, generation: "one", fromRevision: 0n, throughRevision: 1n,
    live: true, events: [{ authority, revision: 1n, operationId, event: 1 }] };
  const client = new HarnessClient<number>({ connect: async () => ({ send: async () => {}, close: () => {},
    async *[Symbol.asyncIterator]() { yield delivery; } }) }, store);
  const projection = new ProjectionStore(client, 0, (value, event) => value + event.event, {
    authority, identity: "adapter", encode: value => ({ value, tooLarge: "x".repeat(1000) }),
    decode: value => (value as { value: number }).value,
  });
  projection.start();
  client.failure.subscribe(() => stop.abort());
  await client.run(stop.signal);
  expect(client.failure.getSnapshot()).toBeInstanceOf(RangeError);
  expect(projection.getSnapshot()).toBe(0);
  const reopened = new IndexedDbClientStore({ ...options, maximumCheckpointBytes: 4096, maximumCursors: 1 });
  expect((await reopened.loadCursors()).size).toBe(0);
  expect((await reopened.load())[0]?.operationId).toBe(operationId);
  await reopened.putCursor(authority, { generation: "one", revision: 0n });
  await expect(reopened.commit({ ...authority, id: "another" }, { generation: "one", revision: 1n }, operationId))
    .rejects.toThrow();
  expect((await reopened.load())[0]?.operationId).toBe(operationId);
  expect((await reopened.loadCursors()).get("conversation:conversation-1")?.revision).toBe(0n);
  const broken = new HarnessClient<number>({ connect: async () => { throw new Error("must not connect"); } }, reopened);
  new ProjectionStore(broken, 0, (value, event) => value + event.event, {
    authority, identity: "adapter", encode: value => value, decode: value => value as number,
  }).start();
  await expect(broken.run()).rejects.toThrow();
});

test("disposal during a durable batch commits only the prepared event", async () => {
  let started!: () => void, release!: () => void, settled!: () => void;
  const entered = new Promise<void>(resolve => { started = resolve; });
  const blocked = new Promise<void>(resolve => { release = resolve; });
  const completed = new Promise<void>(resolve => { settled = resolve; });
  class PausedStore extends IndexedDbClientStore {
    override async commit(...args: Parameters<IndexedDbClientStore["commit"]>): Promise<bigint> {
      started(); await blocked;
      try { return await super.commit(...args); } finally { settled(); }
    }
  }
  const store = new PausedStore({ indexedDB: new IDBFactory(), databaseName: "dispose-durable-batch" });
  const secondOperation = "02020202-0202-0202-0202-020202020202" as OperationId;
  for (const id of [operationId, secondOperation]) await store.put({ operationId: id, authority, kind: "message.append", payload: {}, offlineSafe: true });
  const client = new HarnessClient<number>({ connect: async () => ({ send: async () => {}, close() {},
    async *[Symbol.asyncIterator]() {
      yield { authority, generation: "one", fromRevision: 0n, throughRevision: 2n, live: true,
        events: [operationId, secondOperation].map((id, index) => ({ authority, operationId: id, revision: BigInt(index + 1), event: 1 })) };
    } }) }, store);
  const projection = new ProjectionStore(client, 0, (value, event) => value + event.event, {
    authority, identity: "dispose-cut", encode: value => value, decode: value => value as number,
  });
  projection.start();
  const running = client.run();
  await entered; client.dispose(); release();
  await running; await completed;
  const remaining = await store.load();
  const saved = (await store.loadRecovery())[0]!;
  expect(saved.cursor.revision).toBe(1n);
  expect(saved.checkpoint?.state).toBe(1);
  expect(projection.getSnapshot()).toBe(0);
  expect(remaining.map(command => command.operationId)).toEqual([secondOperation]);
  projection.dispose();
});

test("durable legacy listeners remain retryable without reducing twice", async () => {
  const store = new IndexedDbClientStore({ indexedDB: new IDBFactory(), databaseName: "listener-retry" });
  const stop = new AbortController();
  let connections = 0, firstCalls = 0, laterCalls = 0;
  const client = new HarnessClient<{ increment: number }>({ connect: async cursors => {
    connections++;
    if (connections > 2) {
      stop.abort(); return { send: async () => {}, close() {}, async *[Symbol.asyncIterator]() {} };
    }
    expect(cursors.get("conversation:conversation-1")).toBeUndefined();
    return { send: async () => {}, close() {},
      async *[Symbol.asyncIterator]() {
        yield { authority, generation: "one", fromRevision: 0n, throughRevision: 1n, live: true,
          events: [{ authority, operationId, revision: 1n, event: { increment: 1 } }] };
        stop.abort();
      } };
  } }, store);
  const projection = new ProjectionStore(client, 0, (value, event) => value + event.event.increment, {
    authority, identity: "listener-cut", encode: value => value, decode: value => value as number,
  });
  projection.start();
  client.subscribe(() => { firstCalls++; if (firstCalls === 1) throw new Error("retry listener"); });
  client.subscribe(() => { laterCalls++; });
  await client.run(stop.signal);
  expect(connections).toBe(2);
  expect(firstCalls).toBe(2);
  expect(laterCalls).toBe(1);
  expect(projection.getSnapshot()).toBe(1);
  expect((await store.loadRecovery())[0]?.checkpoint?.state).toBe(1);
});

test("online undefined fields survive bounded copying while outboxes stay strict", async () => {
  const stop = new AbortController();
  let connections = 0, deliveries = 0, sends = 0;
  let submitted: Promise<void> | undefined;
  const client = new HarnessClient<{ optional?: undefined }>({ connect: async () => {
    if (++connections > 2) stop.abort();
    return { send: async command => { sends++; expect(command.payload).toEqual({ optional: undefined }); }, close() {},
      async *[Symbol.asyncIterator]() {
        yield { authority, generation: "one", fromRevision: 0n, throughRevision: 1n, live: true,
          events: [{ authority, operationId, revision: 1n, event: { optional: undefined } }] };
        await submitted; stop.abort();
      } };
  } });
  client.subscribe(event => {
    expect(Object.hasOwn(event.event, "optional")).toBe(true);
    deliveries++;
    submitted = client.submit({ operationId, authority, kind: "online", payload: { optional: undefined } });
  });
  await client.run(stop.signal);
  expect(deliveries).toBe(1); expect(sends).toBe(1);
  const store = new MemoryOutbox();
  await expect(store.put({ operationId, authority, kind: "offline", payload: { metadata: { optional: undefined } }, offlineSafe: true } as unknown as ClientCommand))
    .rejects.toThrow();
});


test("offline outbox accepts only explicitly safe non-approval commands", async () => {
  const outbox = new MemoryOutbox();
  const transport: Transport = { connect: async () => { throw new Error("offline"); } };
  const client = new HarnessClient(transport, outbox);
  const safe: ClientCommand = {
    operationId,
    authority,
    kind: "message.append",
    payload: { content },
    offlineSafe: true,
  };
  await client.submit(safe);
  expect(await outbox.load()).toEqual([safe]);
  await expect(
    client.submit({ ...safe, kind: "interaction.resolve.approval" }),
  ).rejects.toThrow();
});

test("IndexedDB atomically persists outbox acknowledgements and replay cursors across restart", async () => {
  const indexedDB = new IDBFactory();
  const options = { indexedDB, databaseName: "durable-client", maximumCommands: 2, maximumBytes: 4_096 };
  const first = new IndexedDbClientStore(options);
  const command: ClientCommand = {
    operationId,
    authority,
    kind: "message.append",
    payload: { content },
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
  })).rejects.toThrow();
  const customArray: unknown[] & { extra?: string } = [];
  customArray.extra = "x".repeat(10_000);
  await expect(store.put({ operationId, authority, kind: "message.append", payload: customArray as unknown as OfflinePayload, offlineSafe: true }))
    .rejects.toThrow();
  const accessor = {} as { value: string };
  Object.defineProperty(accessor, "value", { enumerable: true, get: () => "x".repeat(10_000) });
  await expect(store.put({ operationId, authority, kind: "message.append", payload: accessor as unknown as OfflinePayload, offlineSafe: true }))
    .rejects.toThrow();
});

test("IndexedDB rejects non-canonical structured-clone payloads", async () => {
  const store = new IndexedDbClientStore({ indexedDB: new IDBFactory(), databaseName: "canonical-client", maximumBytes: 128 });
  for (const payload of [new Map([["large", "x".repeat(1_024)]]), new Set(["x".repeat(1_024)]),
    Number.POSITIVE_INFINITY, { toJSON: () => ({}) }, new ArrayBuffer(1_024)]) {
    await expect(store.put({ operationId, authority, kind: "message.append", payload: payload as unknown as OfflinePayload,
      offlineSafe: true })).rejects.toThrow();
  }
});

test("offline outboxes never retain bearer scopes, inline bodies, or mutable caller objects", async () => {
  const outbox = new MemoryOutbox();
  const safe: ClientCommand = { operationId, authority, kind: "message.append",
    payload: { content, metadata: { sequence: 1 } }, offlineSafe: true };
  await outbox.put(safe);
  (safe.payload.metadata as { sequence: number }).sequence = 2;
  expect((await outbox.load())[0]?.payload).toEqual({ content, metadata: { sequence: 1 } });
  await expect(outbox.put({ ...safe, payload: { scope: { id: "signed", proof: [1] } } as unknown as OfflinePayload }))
    .rejects.toThrow();
  await expect(outbox.put({ ...safe, payload: { text: "uncommitted body" } as unknown as OfflinePayload }))
    .rejects.toThrow();
  await expect(outbox.put({ ...safe, payload: { content: new Uint8Array([1]) } as unknown as OfflinePayload }))
    .rejects.toThrow();
  await expect(outbox.put({ ...safe, payload: { metadata: { sequence: Number.MAX_SAFE_INTEGER + 1 } } }))
    .rejects.toThrow();
  const credentialField = { ...safe, authorization: "Bearer secret" };
  await expect(outbox.put(credentialField)).rejects.toThrow();
  const bearerAuthority = { ...safe, authority: { ...authority, proof: [1] } };
  await expect(outbox.put(bearerAuthority)).rejects.toThrow();
});

test("IndexedDB preserves enqueue order across restart and isolates database namespaces", async () => {
  const indexedDB = new IDBFactory();
  const first = new IndexedDbClientStore({ indexedDB, databaseName: "ordered-client" });
  const laterKey: ClientCommand = {
    operationId: "ffffffff-ffff-ffff-ffff-ffffffffffff" as OperationId,
    authority,
    kind: "first",
    payload: {},
    offlineSafe: true,
  };
  const earlierKey: ClientCommand = {
    operationId: "00000000-0000-0000-0000-000000000000" as OperationId,
    authority,
    kind: "second",
    payload: {},
    offlineSafe: true,
  };
  await first.put(laterKey);
  await first.put(earlierKey);
  expect(await new IndexedDbClientStore({ indexedDB, databaseName: "ordered-client" }).load()).toEqual([
    laterKey,
    earlierKey,
  ]);
  expect(await new IndexedDbClientStore({ indexedDB, databaseName: "other-client" }).load()).toEqual([]);
});

test("IndexedDB pages a fixed enqueue cut", async () => {
  const indexedDB = new IDBFactory();
  const state = new IndexedDbClientStore({ indexedDB, databaseName: "paged-outbox", maximumCommands: 80 });
  const ids = Array.from({ length: 70 }, (_, i) =>
    `01010101-0101-0101-0101-${String(100 - i).padStart(12, "0")}` as OperationId);
  for (const id of ids) await state.put({ operationId: id, authority, kind: "message.append", payload: {}, offlineSafe: true });
  const first = await state.loadPage();
  expect(first.commands.map(command => command.operationId)).toEqual(ids.slice(0, 64));
  await state.put({ operationId, authority, kind: "message.append", payload: {}, offlineSafe: true });
  const tail = await state.loadPage(first.afterSequence, first.throughSequence);
  expect(tail.commands.map(command => command.operationId)).toEqual(ids.slice(64));
  expect(tail.afterSequence).toBe(tail.throughSequence);
  expect((await state.load()).map(command => command.operationId)).toEqual([...ids, operationId]);
  for (const id of [...ids.slice(64), operationId]) await state.delete(id);
  await state.put({ operationId, authority, kind: "message.append", payload: {}, offlineSafe: true });
  expect((await state.loadPage(first.afterSequence, first.throughSequence)).commands).toEqual([]);
  for (const command of await state.load()) await state.delete(command.operationId);
  const reopened = new IndexedDbClientStore({ indexedDB, databaseName: "paged-outbox" });
  await reopened.put({ operationId, authority, kind: "message.append", payload: {}, offlineSafe: true });
  const newCut = await reopened.loadPage();
  expect(newCut.throughSequence).toBeGreaterThan(first.throughSequence);

});

test("a blocked-open rejection closes an eventual successful database handle", async () => {
  const indexedDB = new IDBFactory();
  const open = indexedDB.open.bind(indexedDB);
  const lateSuccess = Promise.withResolvers<void>();
  indexedDB.open = (...args) => {
    const opening = open(...args);
    opening.addEventListener("success", () => lateSuccess.resolve(), { once: true });
    queueMicrotask(() => opening.onblocked?.(new Event("blocked") as IDBVersionChangeEvent));
    return opening;
  };
  const store = new IndexedDbClientStore({ indexedDB, databaseName: "late-open" });
  await expect(store.load()).rejects.toThrow();
  await lateSuccess.promise;
  const deleted = Promise.withResolvers<void>();
  const deleting = indexedDB.deleteDatabase("late-open");
  deleting.onsuccess = () => deleted.resolve();
  deleting.onblocked = () => deleted.reject(new Error("late database handle stayed open"));
  deleting.onerror = () => deleted.reject(deleting.error);
  await deleted.promise;
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
    .rejects.toThrow();
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

test("durable rebase preserves the previous checkpoint when authoritative decoding fails", async () => {
  const state = new IndexedDbClientStore({ indexedDB: new IDBFactory(), databaseName: "rebase-rollback" });
  await state.commit(authority, { generation: "old", revision: 4n }, undefined,
    { identity: "rollback-reducer", state: 4 }, null, null);
  const client = new HarnessClient<number>({ connect: async () => { throw new Error("unused"); } }, state);
  const projection = new ProjectionStore(client, 0, (value, event) => value + event.event, {
    authority, identity: "rollback-reducer", encode: value => value,
    decode: (value, cursor) => {
      if (cursor.generation !== "old" || value !== 4) throw new Error("projection reset failed");
      return 4;
    },
  });
  projection.start();
  await expect(client.rebase(authority, { generation: "new", revision: 0n },
    { identity: "rollback-reducer", state: 0 })).rejects.toThrow();
  const saved = (await state.loadRecovery())[0]!;
  expect(saved.cursor).toEqual({ generation: "old", revision: 4n });
  expect(saved.checkpoint?.state).toBe(4);
  expect(projection.getSnapshot()).toBe(0);
  projection.dispose(); client.dispose();
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
  const projection = new ProjectionStore(client, "", (value, event) => value + event.event, {
    authority, identity: "fence-reducer", encode: value => value,
    decode: value => { if (typeof value !== "string") throw new Error("invalid fence checkpoint"); return value; },
  });
  projection.start();
  const received: string[] = [];
  client.subscribe(event => received.push(event.event));
  const running = client.run(controller.signal);
  await connectionStarted;
  await iterating;
  const rebasing = client.rebase(authority, { generation: "new", revision: 0n },
    { identity: "fence-reducer", state: "" });
  await closing;
  releaseDelivery?.();
  await rebasing;
  await running;
  expect(received).toEqual([]);
  expect((await state.loadCursors()).get("conversation:conversation-1")).toEqual({
    generation: "new",
    revision: 0n,
  });
  projection.dispose(); client.dispose();
});


test("cursor-only durable records never resume an empty projection", async () => {
  const state = new IndexedDbClientStore({ indexedDB: new IDBFactory(), databaseName: "cursor-only-rejection" });
  await state.putCursor(authority, { generation: "persisted", revision: 9n });
  const stop = new AbortController();
  let connections = 0;
  const client = new HarnessClient({ connect: async () => {
    connections++; stop.abort(); return { send: async () => {}, close() {}, async *[Symbol.asyncIterator]() {} };
  } }, state);
  await expect(client.run(stop.signal)).rejects.toThrow();
  expect(connections).toBe(0);
  expect((await state.loadCursors()).get("conversation:conversation-1")?.revision).toBe(9n);
  client.dispose();
});

test("durable delivery without a registered recovery projection preserves the original retry", async () => {
  const state = new IndexedDbClientStore({ indexedDB: new IDBFactory(), databaseName: "missing-projection" });
  await state.put({ operationId, authority, kind: "message.append", payload: {}, offlineSafe: true });
  const stop = new AbortController();
  const client = new HarnessClient<number>({ connect: async () => ({
    send: async () => {}, close() {}, async *[Symbol.asyncIterator]() {
      yield { authority, generation: "one", fromRevision: 0n, throughRevision: 1n, live: true,
        events: [{ authority, revision: 1n, operationId, event: 1 }] };
      stop.abort();
    },
  }) }, state);
  let deliveries = 0;
  client.subscribe(() => { deliveries++; });
  await expect(client.run(stop.signal)).rejects.toThrow();
  expect(deliveries).toBe(0);
  expect((await state.loadCursors()).size).toBe(0);
  expect((await state.load())[0]?.operationId).toBe(operationId);
  client.dispose();
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
  const connection = replayConnection(controller, [delivery]);  const transport: Transport<string> = { connect: async () => connection };
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
  const connection = replayConnection(controller, [{
    authority, generation: "one", fromRevision: 0n, throughRevision: 2n, live: false,
    events: [{ authority, revision: 1n, operationId, event: "invalid" }],
  }]);  const outbox = new MemoryOutbox();
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
  const connection = replayConnection(controller, deliveries);  const client = new HarnessClient<string>({ connect: async () => connection });
  const received: string[] = [];
  client.subscribe(event => received.push(event.event));
  await client.run(controller.signal);
  expect(received).toEqual(["first"]);
});

test("listener failure does not advance the cursor or acknowledge the outbox", async () => {
  const controller = new AbortController();
  const connection = replayConnection(controller, [{
    authority, generation: "one", fromRevision: 0n, throughRevision: 1n, live: true,
    events: [{ authority, revision: 1n, operationId, event: "committed" }],
  }]);  const outbox = new MemoryOutbox();
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
  let sends = 0;
  const connection: Connection = {
    async send() {
      sends++;
      controller.abort();
      throw new TerminalAdmissionError("rejected");
    },
    close() {},
    async *[Symbol.asyncIterator]() {},
  };
  const outbox = new MemoryOutbox();
  await outbox.put({ operationId, authority, kind: "message.append", payload: {}, offlineSafe: true });
  const unsent = "02020202-0202-0202-0202-020202020202" as OperationId;
  await outbox.put({ operationId: unsent, authority, kind: "message.append", payload: {}, offlineSafe: true });
  const client = new HarnessClient({ connect: async () => connection }, outbox);
  await client.run(controller.signal);
  expect((await outbox.load()).map(command => command.operationId)).toEqual([unsent]);
  expect(sends).toBe(1);
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
  let release: ((page: Page<{ id: string }, unknown>) => void) | undefined;
  const window = new PageWindow(
    (item: { id: string }) => item.id,
    () => new Promise<Page<{ id: string }, unknown>>(resolve => { release = resolve; }),
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

test("page window evicts the opposite edge at maxItems after loads and oversized reset", async () => {
  type Item = { id: string; value: number };
  const pages: Page<Item, string>[] = [
    { generation: "one", items: [{ id: "c", value: 2 }, { id: "d", value: 1 }], after: "d", hasMoreBefore: true, hasMoreAfter: true },
    { generation: "one", items: [{ id: "e", value: 1 }, { id: "f", value: 1 }, { id: "g", value: 1 }], after: "g", hasMoreBefore: true, hasMoreAfter: true },
    { generation: "one", items: [{ id: "b", value: 2 }, { id: "c", value: 3 }], before: "b", hasMoreBefore: false, hasMoreAfter: true },
  ];
  const requests: [string, string | undefined][] = [];
  const window = new PageWindow<Item, string>(item => item.id, async (direction, cursor) => {
    requests.push([direction, cursor]);
    const page = pages.shift();
    if (page === undefined) throw new Error("unexpected page request");
    return page;
  }, 2);
  window.reset({ generation: "one", items: [{ id: "b", value: 1 }, { id: "c", value: 1 }], before: "b", after: "c", hasMoreBefore: true, hasMoreAfter: true });
  expect(window.items).toEqual([{ id: "b", value: 1 }, { id: "c", value: 1 }]);
  expect(await window.load("after")).toEqual([{ id: "d", value: 1 }]);
  expect(window.items).toEqual([{ id: "c", value: 2 }, { id: "d", value: 1 }]);
  expect(await window.load("after")).toEqual([{ id: "f", value: 1 }, { id: "g", value: 1 }]);
  expect(window.items).toEqual([{ id: "f", value: 1 }, { id: "g", value: 1 }]);
  expect(await window.load("before")).toEqual([{ id: "b", value: 2 }, { id: "c", value: 3 }]);
  expect(window.items).toEqual([{ id: "b", value: 2 }, { id: "c", value: 3 }]);
  expect(requests).toEqual([["after", "c"], ["after", "d"], ["before", "b"]]);
  expect(await window.load("before")).toEqual([]);
  expect(requests).toHaveLength(3);

  window.reset({ generation: "two", items: [{ id: "x", value: 1 }, { id: "y", value: 1 }, { id: "z", value: 1 }, { id: "w", value: 1 }], hasMoreBefore: false, hasMoreAfter: false });
  expect(window.items).toEqual([{ id: "z", value: 1 }, { id: "w", value: 1 }]);
  expect(window.hasMoreBefore).toBe(false);
  expect(window.hasMoreAfter).toBe(false);
});

test("client rebase fences both stale page directions without clearing a fresh in-flight load", async () => {
  type Item = { id: string };
  const requests: { direction: string; cursor: string | undefined; resolve: (page: Page<Item, string>) => void }[] = [];
  const window = new PageWindow<Item, string>(item => item.id, (direction, cursor) =>
    new Promise(resolve => { requests.push({ direction, cursor, resolve }); }), 2);
  window.reset({ generation: "old", items: [{ id: "old" }], before: "old-before", after: "old-after", hasMoreBefore: true, hasMoreAfter: true });
  const staleBefore = window.load("before");
  const staleAfter = window.load("after");
  const client = new HarnessClient({ connect: async () => { throw new Error("unused connection"); } }, new MemoryOutbox());
  await client.rebase(authority, { generation: "new", revision: 0n }, () => {
    window.reset({ generation: "new", items: [{ id: "fresh-a" }, { id: "fresh-b" }, { id: "fresh-c" }], before: "new-before", after: "new-after", hasMoreBefore: true, hasMoreAfter: true });
  });
  expect(window.items).toEqual([{ id: "fresh-b" }, { id: "fresh-c" }]);
  const freshBefore = window.load("before");
  expect(requests.map(({ direction, cursor }) => [direction, cursor])).toEqual([
    ["before", "old-before"], ["after", "old-after"], ["before", "new-before"],
  ]);
  for (const request of requests.slice(0, 2)) {
    request.resolve({ generation: "old", items: [{ id: "stale" }], before: "stale-before", after: "stale-after", hasMoreBefore: false, hasMoreAfter: false });
  }
  expect(await staleBefore).toEqual([]);
  expect(await staleAfter).toEqual([]);
  expect(window.items).toEqual([{ id: "fresh-b" }, { id: "fresh-c" }]);
  expect(window.hasMoreBefore).toBe(true);
  expect(window.hasMoreAfter).toBe(true);
  expect(window.load("before")).toBe(freshBefore);
  expect(requests).toHaveLength(3);
  requests[2]?.resolve({ generation: "new", items: [{ id: "fresh-a" }], before: "new-start", hasMoreBefore: false, hasMoreAfter: true });
  expect(await freshBefore).toEqual([{ id: "fresh-a" }]);
  expect(window.items).toEqual([{ id: "fresh-a" }, { id: "fresh-b" }]);
  expect(window.hasMoreBefore).toBe(false);
  expect(window.hasMoreAfter).toBe(true);
});
