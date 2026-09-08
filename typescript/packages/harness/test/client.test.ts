import { expect, test } from "bun:test";
import {
  HarnessClient,
  HydrationCache,
  MemoryOutbox,
  PageWindow,
  TerminalAdmissionError,
  type ClientCommand,
  type Connection,
  type Delivery,
  type OperationId,
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
