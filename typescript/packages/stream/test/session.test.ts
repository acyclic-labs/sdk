import { expect, test } from "bun:test";
import { create } from "@bufbuild/protobuf";
import { SessionFollowSchema } from "../generated/proto/stream/v1/session_pb.js";
import { StreamSessions } from "../src/session.js";
import { SessionQueue } from "../src/session-queue.js";
import { SessionFilterSchema, StreamLimit } from "../generated/proto/stream/v1/stream_pb.js";
import type { SessionFrame } from "../generated/proto/stream/v1/session_pb.js";
import { SessionNode, checkpoint } from "./session-node.js";

test("session slots, filters and record headers survive the TypeScript bridge", async () => {
  const node = new SessionNode();
  node.append("scoped");
  const record = node.history.get("scoped")![0]!;
  record.keys = [new Uint8Array([4])];
  record.attributes = [{ $typeName: "acyclic.stream.v1.Attribute", name: "owner", value: new Uint8Array([9]) }];
  const client = new StreamSessions(node.connect);
  const credential = new Uint8Array([1, 2, 3]);
  const registration = client.registerCredential(2, credential);
  credential.fill(0);
  await registration;
  const filter = create(SessionFilterSchema, { keys: [{ start: new Uint8Array([4]), end: new Uint8Array([5]) }] });
  const follow = client.follow("scoped", { from: 0n, credential: 2, filter });
  const first = (await follow.next()).value!;
  expect(first.keys).toEqual(record.keys);
  expect(first.attributes).toEqual(record.attributes);
  const hello = node.received.find(frame => frame.body.case === "hello")!;
  if (hello.body.case !== "hello") throw new Error("missing hello");
  expect(hello.body.value.credentials[2]).toEqual(new Uint8Array([1, 2, 3]));
  const interest = node.received.find(frame => frame.body.case === "interest")!;
  if (interest.body.case !== "interest") throw new Error("missing interest");
  expect(interest.body.value.upsert[0]!.credential).toBe(2);
  expect(interest.body.value.upsert[0]!.filter).toEqual(filter);
  await client.registerCredential(2, new Uint8Array([4, 5]));
  await node.until(() => node.received.some(frame => frame.body.case === "credential"));
  const renewal = node.received.find(frame => frame.body.case === "credential")!;
  if (renewal.body.case !== "credential") throw new Error("missing renewal");
  expect(renewal.body.value.slot).toBe(2);
  expect(renewal.body.value.credential).toEqual(new Uint8Array([4, 5]));
  await follow.return(undefined);
  await expect(client.registerCredential(-1, new Uint8Array([1]))).rejects.toThrow(RangeError);
  await expect(client.registerCredential(0xffff_ffff, new Uint8Array([1]))).rejects.toMatchObject({ code: "capacity_exhausted" });
  await expect(client.registerCredential(0, new Uint8Array(StreamLimit.MAX_COMMAND_BYTES + 1))).rejects.toMatchObject({ code: "limit_exceeded" });
  expect(() => client.followData(create(SessionFollowSchema, { resource: "streams:/scoped" }), { from: 0n, credential: 0x1_0000_0000 })).toThrow(RangeError);
});

test("concurrent follows share a node connection and remove every interest", async () => {
  const node = new SessionNode();
  const client = new StreamSessions(node.connect);
  const controller = new AbortController();
  const follows = Array.from({ length: 64 }, (_, id) => client.follow(`agent/${id}`, { from: 0n, signal: controller.signal }));
  const first = follows.map(follow => follow.next());
  await checkpoint();
  expect(node.active).toBe(64);
  for (let id = 0; id < follows.length; id += 1) node.append(`agent/${id}`);
  expect((await Promise.all(first)).every(record => record.value?.sequence === 0n)).toBe(true);
  expect(node.connections.length).toBe(1);
  expect(node.actorChannels).toBe(0);
  controller.abort();
  await Promise.all(follows.map(follow => follow.return(undefined)));
  expect(node.active).toBe(0);
  expect(node.outstandingFrames).toBe(0);
});

test("returning unread buffered deliveries never loses aggregate credit", async () => {
  const node = new SessionNode();
  node.append("unread", 2);
  const client = new StreamSessions(node.connect);
  const anchorCancel = new AbortController();
  const anchor = client.follow("anchor", { from: 0n, signal: anchorCancel.signal });
  const parked = anchor.next();
  await node.until(() => node.active === 1);
  for (let index = 0; index < StreamLimit.MAX_ITEMS + 8; index += 1) {
    const follow = client.follow("unread", { from: 0n });
    expect((await follow.next()).value?.sequence).toBe(0n);
    await follow.return(undefined);
  }
  const healthy = client.follow("healthy", { from: 0n });
  const next = healthy.next();
  await checkpoint();
  node.append("healthy");
  expect((await next).value?.sequence).toBe(0n);
  await healthy.return(undefined);
  await checkpoint();
  expect(node.connections.length).toBe(1);
  expect(node.returned).toBeGreaterThan(StreamLimit.MAX_ITEMS);
  expect(node.returned).toBeLessThanOrEqual(2 * (StreamLimit.MAX_ITEMS + 8) + 1);
  anchorCancel.abort();
  await parked;
  await anchor.return(undefined);
  await node.until(() => node.active === 0);
  expect(node.outstandingFrames).toBe(0);
}, 2_000);

test("moved resumes the yielded cursor while another channel stays live", async () => {
  const node = new SessionNode();
  node.append("moving", 3);
  let release!: () => void;
  const parked = new Promise<void>(resolve => { release = resolve; });
  node.beforeConnect = async name => { if (name === "owner") await parked; };
  const client = new StreamSessions(node.connect);
  const moving = client.follow("moving", { from: 0n });
  expect((await moving.next()).value?.sequence).toBe(0n);
  const healthy = client.follow("healthy", { from: 0n });
  const next = healthy.next();
  await checkpoint();
  node.move("moving");
  await checkpoint();
  node.append("healthy");
  expect((await next).value?.sequence).toBe(0n);
  release();
  await checkpoint();
  expect((await moving.next()).value?.sequence).toBe(1n);
  expect((await moving.next()).value?.sequence).toBe(2n);
  expect(node.resumes.find(resume => resume.node === "owner")?.next).toBe(1n);
  expect(node.connections.length).toBe(2);
  await moving.return(undefined);
  await healthy.return(undefined);
  expect(node.actorChannels).toBe(0);
});

test("transport recovery fences queued records and reuses one live node session", async () => {
  const node = new SessionNode();
  node.append("events", 4);
  const client = new StreamSessions(node.connect);
  const records = client.follow("events", { from: 0n });
  expect((await records.next()).value?.sequence).toBe(0n);
  node.disconnect();
  await checkpoint();
  for (let next = 1n; next < 4n; next += 1n) expect((await records.next()).value?.sequence).toBe(next);
  expect(node.connections.length).toBe(2);
  expect(node.connections.filter(connection => !connection.closed).length).toBe(1);
  expect(node.resumes.map(resume => resume.next)).toEqual([0n, 1n]);
  await records.return(undefined);
});

test("aborting a suspended iterator releases its channel reservation immediately", async () => {
  const node = new SessionNode();
  node.append("events");
  const client = new StreamSessions(node.connect);
  const anchorCancel = new AbortController();
  const anchor = client.follow("anchor", { from: 0n, signal: anchorCancel.signal });
  const parked = anchor.next();
  await node.until(() => node.active === 1);
  for (let index = 0; index < 16_385; index++) {
    const controller = new AbortController();
    const records = client.follow("events", { from: 0n, signal: controller.signal });
    expect((await records.next()).value?.sequence).toBe(0n);
    controller.abort();
    // Deliberately discard the iterator while it is suspended at yield.
  }
  await node.until(() => node.active === 1);
  expect(node.connections.length).toBe(1);
  anchorCancel.abort();
  await parked;
  await anchor.return(undefined);
  await node.until(() => node.active === 0);
  expect(node.outstandingFrames).toBe(0);
});

test("concurrent first follows share a bounded silent-peer handshake", async () => {
  let connections = 0;
  const client = new StreamSessions(async () => {
    connections += 1;
    const incoming = new SessionQueue<SessionFrame>(2);
    return { incoming, async send() {}, close() { incoming.end(); } };
  });
  const first = client.follow("one", { from: 0n });
  const second = client.follow("two", { from: 0n });
  const results = await Promise.allSettled([first.next(), second.next()]);
  expect(results.every(result => result.status === "rejected" && result.reason.code === "unavailable")).toBe(true);
  expect(connections).toBe(3);
}, 4_000);

test("Welcome followed by EOF cannot strand the first subscription", async () => {
  const node = new SessionNode();
  const client = new StreamSessions(async (target, signal) => {
    const transport = await node.connect(target, signal);
    return { ...transport, async send(input) {
      if (input.body.case === "hello") {
        await transport.send(input);
        transport.close();
      }
    } };
  });
  const nativeSetTimeout = globalThis.setTimeout;
  const nativeClearTimeout = globalThis.clearTimeout;
  const handle = {};
  let expire: (() => void) | undefined;
  Object.assign(globalThis, {
    setTimeout(callback: () => void, milliseconds?: number, ...args: unknown[]) {
      if (milliseconds !== 10_000) return nativeSetTimeout(callback, milliseconds, ...args);
      expire = callback;
      return handle;
    },
    clearTimeout(timer: ReturnType<typeof setTimeout>) {
      if (timer !== handle) nativeClearTimeout(timer);
    },
  });
  try {
    const pending = client.follow("events", { from: 0n }).next();
    await node.until(() => node.connections.length >= 2);
    expect(expire).toBeDefined();
    expire!();
    await expect(pending).rejects.toMatchObject({ code: "deadline_elapsed" });
    await node.until(() => node.active === 0);
  } finally { Object.assign(globalThis, { setTimeout: nativeSetTimeout, clearTimeout: nativeClearTimeout }); }
});
