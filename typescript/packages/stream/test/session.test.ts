import { expect, test } from "bun:test";
import { StreamSessions } from "../src/session.js";
import { SessionQueue } from "../src/session-queue.js";
import { StreamLimit } from "../generated/proto/stream/v1/stream_pb.js";
import type { SessionFrame } from "../generated/proto/stream/v1/session_pb.js";
import { SessionNode, checkpoint } from "./session-node.js";

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
  expect(node.outstandingFrames).toBe(0);
  expect(node.returned).toBe(2 * (StreamLimit.MAX_ITEMS + 8) + 1);
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
