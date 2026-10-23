import { streamResource } from "../src/contract.js";
import { expect, test } from "bun:test";
import { create, equals } from "@bufbuild/protobuf";
import * as wire from "../generated/proto/stream/v1/session_pb.js";
import { StreamSessions } from "../src/session.js";
import { SessionNode, checkpoint } from "./session-node.js";

const latest = (path = "status") => create(wire.SessionFollowSchema, { resource: streamResource(path), delivery: wire.SessionDelivery.LATEST });

test("LATEST keeps jumps, attention and watermarks while a full follow progresses", async () => {
  const peer = new SessionNode();
  for (const [next, attention] of [[2n, false], [3n, false], [9n, true], [10n, false]] as const) peer.publishLatest("status", next, attention);
  const client = new StreamSessions(peer.connect);
  const status = client.followData(latest(), { from: 0n });
  const first = await status.next();
  expect(equals(wire.SessionDataSchema, first.value!, peer.latest.get("status")![0]!)).toBe(true);
  await checkpoint();
  // The status consumer is suspended with buffered data; ALL stays independent.
  const full = client.follow("events", { from: 0n });
  const pending = full.next();
  await peer.until(() => peer.active === 2);
  peer.append("events");
  expect((await pending).value?.sequence).toBe(0n);
  for (const expected of peer.latest.get("status")!.slice(1)) {
    expect(equals(wire.SessionDataSchema, (await status.next()).value!, expected)).toBe(true);
  }
  expect(peer.connections.length).toBe(1);
  expect(peer.actorChannels).toBe(0);
  const acknowledgements = peer.received.flatMap(frame => frame.body.case === "ack" && frame.channelId === 1n ? [frame.body.value.applied!.next] : []);
  expect(acknowledgements.length).toBeGreaterThan(0);
  expect(acknowledgements.every((cut, index) => [2n, 3n, 9n, 10n].includes(cut)
    && (index === 0 || cut > acknowledgements[index - 1]!))).toBe(true);
  await status.return(undefined);
  await full.return(undefined);
  expect(peer.outstandingFrames).toBe(0);
});

test("LATEST fences buffered generations and resumes the consumed cut after moved and disconnect", async () => {
  const peer = new SessionNode();
  for (const next of [2n, 7n, 11n]) peer.publishLatest("status", next, next === 7n);
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  peer.beforeConnect = async node => { if (node === "owner") await gate; };
  const client = new StreamSessions(peer.connect);
  const status = client.followData(latest(), { from: 0n });
  expect((await status.next()).value?.after?.next).toBe(2n);
  peer.move("status");
  await checkpoint();
  const resumed = status.next();
  release();
  expect((await resumed).value?.after?.next).toBe(7n);
  peer.disconnect("owner");
  await peer.until(() => peer.resumes.length === 3);
  const last = await status.next();
  expect(last.value?.after?.next).toBe(11n);
  expect(peer.resumes.map(resume => resume.next)).toEqual([0n, 2n, 7n]);
  expect(peer.connections.filter(connection => !connection.closed && connection.node === "owner").length).toBe(1);
  await status.return(undefined);
  expect(peer.outstandingFrames).toBe(0);
});

test("raw ALL data retains metadata and invalid modes fail before opening a session", async () => {
  const peer = new SessionNode();
  const client = new StreamSessions(peer.connect);
  for (const [delivery, from, guard] of [[wire.SessionDelivery.UNSPECIFIED, 0n, 0], [wire.SessionDelivery.LATEST, 1n, 32], [wire.SessionDelivery.ALL, 1n, 31]] as const) {
    await expect(client.followData(create(wire.SessionFollowSchema, { resource: "streams:/events", delivery, precedingCommitId: new Uint8Array(guard) }), { from }).next()).rejects.toBeDefined();
  }
  expect(peer.connections.length).toBe(0);
  peer.append("events");
  const records = client.followData(create(wire.SessionFollowSchema, { resource: "streams:/events", delivery: wire.SessionDelivery.ALL }), { from: 0n });
  const data = (await records.next()).value!;
  expect(data.after?.next).toBe(1n);
  expect(data.accepted).toBe(1n);
  expect(data.durable).toBe(0n);
  expect(data.item.case).toBe("record");
  await records.return(undefined);
});

test("checked ALL confirms its anchor and reconnects with the last yielded commit", async () => {
  const peer = new SessionNode();
  peer.append("events", 3);
  const client = new StreamSessions(peer.connect);
  const follow = create(wire.SessionFollowSchema, { resource: "streams:/events", delivery: wire.SessionDelivery.ALL, precedingCommitId: peer.history.get("events")![0]!.commitId });
  const data = client.followData(follow, { from: 1n });
  expect((await data.next()).value?.after?.next).toBe(2n);
  peer.disconnect();
  await peer.until(() => peer.resumes.length === 2);
  expect((await data.next()).value?.after?.next).toBe(3n);
  const interests = peer.received.flatMap(frame => frame.body.case === "interest" ? frame.body.value.upsert : []);
  expect(interests.map(interest => interest.resume?.next)).toEqual([1n, 2n]);
  expect(interests.map(interest => interest.kind.case === "follow" ? [...interest.kind.value.precedingCommitId] : [])).toEqual([...peer.history.get("events")!.slice(0, 2).map(record => [...record.commitId])]);
  await data.return(undefined);
});

test("missing or mismatched checked confirmation removes only its channel without data credit", async () => {
  for (const confirmation of [new Uint8Array(), new Uint8Array(32).fill(9)]) {
    const peer = new SessionNode();
    const client = new StreamSessions(async (node, signal) => {
      const transport = await peer.connect(node, signal);
      return { ...transport, incoming: { async *[Symbol.asyncIterator]() {
        for await (const input of transport.incoming) {
          if (input.body.case === "opened" && input.channelId === 3n) input.body.value.precedingCommitId = confirmation;
          yield input;
        }
      } } };
    });
    const healthy = client.follow("healthy", { from: 0n });
    const pending = healthy.next();
    await peer.until(() => peer.active === 1);
    const checked = client.followData(create(wire.SessionFollowSchema, { resource: "streams:/events", delivery: wire.SessionDelivery.ALL, precedingCommitId: new Uint8Array(32).fill(7) }), { from: 1n });
    await expect(checked.next()).rejects.toMatchObject({ code: "unsupported" });
    await peer.until(() => peer.active === 1);
    expect(peer.received.some(input => input.channelId === 3n && input.body.case === "window")).toBe(false);
    peer.append("healthy");
    expect((await pending).value?.sequence).toBe(0n);
    expect(peer.connections.length).toBe(1);
    await healthy.return(undefined);
  }
});
