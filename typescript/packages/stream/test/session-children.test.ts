import { expect, test } from "bun:test";
import { StreamSessions } from "../src/session.js";
import { SessionNode, checkpoint } from "./session-node.js";
import { StreamLimit } from "../generated/proto/stream/v1/stream_pb.js";

test("a maximum-size child and its opaque cursor fit the advertised credit", async () => {
  const peer = new SessionNode();
  const path = "a".repeat(StreamLimit.MAX_PATH_BYTES);
  peer.append(path);
  const client = new StreamSessions(peer.connect);
  const children = client.children({ limit: 1 });
  try { expect((await children.next()).value?.path).toBe(path); }
  finally { await children.return(undefined); }
});

test("children replay and creation share the record follow session", async () => {
  const peer = new SessionNode();
  const client = new StreamSessions(peer.connect);
  const cancel = new AbortController();
  const children = client.children({ parent: "agents", limit: 1, signal: cancel.signal });
  const records = client.follow("healthy", { from: 0n, signal: cancel.signal });
  try {
    const first = children.next();
    const healthy = records.next();
    await peer.until(() => peer.active === 2);
    peer.append("agents/m");
    expect((await first).value?.path).toBe("agents/m");
    const later = children.next();
    peer.append("agents/a/deep");
    expect((await later).value?.path).toBe("agents/a");
    peer.append("healthy");
    expect((await healthy).value?.sequence).toBe(0n);
    expect(peer.connections.length).toBe(1);
    expect(peer.actorChannels).toBe(0);
  } finally { cancel.abort(); await children.return(undefined); await records.return(undefined); }
  expect(peer.active).toBe(0);
  expect(peer.outstandingFrames).toBe(0);
});

test("children movement resumes its yielded opaque cursor and fences queued replay", async () => {
  const peer = new SessionNode();
  peer.append("agents/m"); peer.append("agents/a");
  const client = new StreamSessions(peer.connect);
  const children = client.children({ parent: "agents", limit: 1 });
  try {
    expect((await children.next()).value?.path).toBe("agents/m");
    peer.move("agents");
    await checkpoint();
    expect((await children.next()).value?.path).toBe("agents/a");
    const later = children.next();
    peer.append("agents/0");
    expect((await later).value?.path).toBe("agents/0");
    expect(peer.resumes.filter(resume => resume.node === "owner").map(resume => resume.next)).toEqual([1n]);
    expect(peer.connections.length).toBe(2);
  } finally { await children.return(undefined); }
});

test("next immediately after children disconnect recovers instead of terminating", async () => {
  const peer = new SessionNode();
  peer.append("agents/m"); peer.append("agents/a");
  const client = new StreamSessions(peer.connect);
  const children = client.children({ parent: "agents", limit: 1 });
  try {
    expect((await children.next()).value?.path).toBe("agents/m");
    peer.disconnect();
    expect((await children.next()).value?.path).toBe("agents/a");
    expect(peer.connections.length).toBe(2);
  } finally { await children.return(undefined); }
});

test("cancelling unread discovery returns credit and its channel reservation", async () => {
  const peer = new SessionNode();
  peer.append("agents/m"); peer.append("agents/a"); peer.append("agents/0");
  const client = new StreamSessions(peer.connect);
  for (let n = 0; n < 8; n++) {
    const cancel = new AbortController();
    const children = client.children({ parent: "agents", limit: 1, signal: cancel.signal });
    expect((await children.next()).value?.path).toBe("agents/m");
    cancel.abort();
    expect(peer.active).toBe(0);
    await peer.until(() => peer.outstandingFrames === 0);
    expect(peer.outstandingFrames).toBe(0);
  }
  expect(peer.connections.length).toBe(1);
});
