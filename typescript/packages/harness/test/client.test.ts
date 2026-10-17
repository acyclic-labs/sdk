import { beforeAll, expect, test } from "bun:test";
import "fake-indexeddb/auto";
import { HarnessClient, ClientQuery, TaskVolume, WasmStream, initializeClientQueries } from "../src/client.js";

beforeAll(initializeClientQueries);

test("nested reference owner credentials are rejected before persistence", async () => {
  const f = await setup();
  const file = { volume: { provider: { namespace: "test", family: "filesystem", version: "2" }, id: "files", class: "project", owner: { kind: "project", id: "project" } }, path: "prompt.txt", version: "v1", descriptor: { sha256: Array(32).fill(1), byte_length: 1, media_type: "text/plain" }, display_name: "prompt" };
  const command = JSON.parse(new TextDecoder().decode(encode.encode("pinned")));
  const bytes = (value: unknown) => new TextEncoder().encode(JSON.stringify(value));
  await f.client.enqueue(request("pinned.json"), bytes({ ...command, payload: { content: file } }));
  const before = await f.outbox.head();
  for (const container of ["content", "artifacts", "references"]) {
    for (const key of ["token", "Authorization", "credential", "bearer", "unexpected"]) {
      const reference = structuredClone(file);
      Object.assign(reference.volume.owner, { [key]: "secret" });
      const payload = { [container]: container === "content" ? reference : [reference] };
      await expect(f.client.enqueue(request("owner-token.json"), bytes({ ...command, payload }))).rejects.toMatchObject({ kind: "Harness" });
      expect(f.client.pending()).toBe(1);
      expect(await f.outbox.head()).toEqual(before);
    }
  }
  expect(await f.client.flush()).toBe(1);
  f.free();
});
const encode = { encode: (operationId: string) => new TextEncoder().encode(JSON.stringify({ operationId, authority: { kind: "task", id: "task" }, kind: "task.submit", offlineSafe: true, payload: {} })) };
const limits = { commands: 8n, bytes: 4096n, durability: "Accepted" } as const;
const queryLimits = { files: 32n, segments: 64n, bytes: 8192n };
const request = (id: string) => ({ Task: { task: "task", file: { Request: id } } }) as const;

async function setup(durable = false) {
  const name = `file-client-${crypto.randomUUID()}`;
  const stream = durable ? await WasmStream.openBrowser(name, 64, 65536n) : new WasmStream();
  const outbox = await TaskVolume.create(stream, "client/files");
  const target = await TaskVolume.create(stream, "agent/files");
  const policy = { ...limits, durability: durable ? "Durable" : "Accepted" } as const;
  const client = await HarnessClient.open(outbox, target, policy);
  return { name, stream, outbox, target, client, policy,
    free() { client.free(); outbox.free(); target.free(); stream.free(); } };
}

test("the exported client is the Rust file client and restores enqueue order", async () => {
  const f = await setup();
  await f.client.enqueue(request("z.json"), encode.encode("first"));
  await f.client.enqueue(request("a.json"), encode.encode("second"));
  expect(f.client.pending()).toBe(2);
  const query = await ClientQuery.open(f.target, "Tasks", null, queryLimits);
  const stop = Promise.withResolvers<void>();
  const seen: string[] = [];
  const follow = query.follow(delivery => { if (delivery.file) seen.push(JSON.parse(new TextDecoder().decode(delivery.file.bytes)).operationId); }, () => {
    if (seen.length === 2) stop.resolve();
  }, stop.promise);
  f.client.free();
  const restored = await HarnessClient.open(f.outbox, f.target, f.policy);
  expect(restored.pending()).toBe(2);
  expect(await restored.flush()).toBe(2);
  await follow;
  expect(seen).toEqual(["first", "second"]);
  expect(await restored.flush()).toBe(0);
  query.free(); restored.free(); f.outbox.free(); f.target.free(); f.stream.free();
});

test("duplicate requests keep identity and conflicting metadata cannot replace them", async () => {
  const f = await setup();
  await f.client.enqueue(request("one.json"), encode.encode("one"));
  await f.client.enqueue(request("one.json"), encode.encode("one"));
  expect(f.client.pending()).toBe(1);
  await expect(f.client.enqueue(request("one.json"), encode.encode("different"))).rejects.toBeDefined();
  expect(f.client.pending()).toBe(1);
  expect(await f.client.flush()).toBe(1);
  await f.client.enqueue(request("one.json"), encode.encode("one"));
  expect(await f.client.flush()).toBe(0);
  f.free();
});

for (const [commands, kind] of [[1n, "CommandQuota"], [2n, "ByteQuota"]] as const) {
test(`${kind} rejects before storing another file and allows acknowledged retries`, async () => {
  const f = await setup();
  f.client.free();
  const bytes = BigInt(encode.encode("one").byteLength);
  const limit = kind === "CommandQuota" ? commands : bytes;
  const client = await HarnessClient.open(f.outbox, f.target, { ...f.policy, commands, bytes });
  await client.enqueue(request("one.json"), encode.encode("one"));
  const before = await f.outbox.head();
  await expect(client.enqueue(request("two.json"), encode.encode("two"))).rejects.toEqual({ kind, detail: { limit } });
  await expect(client.enqueue("Goal", encode.encode("goal"))).rejects.toMatchObject({ kind: "Harness" });
  expect(await f.outbox.head()).toEqual(before);
  expect(client.pending()).toBe(1);
  expect(await client.flush()).toBe(1);
  await client.enqueue(request("two.json"), encode.encode("two"));
  await client.enqueue(request("one.json"), encode.encode("one"));
  expect(client.pending()).toBe(1);
  client.free();
  const reopened = await HarnessClient.open(f.outbox, f.target, { ...f.policy, commands, bytes });
  await reopened.enqueue(request("one.json"), encode.encode("one"));
  expect(reopened.pending()).toBe(1);
  expect(await reopened.flush()).toBe(1);
  reopened.free(); f.outbox.free(); f.target.free(); f.stream.free();
});
}

test("a stale client cannot move the persisted cursor backwards", async () => {
  const f = await setup();
  await f.client.enqueue(request("one.json"), encode.encode("one"));
  await f.client.enqueue(request("two.json"), encode.encode("two"));
  const stale = await HarnessClient.open(f.outbox, f.target, f.policy);
  expect(await f.client.flush()).toBe(2);
  const cursor = f.client.cursor();
  await expect(stale.flush()).rejects.toBeDefined();
  stale.free();
  const reopened = await HarnessClient.open(f.outbox, f.target, f.policy);
  expect(reopened.cursor()).toEqual(cursor);
  expect(reopened.pending()).toBe(0);
  reopened.free(); f.free();
});

test("a terminal rejection retires its entry and allows the next request after reopen", async () => {
  const f = await setup();
  await f.target.write(await f.target.head(), request("rejected.json"), encode.encode("different"), null);
  await f.client.enqueue(request("rejected.json"), encode.encode("rejected"));
  await f.client.enqueue(request("valid.json"), encode.encode("valid"));
  await expect(f.client.flush()).rejects.toMatchObject({ kind: "Rejected", detail: { request: request("rejected.json") } });
  expect(f.client.pending()).toBe(1);
  f.client.free();
  const reopened = await HarnessClient.open(f.outbox, f.target, f.policy);
  expect(await reopened.flush()).toBe(1);
  reopened.free();
  const restored = await HarnessClient.open(f.outbox, f.target, f.policy);
  expect(restored.pending()).toBe(0);
  restored.free(); f.outbox.free(); f.target.free(); f.stream.free();
});

for (const mutation of [
  { kind: "interaction.resolve.approval" }, { offlineSafe: false }, { bearer: "credential" },
  { payload: { content: "inline prompt" } }, { payload: { attachments: new Uint8Array([1, 2]) } },
  { payload: { metadata: { authorization: 1 } } }, { payload: { metadata: { hint: "text" } } },
  { authority: { kind: "task", id: "../escape" } },
  { payload: { references: [{ path: "unpinned" }] } },
]) {
  test(`unsafe request is rejected before any file publication: ${JSON.stringify(mutation)}`, async () => {
    const f = await setup();
    const before = await f.outbox.head();
    const value = JSON.parse(new TextDecoder().decode(encode.encode("unsafe")));
    await expect(f.client.enqueue(request("unsafe.json"), new TextEncoder().encode(JSON.stringify({ ...value, ...mutation })))).rejects.toMatchObject({ kind: "Harness" });
    expect(await f.outbox.head()).toEqual(before);
    expect(f.client.pending()).toBe(0);
    f.free();
  });
}

test("persistent provider files recover pending intent after all handles close", async () => {
  const f = await setup(true);
  await f.client.enqueue(request("offline.json"), encode.encode("queued"));
  const { name, policy } = f;
  f.free();
  const stream = await WasmStream.openBrowser(name, 64, 65536n);
  const outbox = await TaskVolume.open(stream, "client/files");
  const target = await TaskVolume.open(stream, "agent/files");
  const client = await HarnessClient.open(outbox, target, policy);
  expect(client.pending()).toBe(1);
  const readerStream = await WasmStream.openBrowser(name, 64, 65536n);
  const readerVolume = await TaskVolume.open(readerStream, "agent/files");
  const query = await ClientQuery.open(readerVolume, "Tasks", null, queryLimits);
  const stop = Promise.withResolvers<void>();
  const followed = query.follow(() => {}, () => stop.resolve(), stop.promise);
  expect(await client.flush()).toBe(1);
  await followed;
  const views = query.views();
  expect(JSON.parse(new TextDecoder().decode(views.segments(request("offline.json"))![0])).operationId).toBe("queued");
  views.free(); query.free(); readerVolume.free(); readerStream.free();
  client.free(); outbox.free(); target.free(); stream.free();
});
