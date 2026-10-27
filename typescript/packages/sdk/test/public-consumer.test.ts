import { expect, test } from "bun:test";
import { harness as harnessApi, objects as objectsApi, stream as streamApi } from "@acyclic-labs/sdk";
import { openMemoryFs } from "@acyclic-labs/fs/memory";
import { create } from "@bufbuild/protobuf";

test("one consumer can use the public stream, objects, filesystem, and WASM APIs", async () => {
  const stream = streamApi.StreamClient.memory().json("runs/one", value => {
    if (value === null || typeof value !== "object" || Array.isArray(value) ||
        !("kind" in value) || typeof value.kind !== "string") throw new TypeError("expected event");
    return { kind: value.kind };
  });
  expect((await stream.append({ kind: "started" })).ok).toBe(true);
  const records = [];
  for await (const record of stream.read({ from: 0n, limit: 4 })) records.push(record.value);
  expect(records).toEqual([{ kind: "started" }]);

  const objects = await objectsApi.MemoryObjectsV1.create();
  const bucket = create(objectsApi.BucketRefSchema, { name: "consumer" });
  expect((await objects.createBucket(create(objectsApi.CreateBucketRequestSchema, {
    name: bucket.name,
  }))).bucket?.name).toBe(bucket.name);
  const document = new TextEncoder().encode(JSON.stringify({ ok: true }));
  await objects.put(create(objectsApi.PutObjectHeaderSchema, {
    bucket, objectKey: "result.json", metadata: { contentType: "application/json" },
  }), document);
  const read = await objects.get(create(objectsApi.GetObjectRequestSchema, {
    bucket, objectKey: "result.json",
  }), BigInt(document.byteLength));
  expect(JSON.parse(new TextDecoder().decode(read.body))).toEqual({ ok: true });
  expect((await objects.list(create(objectsApi.ListObjectsRequestSchema, { bucket })))
    .entries.map(entry => entry.objectKey)).toEqual(["result.json"]);
  const fs = await openMemoryFs();
  try {
    const workspace = await fs.createWorkspace("consumer");
    await workspace.write("/result", Uint8Array.of(7, 8));
    expect([...await workspace.readRange("/result", 0n, 2n)]).toEqual([7, 8]);
    expect((await workspace.checkpoint("result")).id.byteLength).toBe(32);
  } finally {
    fs.close();
  }

  // This public-client admission probe does not claim host execution or bootstrap.
  await harnessApi.initializeClientQueries();
  const encoded = harnessApi.encodeCanonicalJson({ kind: "root" } satisfies harnessApi.AgentCommand);
  using command = new harnessApi.PreparedAgentCommand(encoded);
  expect(command.value()).toEqual({ kind: "root" });
  const admitted = command.bytes();
  encoded.fill(0);
  expect(command.bytes()).toEqual(admitted);
  expect(() => new harnessApi.PreparedAgentCommand(harnessApi.encodeCanonicalJson({
    kind: "root", principal: "untrusted",
  }))).toThrow();
  const cursor = 9_007_199_254_740_993n;
  expect(harnessApi.decodeClientJson(harnessApi.encodeCanonicalJson({ cursor }))).toEqual({ cursor });
});
