import { expect, test } from "bun:test";
import { harness as harnessApi, objects as objectsApi, stream as streamApi } from "@acyclic-labs/sdk";
import { openMemoryFs } from "@acyclic-labs/fs/memory";

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

  const objects = objectsApi.Objects.memory();
  const bucket = await objects.createBucket("consumer");
  const documentCodec = objectsApi.jsonCodec(value => {
    if (value === null || typeof value !== "object" || !("ok" in value) || typeof value.ok !== "boolean") {
      throw new TypeError("expected a result document");
    }
    return { ok: value.ok };
  });
  await bucket.put("result.json", { ok: true }, documentCodec);
  expect((await bucket.get("result.json", documentCodec)).value.ok).toBe(true);
  expect((await bucket.listPage()).entries.map(entry => entry.objectKey)).toEqual(["result.json"]);
  expect((await bucket.put("bytes", Uint8Array.of(1, 2), objectsApi.bytesCodec)).size).toBe(2n);

  const fs = await openMemoryFs();
  try {
    const workspace = await fs.createWorkspace("consumer");
    await workspace.write("/result", Uint8Array.of(7, 8));
    expect([...await workspace.readRange("/result", 0n, 2n)]).toEqual([7, 8]);
    expect((await workspace.checkpoint("result")).id.byteLength).toBe(32);
  } finally {
    fs.close();
  }

  const authority = { kind: "conversation", id: "consumer" } as const;
  const harness = await harnessApi.Harness.create({
    authority,
    issuerId: "consumer",
    issuerKey: new Uint8Array(32).fill(1),
    schemas: [{
      name: "consumer.message",
      version: 1,
      implementation_digest: Array(32).fill(9),
      fork_policy: "inherit",
      schema: { type: "object" },
    }],
  });
  try {
    const scope = harness.issueScope("root", ["event:append"]);
    const content = harness.validateFileRef({
      volume: { provider: { namespace: "consumer", family: "filesystem", version: "2" },
        id: "extension", class: "project", owner: { kind: "project", id: "consumer" } },
      path: "extension/event.json", version: "generation-1",
      descriptor: harness.fileDescriptor(new TextEncoder().encode('{"text":"hello"}'), "application/json"),
      display_name: "event.json",
    });
    const applied = harness.apply({
      authority,
      operation_id: harness.identity("operation", "01010101-0101-0101-0101-010101010101"),
      idempotency_key: "consumer-1",
      expected_revision: 0n,
      scope,
      causal_parent: null,
      action: { kind: "append_custom", schema: "consumer.message", version: 1, content },
    });
    expect(applied.result).toBe("applied");
    expect(harness.snapshot().revision).toBe(1n);
  } finally {
    harness.free();
  }
});
