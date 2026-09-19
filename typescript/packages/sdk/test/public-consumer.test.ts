import { expect, test } from "bun:test";
import { Harness, type OperationId } from "@acyclic-labs/harness";
import { openMemoryFs } from "@acyclic-labs/fs/memory";
import { Objects, bytesCodec, jsonCodec } from "@acyclic-labs/objects";
import { StreamClient } from "@acyclic-labs/stream";

const wasm = new Uint8Array(await Bun.file(
  new URL("../../harness/generated/wasm/acyclic_harness_wasm_bg.wasm", import.meta.url),
).arrayBuffer());

test("one consumer can use the public stream, objects, filesystem, and WASM APIs", async () => {
  const stream = StreamClient.memory().json<{ kind: string }>("runs/one");
  expect((await stream.append({ kind: "started" })).ok).toBe(true);
  const records = [];
  for await (const record of stream.read({ from: 0n, limit: 4 })) records.push(record.value);
  expect(records).toEqual([{ kind: "started" }]);

  const objects = Objects.memory();
  const bucket = await objects.createBucket("consumer");
  const documentCodec = jsonCodec<{ ok: boolean }>();
  await bucket.put("result.json", { ok: true }, documentCodec);
  expect((await bucket.get("result.json", documentCodec)).value.ok).toBe(true);
  expect((await bucket.listPage()).entries.map(entry => entry.objectKey)).toEqual(["result.json"]);
  expect((await bucket.put("bytes", Uint8Array.of(1, 2), bytesCodec)).size).toBe(2n);

  const fs = await openMemoryFs();
  try {
    const workspace = await fs.createWorkspace("consumer");
    await workspace.write("/result", Uint8Array.of(7, 8));
    expect([...await workspace.readRange("/result", 0n, 2n)]).toEqual([7, 8]);
    expect((await workspace.checkpoint("result")).id.byteLength).toBe(32);
  } finally {
    fs.close();
  }

  const harness = await Harness.create({
    authority: { kind: "conversation", id: "consumer" },
    issuerId: "consumer",
    issuerKey: new Uint8Array(32).fill(1),
    wasm,
    schemas: [{ name: "consumer.message", version: 1, schema: { type: "object" } }],
  });
  try {
    const scope = harness.issueScope("root", ["event:append"]);
    const applied = harness.apply({
      authority: { kind: "conversation", id: "consumer" },
      operation_id: "01010101-0101-0101-0101-010101010101" as OperationId,
      idempotency_key: "consumer-1",
      expected_revision: 0n,
      scope,
      causal_parent: null,
      action: { kind: "append_custom", schema: "consumer.message", version: 1, value: { text: "hello" } },
    });
    expect(applied.result).toBe("applied");
    expect(harness.snapshot().revision).toBe(1n);
  } finally {
    harness.free();
  }
});
