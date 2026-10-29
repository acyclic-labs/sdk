import assert from "node:assert/strict";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { WasmStream, decodeReadResponse } from "../../generated/wasm/acyclic_harness_wasm.js";
import { AppendRequestSchema, CommitRequestSchema, CommitResponseSchema, CommitMutationSchema, FollowRequestSchema,
  ReadRequestSchema, ReadResponseSchema, RecordBatchSchema, WaitAppliedRequestSchema,
} from "../../../stream/generated/proto/stream/v1/stream_pb.js";
import type { OwnedStreamProviderPort } from "../../src/stream-provider.js";

/** The same original provider must be visible through independently opened
 * root/child bindings. Supply an actual disposable provider, never responses. */
export async function sharedProviderTree(port: OwnedStreamProviderPort): Promise<void> {
  const root = WasmStream.connectProvider(port);
  const child = WasmStream.connectProvider(port);
  const encode = new TextEncoder();
  const record = (text: string) => ({ value: encode.encode(text) });
  const read = async (stream: WasmStream, path: string) => {
    const chunks = await stream.read(toBinary(ReadRequestSchema,
      create(ReadRequestSchema, { path, from: 0n, limit: 8 })));
    return chunks.flatMap((bytes: Uint8Array) => {
      const page = fromBinary(ReadResponseSchema, bytes);
      return fromBinary(RecordBatchSchema, decodeReadResponse(page.codec, page.data, page.decodedLength)).records;
    });
  };
  try {
    await root.dispatch("append", toBinary(AppendRequestSchema, create(AppendRequestSchema, {
      path: "tree/root", idempotencyKey: encode.encode("root-record"), records: [record("root")],
    })));
    const request = toBinary(CommitRequestSchema, create(CommitRequestSchema, {
      idempotencyKey: encode.encode("child-activation"),
      mutations: [
        { mutation: { case: "fork", value: { source: "tree/root", destination: "tree/child", atTail: 1n } } },
        { mutation: { case: "append", value: { path: "tree/activation", records: [record("child")] } } },
      ],
    }));
    const committed = await root.dispatch("commit", request);
    const receipt = fromBinary(CommitResponseSchema, committed);
    assert.equal(receipt.outcome.case, "committed");
    assert.deepEqual(await child.dispatch("commit", request), committed);
    const changed = fromBinary(CommitRequestSchema, request);
    changed.mutations.push(create(CommitMutationSchema, { mutation: { case: "append", value: {
      path: "tree/changed", records: [record("changed")],
    } } }));
    await assert.rejects(child.dispatch("commit", toBinary(CommitRequestSchema, changed)),
      { code: "idempotency_mismatch" });
    const inherited = await read(child, "tree/child");
    const activation = await read(child, "tree/activation");
    assert.equal(inherited.length, 1);
    assert.deepEqual(inherited[0]!.value, encode.encode("root"));
    assert.equal(activation.length, 1);
    if (receipt.outcome.case !== "committed") throw new Error("fork did not commit");
    assert.deepEqual(activation[0]!.commitId, receipt.outcome.value.commitId);
    await child.dispatch("append", toBinary(AppendRequestSchema, create(AppendRequestSchema, {
      path: "tree/child", idempotencyKey: encode.encode("child-record"), records: [record("child")],
    })));
    assert.equal((await read(root, "tree/root")).length, 1);
    assert.equal((await read(root, "tree/child")).length, 2);
    // The original failure mode: a different Actor-local provider cannot see
    // the adopted tree, even with exactly the same path and request bytes.
    const isolated = new WasmStream();
    try { await assert.rejects(read(isolated, "tree/child"), { code: "not_found" }); }
    finally { isolated.free(); }
    const opened = await port.open("follow", toBinary(FollowRequestSchema,
      create(FollowRequestSchema, { path: "tree/child", from: 2n })), new Uint8Array());
    if ("code" in opened) throw new Error(`follow refused: ${opened.code}`);
    const pending = opened.value.next();
    const revision = receipt.outcome.value.revision!;
    const waiting = port.dispatch("wait_applied", toBinary(WaitAppliedRequestSchema,
      create(WaitAppliedRequestSchema, { atLeast: [{ ...revision, index: revision.index + 100n }] })));
    const registering = port.open("follow", toBinary(FollowRequestSchema,
      create(FollowRequestSchema, { path: "tree/missing", from: 0n, whenAbsent: 1 })), new Uint8Array());
    // A completed real read proves ordinary work progresses while the earlier
    // minimum and missing-path requests are parked on the same executor.
    assert.equal((await read(root, "tree/child")).length, 2);
    port.close();
    assert.deepEqual(await pending, { code: "unavailable" });
    assert.deepEqual(await waiting, { code: "unavailable" });
    assert.deepEqual(await registering, { code: "unavailable" });
    assert.deepEqual(await port.dispatch("commit", request), { code: "unavailable" });
  } finally { root.free(); child.free(); port.close(); }
}
