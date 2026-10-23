import { expect, test } from "bun:test";
import { fromBinary } from "@bufbuild/protobuf";
import { AppendRequestSchema, CommitRequestSchema } from "../generated/proto/stream/v1/stream_pb.js";
import { normalizeWireCommit, normalizeWireCommitBytes, wireAppendRequest } from "../src/contract.js";
import { MemoryStreamProvider } from "../src/memory.js";
import { idempotencyKey } from "../src/types.js";

test("append and atomic commit encode record messages without losing opaque bytes", async () => {
  const payload = new Uint8Array([0, 1, 127, 255]);
  const request = fromBinary(AppendRequestSchema, wireAppendRequest("source", [payload]));
  expect(request.records[0]?.value).toEqual(payload);
  const commit = { conditions: [{ path: "destination", ifAbsent: true as const }],
    mutations: [{ fork: { source: "source", destination: "destination", atTail: 1n, values: [payload] } }] };
  const options = { idempotencyKey: idempotencyKey(new Uint8Array([4])) };
  const normalized = fromBinary(CommitRequestSchema, await normalizeWireCommitBytes(commit, options));
  const mutation = normalized.mutations[0]!.mutation;
  expect(mutation.case === "fork" && mutation.value.records[0]?.value).toEqual(payload);
  expect(await normalizeWireCommit(commit, options)).toEqual(commit);
  const provider = new MemoryStreamProvider();
  expect((await provider.append("source", [payload])).ok).toBe(true);
  expect((await provider.commit(commit, options)).ok).toBe(true);
  const values = [];
  for await (const record of provider.read("destination", { from: 0n, limit: 3 })) values.push(record.value);
  expect(values).toEqual([payload, payload]);
});
