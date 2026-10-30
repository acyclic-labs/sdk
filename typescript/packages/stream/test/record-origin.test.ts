import { expect, test } from "bun:test";
import { create } from "@bufbuild/protobuf";
import { RecordSchema } from "../generated/proto/stream/v1/stream_pb.js";
import { decodeHttpResponse } from "../generated/wasm/acyclic_stream_wasm.js";
import { checkedRecord } from "../src/record.js";
import { MemoryStreamProvider } from "../src/memory.js";
import { idempotencyKey, type EncodedRecord } from "../src/types.js";

test("nested fork records retain their original paths through read, follow and contextual projection", async () => {
  const provider = new MemoryStreamProvider();
  const key = (value: string) => idempotencyKey(new TextEncoder().encode(value));
  const append = (path: string, value: number, name: string) => provider.append(path, [Uint8Array.of(value)], { idempotencyKey: key(name) });
  const fork = (source: string, destination: string, atTail: bigint) =>
    provider.fork(source, destination, { atTail, idempotencyKey: key(destination) });
  await append("origin/root", 1, "root");
  await append("origin/root/status", 4, "status");
  await fork("origin/root", "origin/a", 1n);
  await append("origin/a", 2, "a");
  await fork("origin/a", "origin/b", 2n);
  await fork("origin/b", "origin/c", 2n);
  const accepted = await append("origin/c", 3, "c");
  const expected = ["origin/root", "origin/a", "origin/c"];
  let original: EncodedRecord | undefined;
  for (let from = 0; from < expected.length; from++) {
    const read = [];
    for await (const record of provider.read("origin/c", { from: BigInt(from), limit: 3 })) read.push(record);
    expect(read.map(record => record.origin)).toEqual(expected.slice(from));
    original ??= read[0];
    const follow = provider.follow("origin/c", { from: BigInt(from) })[Symbol.asyncIterator]();
    try {
      for (const origin of expected.slice(from)) expect((await follow.next()).value?.origin).toBe(origin);
    } finally { await follow.return?.(); }
  }
  const head = await provider.head("origin/b");
  expect(head.last?.origin).toBe("origin/a");
  const page = await provider.childrenPage({ parent: "origin", tailStream: "status", limit: 4 });
  expect(page.children.map(child => child.tailRecord?.origin)).toEqual(Array(4).fill("origin/root/status"));
  if (!accepted.ok) throw new Error("original append refused");
  const envelope = await provider.readCommit(accepted.commitId);
  expect(envelope.mutations[0]?.records[0]?.origin).toBe("origin/c");
  const retained = await provider.heads({ paths: ["origin/c"], limit: 4,
    retained: [{ commitId: accepted.commitId, path: "origin/c", sequence: 2n }],
  });
  expect(retained.retained[0]?.record?.origin).toBe("origin/c");

  // Mutate an actual provider fact at the canonical wire boundary. Elision is
  // meaningful only with its followed path; spelling that same path is invalid.
  if (original === undefined) throw new Error("original provider record missing");
  const wire = create(RecordSchema, { ...original, origin: "" });
  expect(checkedRecord(wire, wire.sequence, "origin/root").origin).toBe("origin/root");
  expect(() => checkedRecord(wire, wire.sequence)).toThrow();
  expect(() => checkedRecord({ ...wire, origin: "origin/c" }, wire.sequence, "origin/c")).toThrow();
  expect(() => checkedRecord({ ...wire, origin: "/invalid//path" }, wire.sequence, "origin/c")).toThrow();
  expect(checkedRecord({ ...wire, origin: "origin/root" }, wire.sequence).origin).toBe("origin/root");
  expect(checkedRecord({ ...wire, origin: "origin/root" }, wire.sequence, "origin/c").origin).toBe("origin/root");

  // Use the actual immutable provider fact for JSON projection controls too.
  const json = (value: unknown) => JSON.stringify(value, (_, value: unknown) =>
    typeof value === "bigint" ? value.toString() : value instanceof Uint8Array ? Buffer.from(value).toString("base64") : value);
  const { origin: _, ...elided } = original;
  const project = (record: unknown, followed?: string) =>
    (decodeHttpResponse("read", json({ records: [record] }), followed) as { records: EncodedRecord[] }).records[0];
  expect(project(elided, "origin/root")?.origin).toBe("origin/root");
  expect(project(original, "origin/c")?.origin).toBe("origin/root");
  expect(() => project(elided)).toThrow();
  expect(() => project(original, "origin/root")).toThrow();
  expect(() => project({ ...original, origin: "invalid//path" }, "origin/c")).toThrow();
  expect((decodeHttpResponse("head", json(head)) as typeof head).last?.origin).toBe("origin/a");
  expect(() => decodeHttpResponse("head", json({ ...head, last: elided }))).toThrow();
  expect(() => decodeHttpResponse("children/page", json({ ...page,
    children: page.children.map(child => ({ ...child, tailRecord: elided })),
  }))).toThrow();
  expect(() => decodeHttpResponse("heads", json({ ...retained,
    heads: retained.heads.map(head => ({ ...head, origin: head?.origin ?? null, last: head?.last ?? null })),
    retained: retained.retained.map(selected => ({ ...selected, record: elided })),
  }))).toThrow();
});
