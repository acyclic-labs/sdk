import { describe, expect, test } from "bun:test";
import { create, toBinary } from "@bufbuild/protobuf";
import { zstdCompressSync } from "node:zlib";
import { Codec, ReadResponseSchema, RecordBatchSchema, RecordSchema, StreamLimit } from "../generated/proto/stream/v1/stream_pb.js";
import { ensureStreamWasm, readResponseRecords } from "../src/contract.js";
import { StreamError } from "../src/index.js";

describe("read frames", () => {
  const records = (from: bigint, count: number) => Array.from({ length: count }, (_, index) => create(RecordSchema, { sequence: from + BigInt(index), value: new Uint8Array(100).fill(index), commitId: new Uint8Array(32).fill(1), committedAtMicros: 7n }));
  const batch = (values: ReturnType<typeof records>) => toBinary(RecordBatchSchema, create(RecordBatchSchema, { records: values }));
  const plain = (values: ReturnType<typeof records>) => { const data = batch(values); return create(ReadResponseSchema, { codec: Codec.NONE, data, decodedLength: BigInt(data.length) }); };
  const zstd = (values: ReturnType<typeof records>) => { const data = batch(values); return create(ReadResponseSchema, { codec: Codec.ZSTD, data: zstdCompressSync(data), decodedLength: BigInt(data.length) }); };
  const invalid = (frame: ReturnType<typeof plain>) => expect(() => readResponseRecords(frame)).toThrow(StreamError);

  test("a compressed frame decodes to the same records", async () => {
    await ensureStreamWasm();
    for (const count of [0, 1, 64]) {
      const expected = records(5n, count);
      expect(readResponseRecords(zstd(expected))).toEqual(expected);
      expect(readResponseRecords(plain(expected))).toEqual(expected);
    }
  });

  test("mixed compressed and plain frames reassemble in order", async () => {
    await ensureStreamWasm();
    const expected = records(0n, 30);
    const frames = Array.from({ length: 5 }, (_, index) => (index % 2 === 0 ? zstd : plain)(expected.slice(index * 6, index * 6 + 6)));
    expect(frames.flatMap(frame => readResponseRecords(frame))).toEqual(expected);
  });

  test("oversized, mismatched, corrupt and unknown-codec frames are refused", async () => {
    await ensureStreamWasm();
    const maximum = BigInt(StreamLimit.MAX_COMMAND_BYTES);
    // Garbage data proves the declared length is refused before decompressing.
    for (const codec of [Codec.NONE, Codec.ZSTD]) {
      for (const decodedLength of [maximum + 1n, 0xffff_ffff_ffff_ffffn]) invalid(create(ReadResponseSchema, { codec, data: new Uint8Array(16).fill(255), decodedLength }));
    }
    const expanding = zstd(records(0n, 64));
    invalid({ ...expanding, decodedLength: 100n });
    const long = zstd(records(0n, 3));
    invalid({ ...long, decodedLength: long.decodedLength + 1n });
    const short = plain(records(0n, 3));
    invalid({ ...short, decodedLength: short.decodedLength - 1n });
    for (const codec of [0, 3]) invalid({ ...plain(records(0n, 3)), codec: codec as Codec });
    invalid(create(ReadResponseSchema, { codec: Codec.ZSTD, data: new Uint8Array(0), decodedLength: 0n }));
    expect(readResponseRecords(plain([]))).toEqual([]);
    const corrupt = zstd(records(0n, 30));
    invalid({ ...corrupt, data: corrupt.data.slice(0, corrupt.data.length / 2) });
  });
});
