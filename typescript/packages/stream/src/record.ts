import { ReadResponseSchema, StreamLimit, type ReadResponse, type Record as WireRecord } from "../generated/proto/stream/v1/stream_pb.js";
import { StreamError, commitId, type EncodedRecord, type ReadKeySeek, type ReadOptions } from "./types.js";
import { readResponseRecords, validatePathValue } from "./contract.js";
import { fromBinary } from "@bufbuild/protobuf";

/** One cursor/key/origin admission for native and WASM record frames. */
export async function* recordsFromFrames(
  frames: AsyncIterable<Uint8Array>, path: string, next: bigint, read?: ReadOptions, signal?: AbortSignal,
): AsyncGenerator<EncodedRecord> {
  let count = 0;
  for await (const bytes of frames) {
    let response: ReadResponse;
    try { response = fromBinary(ReadResponseSchema, bytes); }
    catch { throw new StreamError("invalid_response", "stream response contains a malformed record frame"); }
    // Admit a whole bounded frame before exposing its first record.
    const records = readResponseRecords(response).map(decoded => {
      if (read !== undefined) {
        if (++count > read.limit) throw new StreamError("invalid_response", "read exceeds requested limit");
        next = checkedReadSequence(decoded, next, read.key);
      } else if (decoded.sequence < next) {
        throw new StreamError("invalid_response", "follow record cursor regressed");
      }
      const record = checkedRecord(decoded, decoded.sequence, path);
      next = record.sequence + 1n;
      return record;
    });
    for (const record of records) {
      if (signal?.aborted) return;
      yield record;
    }
  }
}

/** Preserve dense reads and validate sparse indexed results at every transport. */
export function checkedReadSequence(record: Pick<EncodedRecord, "sequence" | "keys">, next: bigint, key?: ReadKeySeek): bigint {
  if (key === undefined ? record.sequence !== next : record.sequence < next || record.sequence >= key.before
    || !record.keys.some(value => value.length === key.key.length && value.every((byte, index) => byte === key.key[index]))) {
    throw new StreamError("invalid_response", "read response violates its cursor or indexed key");
  }
  return record.sequence;
}

/** Preserve the canonical origin: only the followed path may be elided. */
export function checkedRecord(record: WireRecord | undefined, expected: bigint, followed?: string): EncodedRecord {
  if (record === undefined || record.sequence !== expected || record.value.length > StreamLimit.MAX_RECORD_BYTES) {
    throw new StreamError("invalid_response", "stream response contains an invalid record cursor or body");
  }
  try {
    if (typeof record.origin !== "string" || record.origin === followed) throw new Error("noncanonical origin");
    const origin = record.origin === "" ? followed : record.origin;
    if (origin === undefined) throw new Error("missing origin");
    if (record.origin !== "") validatePathValue(origin);
    return projectRecord(record, followed);
  } catch { throw new StreamError("invalid_response", "stream record has an invalid commit identity or origin"); }
}

/** Project an admitted record, resolving its elided origin from the followed path. */
export function projectRecord(record: WireRecord, followed?: string): EncodedRecord {
  return { sequence: record.sequence, value: record.value, commitId: commitId(record.commitId),
    committedAtMicros: record.committedAtMicros, keys: record.keys, attributes: record.attributes,
    origin: record.origin === "" && followed !== undefined ? followed : record.origin };
}
