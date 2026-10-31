import { StreamLimit, type Record as WireRecord } from "../generated/proto/stream/v1/stream_pb.js";
import { StreamError, commitId, type EncodedRecord, type ReadKeySeek } from "./types.js";
import { validatePathValue } from "./contract.js";

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
    return { sequence: record.sequence, value: record.value, commitId: commitId(record.commitId), committedAtMicros: record.committedAtMicros, origin, keys: record.keys, attributes: record.attributes };
  } catch { throw new StreamError("invalid_response", "stream record has an invalid commit identity or origin"); }
}
