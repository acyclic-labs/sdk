import { StreamLimit, type Record as WireRecord } from "../generated/proto/stream/v1/stream_pb.js";
import { StreamError, commitId, type EncodedRecord } from "./types.js";

/** Validate a record's cursor, body bound and canonical commit identity. */
export function checkedRecord(record: WireRecord | undefined, expected: bigint): EncodedRecord {
  if (record === undefined || record.sequence !== expected || record.value.length > StreamLimit.MAX_RECORD_BYTES) {
    throw new StreamError("invalid_response", "stream response contains an invalid record cursor or body");
  }
  try { return { sequence: record.sequence, value: record.value, commitId: commitId(record.commitId), committedAtMicros: record.committedAtMicros, keys: record.keys, attributes: record.attributes }; }
  catch { throw new StreamError("invalid_response", "stream record omitted its canonical commit identity"); }
}
