import { StreamLimit, type Record as WireRecord } from "../generated/proto/stream/v1/stream_pb.js";
import { StreamError, commitId, type EncodedRecord, type ReadKeySeek } from "./types.js";
import { validatePathValue } from "./contract.js";

/** Lexical byte order, as the Rust key index orders keys. */
function compareBytes(left: Uint8Array, right: Uint8Array): number {
  for (let index = 0; index < Math.min(left.length, right.length); index += 1) {
    if (left[index] !== right[index]) return left[index]! - right[index]!;
  }
  return left.length - right.length;
}

/** Admits each read result at its place in the read, at every transport: dense
 * for positional reads; for indexed reads the same rule as Rust's
 * `ReadKeySeek::advance`, carrying the continuation read after each record. */
export class ReadCursor {
  #from: bigint;
  #key: ReadKeySeek | undefined;
  constructor(from: bigint, key?: ReadKeySeek) { this.#from = from; this.#key = key; }
  admit(record: Pick<EncodedRecord, "sequence" | "keys">): bigint {
    const key = this.#key;
    const invalid = () => new StreamError("invalid_response", "read response violates its cursor or indexed key");
    if (key === undefined) {
      if (record.sequence !== this.#from) throw invalid();
      this.#from += 1n;
      return record.sequence;
    }
    const end = key.end;
    const selected = end === undefined
      ? record.keys.find(value => compareBytes(value, key.key) === 0)
      : record.keys
        .filter(value => compareBytes(value, end) < 0 && (compareBytes(value, key.key) > 0
          || (compareBytes(value, key.key) === 0 && record.sequence >= this.#from)))
        .sort(compareBytes)[0];
    if (selected === undefined || record.sequence >= key.before || (end === undefined && record.sequence < this.#from)) throw invalid();
    if (key.order === "latest_first") this.#key = { ...key, before: record.sequence };
    else { this.#key = { ...key, key: selected }; this.#from = record.sequence + 1n; }
    return record.sequence;
  }
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
