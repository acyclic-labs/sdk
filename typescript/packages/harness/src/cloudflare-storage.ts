import type { WasmObjectsMedium } from "../generated/wasm/acyclic_harness_wasm.js";
import type { EventStorage, StreamChange } from "./event-storage.js";
export type { StreamChange } from "./event-storage.js";

/** The synchronous SQLite surface used by the storage adapter. */
export interface SqliteStorage {
  readonly sql: {
    exec<Row extends Record<string, SqlValue>>(
      query: string, ...bindings: SqlValue[]
    ): Iterable<Row>;
  };
  transactionSync<T>(callback: () => T): T;
  sync(): Promise<void>;
}

/** Values accepted by Durable Objects SQLite parameter binding. */
export type SqlValue = string | number | ArrayBuffer | null;

/** Raw metadata rows and alarm I/O. Host rules live in the shared event core. */
export class SqliteEventStorage implements EventStorage {
  constructor(private readonly storage: SqliteStorage & {
    setAlarm(time: number): Promise<void>; deleteAlarm(): Promise<void>;
  }, private readonly domain: string) {
    storage.sql.exec("CREATE TABLE IF NOT EXISTS acyclic_host_metadata (domain TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(domain, key))");
  }
  get(key: string): string | undefined {
    const [row] = this.storage.sql.exec("SELECT value FROM acyclic_host_metadata WHERE domain = ? AND key = ?", this.domain, key);
    if (!row) return undefined;
    if (typeof row.value !== "string") throw new Error("STEP sqlite-metadata-read failed; recovery: restore metadata from a verified SQLite backup");
    return row.value;
  }
  put(key: string, value: string): void {
    this.storage.sql.exec("INSERT INTO acyclic_host_metadata (domain, key, value) VALUES (?, ?, ?) ON CONFLICT(domain, key) DO UPDATE SET value = excluded.value", this.domain, key, value);
  }
  entries(): [string, string][] {
    return Array.from(this.storage.sql.exec("SELECT key, value FROM acyclic_host_metadata WHERE domain = ? ORDER BY key", this.domain), row => {
      if (typeof row.key !== "string" || typeof row.value !== "string") throw new Error("STEP sqlite-metadata-read failed; recovery: restore metadata from a verified SQLite backup");
      return [row.key, row.value];
    });
  }
  transaction<T>(operation: () => T): T { return this.storage.transactionSync(operation); }
  flush(): Promise<void> { return this.storage.sync(); }
  alarm(time: number | undefined): Promise<void> {
    return time === undefined ? this.storage.deleteAlarm() : this.storage.setAlarm(time);
  }
}

const CHUNK_BYTES = 128 * 1024;

function bytes(value: SqlValue | undefined): Uint8Array {
  if (!(value instanceof ArrayBuffer)) {
    throw new Error("STEP sqlite-read failed: invalid blob; recovery: reopen the Durable Object from its retained SQLite storage");
  }
  return new Uint8Array(value);
}

function count(value: SqlValue | undefined): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new Error("STEP sqlite-read failed: invalid counter; recovery: reopen the Durable Object from its retained SQLite storage");
  }
  return value;
}

function equal(a: Uint8Array, b: Uint8Array): boolean {
  return a.length === b.length && a.every((byte, index) => byte === b[index]);
}

/**
 * SQL persistence for the shared Rust JournalStream. This class never interprets
 * a Stream command, retry key, path, filter, fork or commit condition.
 *
 * Retain one instance per consistency domain, and inject that same instance into
 * every provider opener. Journal chunks stay below SQLite's per-row bound;
 * transactionSync atomically publishes all chunks and the command's ordinal.
 */
export class SqliteJournal {
  private maximumFrameBytes = 0;
  private readonly subscribers = new Set<{ after: number; ready: () => void }>();
  constructor(private readonly storage: SqliteStorage, private readonly domain: string, private readonly appended?: (changes: readonly StreamChange[]) => void) {
    if (domain.length === 0 || domain.length > 256) {
      throw new Error("STEP sqlite-open failed: invalid domain; recovery: choose a nonempty domain of at most 256 characters");
    }
  }

  async configure(configuration: Uint8Array, maximumFrameBytes: number, authority: Uint8Array): Promise<Uint8Array> {
    this.maximumFrameBytes = maximumFrameBytes;
    const retained = this.storage.transactionSync(() => {
      this.storage.sql.exec("CREATE TABLE IF NOT EXISTS acyclic_stream_domains (domain TEXT PRIMARY KEY, configuration BLOB NOT NULL, authority BLOB NOT NULL, count INTEGER NOT NULL, bytes INTEGER NOT NULL)");
      this.storage.sql.exec("CREATE TABLE IF NOT EXISTS acyclic_stream_commands (domain TEXT NOT NULL, ordinal INTEGER NOT NULL, length INTEGER NOT NULL, PRIMARY KEY(domain, ordinal))");
      this.storage.sql.exec("CREATE TABLE IF NOT EXISTS acyclic_stream_chunks (domain TEXT NOT NULL, ordinal INTEGER NOT NULL, chunk INTEGER NOT NULL, data BLOB NOT NULL, PRIMARY KEY(domain, ordinal, chunk))");
      this.storage.sql.exec("INSERT INTO acyclic_stream_domains (domain, configuration, authority, count, bytes) VALUES (?, ?, ?, 0, 0) ON CONFLICT(domain) DO NOTHING", this.domain, Uint8Array.from(configuration).buffer, Uint8Array.from(authority).buffer);
      const [row] = this.storage.sql.exec("SELECT configuration, authority FROM acyclic_stream_domains WHERE domain = ?", this.domain);
      if (!row || !equal(bytes(row.configuration), configuration)) {
        throw new Error("STEP sqlite-open failed: journal configuration differs; recovery: reopen with the original stream limits or choose a new domain");
      }
      return bytes(row.authority);
    });
    await this.storage.sync();
    return retained;
  }

  flush(): Promise<void> {
    return this.storage.sync();
  }

  read(from: number, page: number): Uint8Array[] {
    const [metadata] = this.storage.sql.exec("SELECT count, bytes FROM acyclic_stream_domains WHERE domain = ?", this.domain);
    if (!metadata || !Number.isSafeInteger(from) || !Number.isSafeInteger(page) || from < 0 || page <= 0) {
      throw new Error("STEP sqlite-read failed: invalid page; recovery: reopen this domain and request a positive page budget");
    }
    const head = count(metadata.count);
    if (from > head) throw new Error("STEP sqlite-read failed: cursor exceeds journal head; recovery: reopen this domain and replay from its retained cursor");
    const captured = new Uint8Array(16);
    const view = new DataView(captured.buffer);
    view.setBigUint64(0, BigInt(head), true);
    view.setBigUint64(8, BigInt(count(metadata.bytes)), true);
    const frames: Uint8Array[] = [captured];
    for (let index = from; index < Math.min(head, from + page); index += 1) frames.push(this.readFrame(index));
    return frames;
  }

  private readFrame(index: number): Uint8Array {
    const [row] = this.storage.sql.exec("SELECT length FROM acyclic_stream_commands WHERE domain = ? AND ordinal = ?", this.domain, index);
    if (!row) {
      throw new Error("STEP sqlite-replay failed: journal gap; recovery: restore this Durable Object from a verified SQLite backup");
    }
    const length = count(row.length);
    if (length < 18 || length > this.maximumFrameBytes) {
      throw new Error("STEP sqlite-replay failed: invalid frame length; recovery: restore this Durable Object from a verified SQLite backup");
    }
    const output = new Uint8Array(length);
    let offset = 0;
    let chunk = 0;
    for (const part of this.storage.sql.exec("SELECT chunk, data FROM acyclic_stream_chunks WHERE domain = ? AND ordinal = ? ORDER BY chunk", this.domain, index)) {
      const data = bytes(part.data);
      if (offset >= length || count(part.chunk) !== chunk || data.length !== Math.min(CHUNK_BYTES, length - offset)) {
        throw new Error("STEP sqlite-replay failed: invalid chunk; recovery: restore this Durable Object from a verified SQLite backup");
      }
      output.set(data, offset);
      offset += data.length;
      chunk += 1;
    }
    if (offset !== length) {
      throw new Error("STEP sqlite-replay failed: incomplete frame; recovery: restore this Durable Object from a verified SQLite backup");
    }
    return output;
  }

  changed(after: number, ready: () => void): () => void {
    const [metadata] = this.storage.sql.exec("SELECT count FROM acyclic_stream_domains WHERE domain = ?", this.domain);
    if (!metadata) throw new Error("STEP sqlite-changed failed: missing domain; recovery: reopen the original domain");
    if (count(metadata.count) > after) { ready(); return () => {}; }
    const subscriber = { after, ready };
    this.subscribers.add(subscriber);
    return () => { this.subscribers.delete(subscriber); };
  }

  async append(index: number, admit: () => Uint8Array[], changes: readonly StreamChange[]): Promise<number> {
    const accepted = this.storage.transactionSync(() => {
      const [metadata] = this.storage.sql.exec("SELECT count, bytes FROM acyclic_stream_domains WHERE domain = ?", this.domain);
      if (!metadata) throw new Error("STEP sqlite-append failed: missing domain; recovery: reopen the original domain");
      const head = count(metadata.count);
      if (head !== index) return { head, accepted: false };
      const frames = admit();
      if (frames.length === 0) throw new Error("STEP sqlite-append failed: empty batch; recovery: submit the original nonempty command batch");
      let total = count(metadata.bytes);
      let ordinal = index;
      for (const frame of frames) {
        if (frame.length < 18 || frame.length > this.maximumFrameBytes) throw new Error("STEP sqlite-append failed: invalid frame; recovery: reopen this domain and retry the exact original Stream request");
        this.storage.sql.exec("INSERT INTO acyclic_stream_commands (domain, ordinal, length) VALUES (?, ?, ?)", this.domain, ordinal, frame.length);
        for (let offset = 0, chunk = 0; offset < frame.length; offset += CHUNK_BYTES, chunk += 1) {
          const data = frame.slice(offset, offset + CHUNK_BYTES);
          this.storage.sql.exec("INSERT INTO acyclic_stream_chunks (domain, ordinal, chunk, data) VALUES (?, ?, ?, ?)", this.domain, ordinal, chunk, data.buffer);
        }
        ordinal += 1;
        total += frame.length;
      }
      this.storage.sql.exec("UPDATE acyclic_stream_domains SET count = ?, bytes = ? WHERE domain = ?", ordinal, total, this.domain);
      this.appended?.(changes);
      return { head: ordinal, accepted: true };
    });
    if (accepted.accepted) {
      await this.storage.sync();
      for (const subscriber of this.subscribers) {
        if (accepted.head > subscriber.after) { this.subscribers.delete(subscriber); subscriber.ready(); }
      }
    }
    return accepted.head;
  }

}

/** The durable keys beneath the one Objects v1 blob engine, chunked below
 * SQLite's per-row bound. Each write is one transaction, atomic and visible
 * once it returns; `flush` is the only durability wait. Persisted format v1. */
export class SqliteMedium implements WasmObjectsMedium {
  constructor(private readonly storage: SqliteStorage, private readonly domain: string) {
    if (domain.length === 0 || domain.length > 256) throw new Error("STEP sqlite-object-open failed; recovery: choose a domain of 1 to 256 characters");
    storage.transactionSync(() => {
      storage.sql.exec("CREATE TABLE IF NOT EXISTS acyclic_objects_v1_keys (domain TEXT NOT NULL, key TEXT NOT NULL, length INTEGER NOT NULL, PRIMARY KEY(domain, key))");
      storage.sql.exec("CREATE TABLE IF NOT EXISTS acyclic_objects_v1_chunks (domain TEXT NOT NULL, key TEXT NOT NULL, chunk INTEGER NOT NULL, data BLOB NOT NULL, PRIMARY KEY(domain, key, chunk))");
    });
  }

  private length(key: string): number | null {
    const [row] = this.storage.sql.exec("SELECT length FROM acyclic_objects_v1_keys WHERE domain = ? AND key = ?", this.domain, key);
    return row ? count(row.length) : null;
  }

  /** Bytes `start..end` of a `length`-byte value, reading only the chunks they cover. */
  private value(key: string, length: number, start: number, end: number): Uint8Array {
    const output = new Uint8Array(end - start);
    if (start === end) return output;
    const last = Math.floor((end - 1) / CHUNK_BYTES);
    let chunk = Math.floor(start / CHUNK_BYTES);
    for (const row of this.storage.sql.exec("SELECT chunk, data FROM acyclic_objects_v1_chunks WHERE domain = ? AND key = ? AND chunk BETWEEN ? AND ? ORDER BY chunk", this.domain, key, chunk, last)) {
      const data = bytes(row.data), base = chunk * CHUNK_BYTES;
      if (chunk > last || count(row.chunk) !== chunk || data.length !== Math.min(CHUNK_BYTES, length - base)) throw new Error("STEP sqlite-object-read failed: corrupt chunk; recovery: restore this domain from a verified SQLite backup");
      const from = Math.max(start, base), to = Math.min(end, base + data.length);
      output.set(data.subarray(from - base, to - base), from - start);
      chunk += 1;
    }
    if (chunk !== last + 1) throw new Error("STEP sqlite-object-read failed: incomplete value; recovery: restore this domain from a verified SQLite backup");
    return output;
  }

  private write(key: string, value: Uint8Array): void {
    this.storage.sql.exec("INSERT INTO acyclic_objects_v1_keys (domain, key, length) VALUES (?, ?, ?)", this.domain, key, value.length);
    for (let offset = 0, chunk = 0; offset < value.length; offset += CHUNK_BYTES, chunk += 1) {
      this.storage.sql.exec("INSERT INTO acyclic_objects_v1_chunks (domain, key, chunk, data) VALUES (?, ?, ?, ?)", this.domain, key, chunk, value.slice(offset, offset + CHUNK_BYTES).buffer);
    }
  }

  private erase(key: string): void {
    this.storage.sql.exec("DELETE FROM acyclic_objects_v1_chunks WHERE domain = ? AND key = ?", this.domain, key);
    this.storage.sql.exec("DELETE FROM acyclic_objects_v1_keys WHERE domain = ? AND key = ?", this.domain, key);
  }

  private holds(key: string, expected: Uint8Array): boolean {
    const length = this.length(key);
    return length === expected.length && equal(this.value(key, length, 0, length), expected);
  }

  create(key: string, value: Uint8Array): boolean {
    return this.storage.transactionSync(() => {
      if (this.length(key) !== null) return false;
      this.write(key, value);
      return true;
    });
  }

  read(key: string, start: number | null, end: number | null): Uint8Array | null {
    const length = this.length(key);
    if (length === null) return null;
    const from = start ?? 0, to = end ?? length;
    if (!Number.isSafeInteger(from) || !Number.isSafeInteger(to) || from < 0 || from > to || to > length) {
      throw new RangeError("STEP sqlite-object-read failed: range past the value; recovery: read within the stored length");
    }
    return this.value(key, length, from, to);
  }

  remove(key: string): boolean {
    return this.storage.transactionSync(() => {
      if (this.length(key) === null) return false;
      this.erase(key);
      return true;
    });
  }

  replace(key: string, expected: Uint8Array, value: Uint8Array): boolean {
    return this.storage.transactionSync(() => {
      if (!this.holds(key, expected)) return false;
      this.erase(key);
      this.write(key, value);
      return true;
    });
  }

  removeIf(key: string, expected: Uint8Array): boolean {
    return this.storage.transactionSync(() => {
      if (!this.holds(key, expected)) return false;
      this.erase(key);
      return true;
    });
  }

  list(prefix: string, after: string | null, limit: number): [string, number][] {
    // Keys under one prefix are contiguous in SQLite's byte order, so the page
    // ends at the first key outside it.
    const page: [string, number][] = [];
    for (const row of this.storage.sql.exec("SELECT key, length FROM acyclic_objects_v1_keys WHERE domain = ? AND key >= ? AND (? IS NULL OR key > ?) ORDER BY key LIMIT ?", this.domain, prefix, after, after, limit)) {
      if (typeof row.key !== "string") throw new Error("STEP sqlite-object-list failed: invalid key; recovery: restore this domain from a verified SQLite backup");
      if (!row.key.startsWith(prefix)) break;
      page.push([row.key, count(row.length)]);
    }
    return page;
  }

  flush(): Promise<void> { return this.storage.sync(); }
}
