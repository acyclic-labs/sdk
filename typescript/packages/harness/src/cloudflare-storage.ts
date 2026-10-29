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
      this.storage.sql.exec("CREATE TABLE IF NOT EXISTS acyclic_stream_domains (domain TEXT PRIMARY KEY, configuration BLOB NOT NULL, count INTEGER NOT NULL, bytes INTEGER NOT NULL)");
      this.storage.sql.exec("CREATE TABLE IF NOT EXISTS acyclic_stream_authorities (domain TEXT PRIMARY KEY, authority BLOB NOT NULL)");
      this.storage.sql.exec("CREATE TABLE IF NOT EXISTS acyclic_stream_commands (domain TEXT NOT NULL, ordinal INTEGER NOT NULL, length INTEGER NOT NULL, PRIMARY KEY(domain, ordinal))");
      this.storage.sql.exec("CREATE TABLE IF NOT EXISTS acyclic_stream_chunks (domain TEXT NOT NULL, ordinal INTEGER NOT NULL, chunk INTEGER NOT NULL, data BLOB NOT NULL, PRIMARY KEY(domain, ordinal, chunk))");
      this.storage.sql.exec("INSERT INTO acyclic_stream_domains (domain, configuration, count, bytes) VALUES (?, ?, 0, 0) ON CONFLICT(domain) DO NOTHING", this.domain, Uint8Array.from(configuration).buffer);
      const [row] = this.storage.sql.exec("SELECT configuration FROM acyclic_stream_domains WHERE domain = ?", this.domain);
      if (!row || !equal(bytes(row.configuration), configuration)) {
        throw new Error("STEP sqlite-open failed: journal configuration differs; recovery: reopen with the original stream limits or choose a new domain");
      }
      this.storage.sql.exec("INSERT INTO acyclic_stream_authorities (domain, authority) VALUES (?, ?) ON CONFLICT(domain) DO NOTHING", this.domain, Uint8Array.from(authority).buffer);
      const [owner] = this.storage.sql.exec("SELECT authority FROM acyclic_stream_authorities WHERE domain = ?", this.domain);
      return bytes(owner?.authority);
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

function objectKey(value: Uint8Array): string {
  if (value.length !== 33) throw new Error("STEP sqlite-object-key failed; recovery: supply the canonical 33-byte filesystem ObjectId");
  return Array.from(value, byte => byte.toString(16).padStart(2, "0")).join("");
}

/** Immutable, chunked filesystem objects; put resolves only after durability. */
export class SqliteObjects {
  constructor(private readonly storage: SqliteStorage, private readonly domain: string) {
    if (domain.length === 0 || domain.length > 256) throw new Error("STEP sqlite-object-open failed; recovery: choose a domain of 1 to 256 characters");
    storage.transactionSync(() => {
      storage.sql.exec("CREATE TABLE IF NOT EXISTS acyclic_objects (domain TEXT NOT NULL, key TEXT NOT NULL, length INTEGER NOT NULL, PRIMARY KEY(domain, key))");
      storage.sql.exec("CREATE TABLE IF NOT EXISTS acyclic_object_chunks (domain TEXT NOT NULL, key TEXT NOT NULL, chunk INTEGER NOT NULL, data BLOB NOT NULL, PRIMARY KEY(domain, key, chunk))");
    });
  }

  async length(key: Uint8Array): Promise<number | null> {
    await this.storage.sync();
    const [row] = this.storage.sql.exec("SELECT length FROM acyclic_objects WHERE domain = ? AND key = ?", this.domain, objectKey(key));
    return row ? count(row.length) : null;
  }

  async read(key: Uint8Array, maximum: bigint): Promise<Uint8Array | null> {
    const length = await this.length(key);
    if (length === null) return null;
    if (BigInt(length) > maximum) throw new Error("STEP sqlite-object-read failed: size exceeds admission; recovery: repeat the read with an admitted maximum that covers the object");
    const output = new Uint8Array(length);
    let offset = 0;
    let chunk = 0;
    for (const row of this.storage.sql.exec("SELECT chunk, data FROM acyclic_object_chunks WHERE domain = ? AND key = ? ORDER BY chunk", this.domain, objectKey(key))) {
      const data = bytes(row.data);
      if (offset >= length || count(row.chunk) !== chunk || data.length !== Math.min(CHUNK_BYTES, length - offset)) throw new Error("STEP sqlite-object-read failed: corrupt chunk; recovery: restore this domain from a verified SQLite backup");
      output.set(data, offset);
      offset += data.length;
      chunk += 1;
    }
    if (offset !== length) throw new Error("STEP sqlite-object-read failed: incomplete object; recovery: restore this domain from a verified SQLite backup");
    return output;
  }

  async put(keys: Uint8Array[], values: Uint8Array[]): Promise<void> {
    if (keys.length === 0 || keys.length !== values.length) throw new Error("STEP sqlite-object-put failed: invalid batch; recovery: supply one value per canonical ObjectId");
    this.storage.transactionSync(() => {
      for (let index = 0; index < keys.length; index += 1) {
        const key = objectKey(keys[index]!);
        const value = values[index]!;
        const [old] = this.storage.sql.exec("SELECT length FROM acyclic_objects WHERE domain = ? AND key = ?", this.domain, key);
        if (old) {
          if (count(old.length) !== value.length) throw new Error("STEP sqlite-object-put failed: immutable size changed; recovery: restore this domain from a verified SQLite backup");
          let offset = 0;
          let chunk = 0;
          for (const row of this.storage.sql.exec("SELECT chunk, data FROM acyclic_object_chunks WHERE domain = ? AND key = ? ORDER BY chunk", this.domain, key)) {
            const data = bytes(row.data);
            if (offset >= value.length || count(row.chunk) !== chunk || data.length !== Math.min(CHUNK_BYTES, value.length - offset) || !equal(data, value.subarray(offset, offset + data.length))) throw new Error("STEP sqlite-object-put failed: immutable bytes changed; recovery: restore this domain from a verified SQLite backup");
            offset += data.length;
            chunk += 1;
          }
          if (offset !== value.length) throw new Error("STEP sqlite-object-put failed: immutable value incomplete; recovery: restore this domain from a verified SQLite backup");
          continue;
        }
        this.storage.sql.exec("INSERT INTO acyclic_objects (domain, key, length) VALUES (?, ?, ?)", this.domain, key, value.length);
        for (let offset = 0, chunk = 0; offset < value.length; offset += CHUNK_BYTES, chunk += 1) {
          this.storage.sql.exec("INSERT INTO acyclic_object_chunks (domain, key, chunk, data) VALUES (?, ?, ?, ?)", this.domain, key, chunk, value.slice(offset, offset + CHUNK_BYTES).buffer);
        }
      }
    });
    await this.storage.sync();
  }
}
