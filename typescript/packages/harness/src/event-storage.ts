/**
 * Minimal synchronous metadata storage for an event-owned host. Journal wake
 * writes share the journal append's transaction; flush resolves after durability.
 */
export interface EventStorage {
  get(key: string): string | undefined;
  put(key: string, value: string): void;
  delete(key: string): void;
  /** A key-ordered page, strictly after the cursor and restricted to the prefix. */
  entries(prefix: string, after: string, limit: number): [string, string][];
  transaction<T>(operation: () => T): T;
  flush(): Promise<void>;
  alarm(time: number | undefined): Promise<void>;
}

export type StreamChange = readonly [path: string, subtree: boolean];
/** Definitive placement decisions; rejected transport/durability promises are unknown. */
export type Placement = { admitted: true } | { refused: "invalid_argument" | "conflict" | "capacity" };
interface Registration { paths: string[]; generation: number; pending: boolean }
const PAGE_SIZE = 256;

// Fixed-width code units preserve every JavaScript string and its prefixes.
// Index keys are ASCII, independent of a backing store's Unicode collation.
function indexKey(value: string): string {
  let key = "";
  for (let index = 0; index < value.length; index++) key += value.charCodeAt(index).toString(16).padStart(4, "0");
  return key;
}

export function decodeMetadata(value: string): unknown {
  try { return JSON.parse(value); }
  catch { throw new Error("STEP host-metadata-read failed; recovery: restore host metadata from a verified backup"); }
}

/** Shared durable wake routing; storage never interprets paths or generations. */
export class WakeIndex {
  constructor(private readonly storage: EventStorage, private readonly maximumActors?: number) {
    if (maximumActors !== undefined && (!Number.isSafeInteger(maximumActors) || maximumActors <= 0)) {
      throw { code: "invalid_argument", step: "actor-wake-open", recovery: "omit capacity or supply a positive safe integer" };
    }
  }

  private decode(value: string): Registration {
    const row = decodeMetadata(value);
    if (typeof row !== "object" || row === null || !("paths" in row) || !Array.isArray(row.paths)
      || row.paths.length !== 2 || row.paths.some(path => typeof path !== "string" || !path)
      || new Set(row.paths).size !== 2
      || !("generation" in row) || typeof row.generation !== "number" || !Number.isSafeInteger(row.generation)
      || row.generation < 0 || !("pending" in row) || typeof row.pending !== "boolean") {
      throw new Error("STEP actor-wake-read failed; recovery: restore host metadata from a verified backup");
    }
    return row as Registration;
  }

  private *entries(prefix: string): IterableIterator<[string, string]> {
    let after = prefix;
    for (;;) {
      const page = this.storage.entries(prefix, after, PAGE_SIZE);
      if (page.length === 0) return;
      for (const entry of page) yield entry;
      after = page[page.length - 1]![0];
    }
  }

  private count(key: string): number {
    const value = this.storage.get(key);
    if (value === undefined) return 0;
    const count = decodeMetadata(value);
    if (typeof count !== "number" || !Number.isSafeInteger(count) || count < 0) {
      throw new Error("STEP actor-wake-read failed: invalid count; recovery: restore host metadata from a verified backup");
    }
    return count;
  }

  private adjust(key: string, change: number): void {
    const count = this.count(key) + change;
    if (!Number.isSafeInteger(count) || count < 0) {
      throw new Error("STEP actor-wake failed: invalid count; recovery: restore host metadata from a verified backup");
    }
    this.storage.put(key, JSON.stringify(count));
  }

  async register(actor: string, paths: string[]): Promise<Placement> {
    if (!actor || paths.length !== 2 || new Set(paths).size !== 2 || paths.some(path => !path)) {
      return { refused: "invalid_argument" };
    }
    const result = this.storage.transaction<Placement>(() => {
      const key = `actor/${indexKey(actor)}`;
      const value = this.storage.get(key);
      const existing = value === undefined ? undefined : this.decode(value);
      if ((existing && existing.paths.some(path => !paths.includes(path)))
        || paths.some(path => {
          const owner = this.storage.get(`path/${indexKey(path)}`);
          return owner !== undefined && owner !== actor;
        })) {
        return { refused: "conflict" };
      }
      if (!existing && this.maximumActors !== undefined && this.count("count/actors") >= this.maximumActors) {
        return { refused: "capacity" };
      }
      if (!existing) {
        this.storage.put(key, JSON.stringify({ paths, generation: 0, pending: false }));
        for (const path of paths) this.storage.put(`path/${indexKey(path)}`, actor);
        this.adjust("count/actors", 1);
      }
      return { admitted: true };
    });
    if ("admitted" in result) await this.storage.flush();
    return result;
  }

  /** Called inside the journal append's storage transaction. */
  changed(source: string, changes: readonly StreamChange[]): void {
    const changed = new Set<string>();
    const mark = (actor: string) => {
      if (actor === source || changed.has(actor)) return;
      changed.add(actor);
      const value = this.storage.get(`actor/${indexKey(actor)}`);
      if (value === undefined) throw new Error("STEP actor-wake-read failed: missing owner; recovery: restore host metadata from a verified backup");
      const row = this.decode(value);
      if (!Number.isSafeInteger(row.generation + 1)) {
        throw new Error("STEP actor-wake failed: generation exhausted; recovery: move actors to a new consistency domain");
      }
      this.storage.put(`actor/${indexKey(actor)}`, JSON.stringify({ ...row, generation: row.generation + 1, pending: true }));
      this.storage.put(`pending/${indexKey(actor)}`, actor);
      if (!row.pending) this.adjust("count/pending", 1);
    };
    for (const [path, subtree] of changes) {
      const actor = this.storage.get(`path/${indexKey(path)}`);
      if (actor !== undefined) mark(actor);
      if (subtree) for (const [, owner] of this.entries(`path/${indexKey(`${path}/`)}`)) mark(owner);
    }
  }

  pendingCount(): number { return this.count("count/pending"); }

  *pending(): IterableIterator<[string, number]> {
    for (const [, actor] of this.entries("pending/")) {
      const value = this.storage.get(`actor/${indexKey(actor)}`);
      if (value === undefined) throw new Error("STEP actor-wake-read failed: missing pending owner; recovery: restore host metadata from a verified backup");
      const row = this.decode(value);
      if (row.pending) yield [actor, row.generation];
    }
  }

  acknowledge(actor: string, generation: number): void {
    this.storage.transaction(() => {
      const key = `actor/${indexKey(actor)}`;
      const value = this.storage.get(key);
      if (value === undefined) return;
      const row = this.decode(value);
      if (row.pending && row.generation === generation) {
        this.storage.put(key, JSON.stringify({ ...row, pending: false }));
        this.storage.delete(`pending/${indexKey(actor)}`);
        this.adjust("count/pending", -1);
      }
    });
  }
}
