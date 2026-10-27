/**
 * Minimal synchronous metadata storage for an event-owned host. Journal wake
 * writes share the journal append's transaction; flush resolves after durability.
 */
export interface EventStorage {
  get(key: string): string | undefined;
  put(key: string, value: string): void;
  entries(): [string, string][];
  transaction<T>(operation: () => T): T;
  flush(): Promise<void>;
  alarm(time: number | undefined): Promise<void>;
}

export type StreamChange = readonly [path: string, subtree: boolean];
/** Definitive placement decisions; rejected transport/durability promises are unknown. */
export type Placement = { admitted: true } | { refused: "invalid_argument" | "conflict" | "capacity" };
interface Registration { paths: string[]; generation: number; pending: boolean }

export function decodeMetadata(value: string): unknown {
  try { return JSON.parse(value); }
  catch { throw new Error("STEP host-metadata-read failed; recovery: restore host metadata from a verified backup"); }
}

/** Shared durable wake routing; storage never interprets paths or generations. */
export class WakeIndex {
  constructor(private readonly storage: EventStorage, private readonly maximumActors: number) {
    if (!Number.isSafeInteger(maximumActors) || maximumActors <= 0 || maximumActors > 0xffff_ffff) {
      throw { code: "invalid_argument", step: "actor-wake-open", recovery: "supply the journal's positive u32 path capacity" };
    }
  }

  private decode(value: string): Registration {
    const row = decodeMetadata(value);
    if (typeof row !== "object" || row === null || !("paths" in row) || !Array.isArray(row.paths)
      || row.paths.length !== 2 || row.paths.some(path => typeof path !== "string" || !path || path.length > 4096)
      || !("generation" in row) || typeof row.generation !== "number" || !Number.isSafeInteger(row.generation)
      || row.generation < 0 || !("pending" in row) || typeof row.pending !== "boolean") {
      throw new Error("STEP actor-wake-read failed; recovery: restore host metadata from a verified backup");
    }
    return row as Registration;
  }

  private rows(): [string, Registration][] {
    return this.storage.entries().map(([actor, value]) => [actor, this.decode(value)]);
  }

  async register(actor: string, paths: string[]): Promise<Placement> {
    if (!actor || new TextEncoder().encode(actor).length > 1024 || paths.length !== 2
      || new Set(paths).size !== 2 || paths.some(path => !path || path.length > 4096)) {
      return { refused: "invalid_argument" };
    }
    const result = this.storage.transaction<Placement>(() => {
      const rows = this.rows();
      const existing = rows.find(([name]) => name === actor)?.[1];
      if ((existing && existing.paths.some(path => !paths.includes(path)))
        || rows.some(([name, row]) => name !== actor && row.paths.some(path => paths.includes(path)))) {
        return { refused: "conflict" };
      }
      if (!existing && rows.length >= this.maximumActors) {
        return { refused: "capacity" };
      }
      this.storage.put(actor, JSON.stringify(existing ?? { paths, generation: 0, pending: false }));
      return { admitted: true };
    });
    if ("admitted" in result) await this.storage.flush();
    return result;
  }

  /** Called inside the journal append's storage transaction. */
  changed(source: string, changes: readonly StreamChange[]): void {
    for (const [actor, row] of this.rows()) {
      if (actor === source || !changes.some(([path, subtree]) => row.paths.some(owned =>
        owned === path || (subtree && owned.startsWith(`${path}/`))))) continue;
      if (!Number.isSafeInteger(row.generation + 1)) {
        throw new Error("STEP actor-wake failed: generation exhausted; recovery: move actors to a new consistency domain");
      }
      this.storage.put(actor, JSON.stringify({ ...row, generation: row.generation + 1, pending: true }));
    }
  }

  pending(): [string, number][] {
    return this.rows().filter(([, row]) => row.pending).map(([actor, row]) => [actor, row.generation]);
  }

  acknowledge(actor: string, generation: number): void {
    const value = this.storage.get(actor);
    if (value === undefined) return;
    const row = this.decode(value);
    if (row.generation === generation) this.storage.put(actor, JSON.stringify({ ...row, pending: false }));
  }
}
