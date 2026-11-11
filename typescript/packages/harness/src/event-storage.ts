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
export { WakeIndex } from "./event-wakes.js";

/** A page is a positive finite chunk, never SQLite's negative unlimited sentinel. */
export function checkMetadataPage(limit: number): void {
  if (!Number.isSafeInteger(limit) || limit <= 0) {
    throw new RangeError("STEP host-metadata-page failed; recovery: supply a positive safe page size");
  }
}

/** Lazily visit bounded metadata pages; the chunk never limits total rows. */
export function* metadataPages(storage: EventStorage, prefix: string, limit: number): Generator<[string, string][]> {
  checkMetadataPage(limit);
  let after = "";
  for (;;) {
    const page = storage.entries(prefix, after, limit);
    if (page.length === 0) return;
    after = page[page.length - 1]![0];
    yield page;
    if (page.length < limit) return;
  }
}

export function decodeMetadata(value: string): unknown {
  try { return JSON.parse(value); }
  catch { throw new Error("STEP host-metadata-read failed; recovery: restore host metadata from a verified backup"); }
}
