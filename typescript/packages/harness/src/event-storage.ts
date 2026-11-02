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

export function decodeMetadata(value: string): unknown {
  try { return JSON.parse(value); }
  catch { throw new Error("STEP host-metadata-read failed; recovery: restore host metadata from a verified backup"); }
}
