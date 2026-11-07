import type { Database } from "bun:sqlite";
import type { SqlValue, SqliteStorage } from "../../src/cloudflare-storage.js";

/** Synchronous SQLite transactions complete durability before their flush promise. */
export function sqlite(database: Database) {
  const storage = {
    barriers: 0,
    sql: { exec<Row extends Record<string, SqlValue>>(query: string, ...bindings: SqlValue[]): Iterable<Row> {
      return database.query(query).all(...bindings.map(value => value instanceof ArrayBuffer ? new Uint8Array(value) : value)).map(row =>
        Object.fromEntries(Object.entries(row as Record<string, unknown>).map(([key, value]) => [key, value instanceof Uint8Array ? value.slice().buffer : value])) as Row);
    } },
    transactionSync<T>(operation: () => T): T { return database.transaction(operation)(); },
    async sync() { storage.barriers++; },
  } satisfies SqliteStorage & { barriers: number };
  return storage;
}
