import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { ErrorCode, ObjectsV1Provider, blobDigest, verified } from "@acyclic-labs/objects";
import { WasmObjects } from "../generated/wasm/acyclic_harness_wasm.js";
import { SqliteMedium, type SqlValue, type SqliteStorage } from "../src/cloudflare-storage.js";

/** Durable Objects SQLite semantics over bun:sqlite, counting durability waits. */
function sqlite(database: Database) {
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

/** The typed client over any port, verifying each reply by the Rust rules. */
class Client extends ObjectsV1Provider {
  constructor(private readonly port: WasmObjects) { super(); }
  open(route: string, input: Uint8Array) { return this.port.open(route, input); }
}

const account = new TextEncoder().encode("filesystem");

test("SQLite keys hold one Objects v1 account under the shared engine, durable across reopen", async () => {
  const database = new Database(":memory:");
  const storage = sqlite(database);
  const open = (domain: string) => new WasmObjects({ medium: new SqliteMedium(storage, domain), account });
  const first = open("filesystem");
  try {
    const objects = new Client(first);
    // Spans three SQLite chunks, so ranged reads cross chunk boundaries.
    const body = Uint8Array.from({ length: 300 * 1024 }, (_, index) => index % 251);
    const digest = await blobDigest(body);
    const before = storage.barriers;
    expect(verified(await objects.put(body))?.digest).toEqual(digest);
    expect(storage.barriers).toBeGreaterThan(before);
    expect(verified(await objects.put(body))?.digest).toEqual(digest);
    expect((await objects.head(digest)).length).toBe(BigInt(body.length));
    const reads = storage.barriers;
    const start = 128 * 1024 - 7, end = 256 * 1024 + 9;
    const ranged = await objects.get({ digest, range: { start: BigInt(start), end: BigInt(end) }, maximumBytes: BigInt(end - start) });
    expect(ranged.body).toEqual(body.subarray(start, end));
    // Reads never wait for durability.
    expect(storage.barriers).toBe(reads);

    const reopened = new Client(open("filesystem"));
    expect((await reopened.get({ digest, range: { start: 0n }, maximumBytes: BigInt(body.length) })).body).toEqual(body);
    const other = new Client(open("other"));
    await expect(other.head(digest)).rejects.toMatchObject({ code: ErrorCode.NOT_FOUND });
  } finally { first.free(); database.close(); }
});

test("the medium refuses corrupt chunks and ranges past a value", () => {
  const database = new Database(":memory:");
  try {
    const storage = sqlite(database);
    const medium = new SqliteMedium(storage, "filesystem");
    expect(medium.create("k", Uint8Array.of(1, 2, 3))).toBe(true);
    expect(medium.create("k", Uint8Array.of(9))).toBe(false);
    expect(() => medium.read("k", 1, 4)).toThrow("range past the value");
    expect(medium.replace("k", Uint8Array.of(9), Uint8Array.of(4))).toBe(false);
    expect(medium.replace("k", Uint8Array.of(1, 2, 3), Uint8Array.of(4))).toBe(true);
    expect(medium.list("", null, 10)).toEqual([["k", 1]]);
    database.run("UPDATE acyclic_objects_v1_chunks SET data = x'0102' WHERE key = 'k'");
    expect(() => medium.read("k", null, null)).toThrow("corrupt chunk");
    expect(() => medium.removeIf("k", Uint8Array.of(4))).toThrow("corrupt chunk");
  } finally { database.close(); }
});
