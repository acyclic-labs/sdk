import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { SqliteEventStorage, type SqliteStorage, type SqlValue } from "../src/cloudflare-storage.js";
import { WakeIndex } from "../src/event-storage.js";

function storage(database: Database, domain: string) {
  return new SqliteEventStorage({
    sql: {
      exec<Row extends Record<string, SqlValue>>(query: string, ...bindings: SqlValue[]): Iterable<Row> {
        return database.query(query).all(...bindings.map(value =>
          value instanceof ArrayBuffer ? new Uint8Array(value) : value)) as Row[];
      },
    },
    transactionSync: <T>(operation: () => T): T => database.transaction(operation)(),
    sync: async () => {},
    setAlarm: async (_time: number) => {},
    deleteAlarm: async () => {},
  } satisfies SqliteStorage & { setAlarm(time: number): Promise<void>; deleteAlarm(): Promise<void> }, domain);
}

test("SQLite wake routing pages actual changed owners and preserves later generations through reopen", async () => {
  const database = new Database(":memory:");
  try {
    const metadata = storage(database, "wakes");
    let wakes = new WakeIndex(metadata);
    let reads = 0;
    let pages = 0;
    const get = metadata.get.bind(metadata);
    const entries = metadata.entries.bind(metadata);
    metadata.get = key => { reads++; return get(key); };
    metadata.entries = (prefix, after, limit) => { pages++; return entries(prefix, after, limit); };
    const changed = (path: string) => {
      reads = 0;
      pages = 0;
      metadata.transaction(() => wakes.changed("", [[path, false]]));
      return { reads, pages };
    };
    const names = Array.from({ length: 600 }, (_, index) => `actor-${index}${index === 0 ? "a".repeat(1200) : ""}`);
    const paths = (index: number) => [`root/${index}/${"x".repeat(index === 0 ? 4200 : 1)}`, `root/${index}/inbox`];
    expect(await wakes.register(names[0]!, paths(0))).toEqual({ admitted: true });
    const single = changed(paths(0)[1]!);
    expect(single.pages).toBe(0);
    wakes.acknowledge(names[0]!, 1);
    for (let index = 1; index < names.length; index++) {
      expect(await wakes.register(names[index]!, paths(index))).toEqual({ admitted: true });
    }
    // Exact-path work is independent of all other registered actors.
    expect(changed(paths(0)[1]!)).toEqual(single);
    wakes.acknowledge(names[0]!, 2);
    expect(await wakes.register("collision", paths(0))).toEqual({ refused: "conflict" });
    expect(await wakes.register(names[0]!, paths(1))).toEqual({ refused: "conflict" });
    // Durable change identities remain distinct beyond Number's exact range.
    metadata.put("change", JSON.stringify("9007199254740992"));
    metadata.transaction(() => wakes.changed(names[0]!, [["root", true], ["root/1/inbox", false]]));
    wakes = new WakeIndex(storage(database, "wakes"));
    expect(wakes.pendingCount()).toBe(599);
    const observed = new Set<string>();
    for (const [actor, generation] of wakes.pending()) {
      expect(generation).toBe(1);
      expect(observed.has(actor)).toBe(false);
      observed.add(actor);
      if (actor === names[1]) metadata.transaction(() => wakes.changed("", [["root/1/inbox", false]]));
      wakes.acknowledge(actor, generation);
    }
    expect(observed.size).toBe(599);
    expect(wakes.pendingCount()).toBe(1);
    expect([...wakes.pending()]).toEqual([[names[1]!, 2]]);
    wakes.acknowledge(names[1]!, 2);
    expect(wakes.pendingCount()).toBe(0);
    expect([...wakes.pending()]).toEqual([]);
    metadata.transaction(() => wakes.changed("", [["roo", true]]));
    expect(wakes.pendingCount()).toBe(0);
  } finally { database.close(); }
});

test("an explicit finite wake capacity preserves replay and refuses only new placement", async () => {
  const database = new Database(":memory:");
  try {
    const wakes = new WakeIndex(storage(database, "finite"), 1);
    expect(await wakes.register("owner", ["state", "inbox"])).toEqual({ admitted: true });
    expect(await wakes.register("owner", ["state", "inbox"])).toEqual({ admitted: true });
    expect(await wakes.register("other", ["other/state", "other/inbox"])).toEqual({ refused: "capacity" });
    expect(await wakes.register("other", ["same", "same"])).toEqual({ refused: "invalid_argument" });
  } finally { database.close(); }
});
