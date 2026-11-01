import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { SqliteEventStorage, type SqliteStorage, type SqlValue } from "../src/cloudflare-storage.js";
import { materializeActorState } from "../../actors/src/state-binding.js";
import { ActorStateStorage } from "../src/actor-state-storage.js";
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

test("SQLite metadata pages retain Unicode prefixes and exact cursors", () => {
  const database = new Database(":memory:");
  try {
    const metadata = storage(database, "unicode"), foreign = storage(database, "foreign");
    const maximum = "\u{10ffff}";
    const keys = ["outside", "page", "page/", "page/a", "page/\0tail", "page/\ud7ff", "page/\ue000",
      "page/\uffff", "page/\ufffftail", "page/\u{10000}", "page/😀", `page/${maximum}`,
      `page/${maximum}tail`, "page0", "尾😀", "尾😀/child", "尾😁", "\uffff", "\ufffftail",
      maximum, `${maximum}tail`, `${maximum}${maximum}`, `${maximum}${maximum}tail`];
    for (const key of [...keys].reverse()) metadata.put(key, `value:${key}`);
    for (const key of keys) foreign.put(key, "foreign");
    const compare = (a: string, b: string) => Buffer.compare(Buffer.from(a), Buffer.from(b));
    const ordered = keys.sort(compare);
    const reopened = storage(database, "unicode");
    for (const prefix of ["", "page/", "page/\0", "page/\ud7ff", "page/\uffff", `page/${maximum}`,
      "尾😀", "\uffff", maximum, maximum + maximum, "missing/"]) {
      for (const limit of [1, 2, 5]) {
        const selected = ordered.filter(key => key.startsWith(prefix));
        let after = "";
        for (let offset = 0; offset < selected.length; offset += limit) {
          const page = reopened.entries(prefix, after, limit);
          expect(page).toEqual(selected.slice(offset, offset + limit).map(key => [key, `value:${key}`]));
          after = page.at(-1)![0];
        }
        expect(reopened.entries(prefix, after, limit)).toEqual([]);
        expect(reopened.entries(prefix, "page/😀", limit)).toEqual(selected
          .filter(key => compare(key, "page/😀") > 0).slice(0, limit).map(key => [key, `value:${key}`]));
      }
    }
  } finally { database.close(); }
});

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

test("host metadata, wake rows and journal frames commit as one actor state packet", async () => {
  const retained: Uint8Array[] = [], commits: { writes: { key: string }[]; deletions: string[]; journalFrom: string; journal: Uint8Array[]; alarm: unknown }[] = [];
  const binding = materializeActorState({
    seed: { metadata: [], journalTail: "0" },
    async commit(request) { commits.push(request); retained.push(...request.journal); },
    async readJournal(from, limit) { return retained.slice(Number(from), Number(from) + limit); },
    fence() {},
  });
  const state = new ActorStateStorage(binding);
  const wakes = state.domain("wakes");
  const journal = state.journal(changes => wakes.put("changed", JSON.stringify(changes)));
  expect(await journal.configure(Uint8Array.of(1), 64, Uint8Array.of(2))).toEqual(Uint8Array.of(2));
  const frame = new Uint8Array(18);
  expect(await journal.append(0, () => [frame], [["inbox", false]])).toBe(1);
  expect(commits).toHaveLength(2);
  expect(commits[1]!.journalFrom).toBe("0");
  expect(commits[1]!.journal).toEqual([frame]);
  expect(commits[1]!.writes.map(write => write.key)).toEqual(["journal/bytes", "wakes/changed"]);
  expect(wakes.get("changed")).toBe(JSON.stringify([["inbox", false]]));
  const [header, ...frames] = await journal.read(0, 8);
  expect(new DataView(header!.buffer).getBigUint64(0, true)).toBe(1n);
  expect(new DataView(header!.buffer).getBigUint64(8, true)).toBe(18n);
  expect(frames).toEqual([frame]);
  // A stale index admits nothing and reports the current head.
  expect(await journal.append(0, () => { throw new Error("admitted at a stale index"); }, [])).toBe(1);
  await expect(journal.configure(Uint8Array.of(9), 64, Uint8Array.of(2))).rejects.toThrow("configuration differs");
  // Metadata insertion order must not change the prefix page or its strict cursor.
  for (const name of ["page/c", "outside", "page/a", "page/b"]) wakes.put(name, name);
  state.domain("other").put("page/a", "foreign");
  await wakes.flush();
  expect(wakes.entries("page/", "", 2)).toEqual([["page/a", "page/a"], ["page/b", "page/b"]]);
  expect(wakes.entries("page/", "page/b", 2)).toEqual([["page/c", "page/c"]]);
  expect(() => wakes.entries("", "", 0)).toThrow("page size");
  wakes.delete("page/b");
  expect(() => wakes.transaction(() => {
    wakes.delete("page/a");
    wakes.put("page/b", "refused");
    throw new Error("refused metadata transaction");
  })).toThrow("refused metadata transaction");
  await wakes.flush();
  expect(commits.at(-1)!.deletions).toEqual(["wakes/page/b"]);
  expect(new ActorStateStorage(binding).domain("wakes").entries("page/", "", 2)).toEqual([
    ["page/a", "page/a"], ["page/c", "page/c"],
  ]);
  // A reopened adapter keeps every domain's deadline: one domain's alarm cannot drop another's.
  await wakes.alarm(100);
  await new ActorStateStorage(binding).domain("actor").alarm(200);
  expect(commits.at(-1)).toMatchObject({ alarm: { type: "set", deadline: "100" } });
});
