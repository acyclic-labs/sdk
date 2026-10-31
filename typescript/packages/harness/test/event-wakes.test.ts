import { Database } from "bun:sqlite";
import { test } from "bun:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SqliteEventStorage, type SqlValue } from "../src/cloudflare-storage.js";
import { WakeIndex } from "../src/event-storage.js";

test("durable wakes retain exact generations and visit only selected membership", async () => {
  const directory = mkdtempSync(join(tmpdir(), "event-wakes-"));
  const file = join(directory, "metadata.sqlite");
  let database = new Database(file);
  let reads: string[] = [], writes = 0, scans = 0, forbidScans = false;
  const backend = {
    sql: { exec<Row extends Record<string, SqlValue>>(query: string, ...bindings: SqlValue[]): Iterable<Row> {
      if (query.startsWith("SELECT value")) reads.push(String(bindings[1]));
      if (query.startsWith("INSERT")) ++writes;
      if (query.startsWith("SELECT key")) {
        ++scans;
        assert.equal(forbidScans, false, "wake work must not scan all Actor registrations");
      }
      return database.query(query).all(...bindings.map(value => value instanceof ArrayBuffer ? new Uint8Array(value) : value)) as Row[];
    } },
    transactionSync<T>(operation: () => T): T { return database.transaction(operation)(); },
    async sync() { database.exec("PRAGMA wal_checkpoint(FULL)"); },
    async setAlarm() {}, async deleteAlarm() {},
  };
  const storage = new SqliteEventStorage(backend, "wakes");
  const open = () => new WakeIndex(storage, 1024);
  const change = (index: WakeIndex, source: string, changes: Parameters<WakeIndex["changed"]>[1]) =>
    storage.transaction(() => index.changed(source, changes));
  try {
    let index = open();
    assert.equal(scans, 1); // Only a new domain checks for an unsupported old format.
    forbidScans = true;
    assert.deepEqual(await index.register("root", ["tree/root/inbox", "tree/root/control"]), { admitted: true });
    assert.deepEqual(await index.register("child", ["tree/child/inbox", "tree/child/control"]), { admitted: true });
    assert.deepEqual(await index.register("leaf", ["tree/child/leaf/inbox", "tree/child/leaf/control"]), { admitted: true });
    const costs: { register: number; exact: number; subtree: number; pending: number }[] = [];
    let population = 0;
    for (const total of [1, 16, 256]) {
      for (; population < total; ++population) {
        assert.deepEqual(await index.register(`unrelated-${population}`,
          [`unrelated/${population}/inbox`, `unrelated/${population}/control`]), { admitted: true });
      }
      reads = [];
      assert.deepEqual(await index.register("root", ["tree/root/control", "tree/root/inbox"]), { admitted: true });
      const register = reads.length;
      reads = [];
      change(index, "root", [["tree/child/inbox", false], ["tree/child/control", false]]);
      const exact = reads.length;
      assert.equal(reads.some(key => key.startsWith("actor:unrelated-")), false);
      const first = index.pending();
      assert.equal(first.length, 1);
      assert.equal(first[0]![0], "child");
      index.acknowledge(...first[0]!);
      reads = [];
      change(index, "root", [["tree/child", true], ["tree/child/leaf", true]]);
      const subtree = reads.length;
      reads = [];
      const pending = index.pending();
      const pendingReads = reads.length;
      assert.deepEqual(new Set(pending.map(([actor]) => actor)), new Set(["child", "leaf"]));
      for (const [actor, generation] of pending) index.acknowledge(actor, generation);
      costs.push({ register, exact, subtree, pending: pendingReads });
    }
    assert.deepEqual(costs[1], costs[0]);
    assert.deepEqual(costs[2], costs[0]);
    // Two selected paths and overlapping changes advance an Actor once per publication.
    change(index, "root", [["tree/child", true], ["tree/child/control", false]]);
    const before = new Map(index.pending());
    assert.equal(before.get("child"), 7);
    assert.equal(before.get("leaf"), 4);
    assert.throws(() => storage.transaction(() => {
      index.changed("root", [["tree/child/inbox", false]]);
      throw new Error("abort original publication");
    }), /abort original publication/);
    assert.deepEqual(new Map(index.pending()), before);

    // Lose the owner before delivery acknowledgement, then recover from actual storage.
    database.close();
    database = new Database(file);
    index = open();
    assert.deepEqual(new Map(index.pending()), before);
    change(index, "root", [["tree/child/inbox", false]]);
    index.acknowledge("child", before.get("child")!);
    assert.equal(index.pendingCount(), 2, "a stale delivery cannot erase a later publication");
    change(index, "child", [["tree/child/inbox", false]]);
    assert.equal(new Map(index.pending()).get("child"), before.get("child")! + 1);
    change(index, "", [["tree/root/inbox", false]]);
    const all = new Map(index.pending());
    // Exercise middle, first and last unlink without touching unrelated Actors.
    index.acknowledge("leaf", all.get("leaf")!);
    index.acknowledge("child", all.get("child")!);
    index.acknowledge("root", all.get("root")!);
    assert.deepEqual(index.pending(), []);
    assert.equal(index.pendingCount(), 0);

    const unchanged = writes;
    assert.deepEqual(await index.register("root", ["tree/other/inbox", "tree/other/control"]), { refused: "conflict" });
    assert.deepEqual(await index.register("foreign", ["tree/child/inbox", "foreign/control"]), { refused: "conflict" });
    assert.deepEqual(await new WakeIndex(storage, 1).register("over-capacity", ["new/inbox", "new/control"]), { refused: "capacity" });
    assert.equal(writes, unchanged);
    // Path segments remain exact, including prefixes ending at an empty segment.
    await index.register("slash", ["/inbox", "/control"]);
    change(index, "root", [["", true], ["tree/childish", true]]);
    assert.deepEqual(index.pending(), [["slash", 1]]);
    index.acknowledge("slash", 1);

    change(index, "root", [["tree/child/inbox", false]]);
    const row = JSON.parse(storage.get("actor:child")!);
    storage.put("actor:child", JSON.stringify({ ...row, next: "child" }));
    assert.throws(() => index.pending(), { code: "data_loss" });
    storage.put("actor:child", JSON.stringify(row));
    storage.put("index", JSON.stringify({ format: "acyclic.host.wakes/v0" }));
    assert.throws(open, { code: "failed_precondition" });
    forbidScans = false;
    const legacy = new SqliteEventStorage(backend, "unversioned");
    legacy.put("child", JSON.stringify(row));
    assert.throws(() => new WakeIndex(legacy, 1024), { code: "failed_precondition" });
    assert.equal(legacy.get("index"), undefined);
  } finally { database.close(); rmSync(directory, { recursive: true, force: true }); }
});
