import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import type { ActorStateBinding, ActorStateHost, semantic } from "@acyclic-labs/actors";
import type { GuestAdmissionHost } from "@acyclic-labs/auth";
import type { ImmutableObjects } from "@acyclic-labs/objects";
import { materializeBindings } from "../src/bindings.js";
import { SqliteObjects, type SqliteStorage } from "../../harness/src/cloudflare-storage.js";

const base = { name: "binding", capability: "", resource: "", targetAccount: new Uint8Array() };
const transports = {
  namespace(): never { throw new Error("unexpected namespace binding"); },
  service(): never { throw new Error("unexpected service binding"); },
};

test("stateTransaction aliases stage into the same original commit", async () => {
  const commits: Parameters<ActorStateHost["commit"]>[0][] = [];
  const host: ActorStateHost = {
    seed: { metadata: [], journalTail: "0" },
    async commit(request) { commits.push(request); },
    async readJournal() { return []; },
    fence(reason) { throw reason; },
  };
  const env = await materializeBindings([
    { ...base, name: "first", stateTransaction: true },
    { ...base, name: "second", stateTransaction: true },
  ], { ...transports, stateTransaction: host });
  const first = env.first as ActorStateBinding, second = env.second as ActorStateBinding;
  expect(first).toBe(second);
  first.transaction(state => { state.put("key", new Uint8Array([7])); });
  expect(second.transaction(state => state.get("key"))).toEqual(new Uint8Array([7]));
  expect(commits).toHaveLength(0);
  await second.flush();
  expect(commits).toHaveLength(1);
  await expect(materializeBindings([{ ...base, stateTransaction: true }], transports)).rejects.toBeInstanceOf(TypeError);
});

test("guestAdmission retains original projections and current refusal", async () => {
  const platform = Object.freeze({ authority: "platform" }), access = Object.freeze({ authority: "host" });
  const stopped = new TypeError("admission stopped");
  let revoked = false;
  class Host implements GuestAdmissionHost<typeof platform, typeof access> {
    binding(kind: "platformAuthority"): typeof platform;
    binding(kind: "hostAccess"): typeof access;
    binding(kind: "platformAuthority" | "hostAccess") {
      if (revoked) throw stopped;
      return kind === "platformAuthority" ? platform : access;
    }
  }
  const host = new Host();
  const declarations = ["platformAuthority", "hostAccess"].map(guestAdmission => ({ ...base, name: guestAdmission, guestAdmission }));
  const env = await materializeBindings(declarations, { ...transports, guestAdmission: host });
  expect(env.platformAuthority).toBe(platform); expect(env.hostAccess).toBe(access);
  revoked = true;
  await expect(materializeBindings(declarations, { ...transports, guestAdmission: host })).rejects.toBe(stopped);
  await expect(materializeBindings(declarations, transports)).rejects.toBeInstanceOf(TypeError);
});

test("immutableObjects materializes the original durable SQLite port without exposing its storage", async () => {
  const database = new Database(":memory:");
  let barriers = 0;
  const storage: SqliteStorage = {
    sql: { exec<Row extends Record<string, import("../../harness/src/cloudflare-storage.js").SqlValue>>(query: string, ...bindings: import("../../harness/src/cloudflare-storage.js").SqlValue[]): Iterable<Row> {
      return database.query(query).all(...bindings.map(value => value instanceof ArrayBuffer ? new Uint8Array(value) : value)).map(row =>
        Object.fromEntries(Object.entries(row as Record<string, unknown>).map(([key, value]) => [key, value instanceof Uint8Array ? value.slice().buffer : value])) as Row);
    } },
    transactionSync(operation) { return database.transaction(operation)(); },
    async sync() { barriers++; },
  };
  try {
    const original = new SqliteObjects(storage, "selected");
    const declaration = { ...base, capability: "grant", resource: "objects:/" };
    const scoped = { ...declaration, name: "scoped", resource: "objects:/harness" };
    const env = await materializeBindings([declaration, scoped], { ...transports, immutableObjects(binding) {
      expect(binding).toEqual(binding.name === scoped.name ? scoped : declaration); return original;
    } });
    const objects = env.scoped as ImmutableObjects;
    expect(Object.keys(objects).sort()).toEqual(["length", "put", "read"]);
    expect(barriers).toBe(0);
    const key = new Uint8Array(33); key[0] = 1;
    const value = new Uint8Array([1, 2, 3]);
    await objects.put([key], [value]);
    expect(barriers).toBe(1);
    expect(await objects.length(key)).toBe(3);
    expect(barriers).toBe(2);
    expect(await objects.read(key, 3n)).toEqual(value);
    expect(barriers).toBe(3);
    await expect(objects.read(key, 2n)).rejects.toThrow("size exceeds admission");
    await expect(objects.put([key], [new Uint8Array([3, 2, 1])])).rejects.toThrow("immutable bytes changed");
    const sealed = new Uint8Array(113); sealed.set(key); sealed[0] |= 0x80;
    const otherLocator = sealed.slice(); otherLocator[33] = 1;
    const otherRoot = sealed.slice(); otherRoot[65] = 1;
    const otherOrigin = sealed.slice(); otherOrigin[81] = 1;
    const keys = [sealed, otherLocator, otherRoot, otherOrigin];
    const values = keys.map((_, index) => new Uint8Array([index + 4]));
    await objects.put(keys, values);
    const reopened = new SqliteObjects(storage, "selected");
    expect(await reopened.read(key, 3n)).toEqual(value);
    for (let index = 0; index < keys.length; index++) {
      expect(await reopened.length(keys[index]!)).toBe(1);
      expect(await reopened.read(keys[index]!, 1n)).toEqual(values[index]!);
    }
    expect(await new SqliteObjects(storage, "other").length(sealed)).toBeNull();
    await expect(reopened.put([sealed], [new Uint8Array([99])])).rejects.toThrow("immutable bytes changed");
  } finally { database.close(); }
});

for (const [resource, producer] of [["machines:/", "machinesWorkspace"], ["machines:/harness", "machinesWorkspace"],
  ["streams:/", "streams"], ["streams:/harness", "streams"]] as const) {
  test(`${resource} requires its owner producer and preserves its terminal refusal`, async () => {
    const declaration = { ...base, capability: "grant", resource };
    await expect(materializeBindings([declaration], transports)).rejects.toThrow(`missing ${producer} producer`);
    const stopped = new DOMException("resource stopped", "AbortError");
    await expect(materializeBindings([declaration], { ...transports, [producer](binding: semantic.Binding): never {
      expect(binding).toEqual(declaration); throw stopped;
    } })).rejects.toBe(stopped);
  });
}

test("unsupported and malformed resource selections refuse before any host producer runs", async () => {
  let effects = 0;
  const authority = { ...transports, immutableObjects(): never { effects++; throw new Error("unexpected producer"); } };
  const valid = { ...base, capability: "grant", resource: "objects:/" };
  const invalid: semantic.Binding[] = [
    { ...valid, resource: "unknown:/" }, { ...valid, resource: "objects-other:/child" }, { ...valid, resource: "streams-other:/child" },
    { ...valid, resource: "constructor:/harness" }, { ...valid, resource: "objects/harness" },
    { ...valid, stateTransaction: true }, { ...valid, guestAdmission: "hostAccess" },
    { ...valid, targetAccount: new Uint8Array([1]) }, { ...valid, guestAdmission: "unknown" },
  ];
  for (const binding of invalid) await expect(materializeBindings([valid, { ...binding, name: "invalid" }], authority)).rejects.toBeDefined();
  expect(effects).toBe(0);
});
