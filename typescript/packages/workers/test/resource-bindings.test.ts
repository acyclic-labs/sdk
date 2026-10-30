import { expect, test } from "bun:test";
import type { ActorStateBinding, ActorStateHost, semantic } from "@acyclic-labs/actors";
import type { GuestAdmissionHost } from "@acyclic-labs/auth";
import type { ObjectsV1BinaryPort } from "@acyclic-labs/objects";
import { materializeBindings } from "../src/bindings.js";

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

test("objects materializes only the admitted account port, keeping its receiver and outcomes", async () => {
  const refused = new Error("original refusal");
  class Account {
    readonly calls: [string, Uint8Array][] = [];
    open(route: string, input: Uint8Array) {
      this.calls.push([route, input]);
      return { next: async () => {
        if (route === "blobs/delete") throw refused;
        return { body: Uint8Array.of(input.length) };
      }, close() {} };
    }
    readonly secret = "account storage";
  }
  const original = new Account();
  const declaration = { ...base, capability: "grant", resource: "objects:/" };
  const scoped = { ...declaration, name: "scoped", resource: "objects:/harness" };
  const env = await materializeBindings([declaration, scoped], { ...transports, objects(binding) {
    expect(binding).toEqual(binding.name === scoped.name ? scoped : declaration); return original;
  } });
  const port = env.scoped as ObjectsV1BinaryPort;
  expect(Object.isFrozen(port)).toBe(true);
  expect(Object.keys(port)).toEqual(["open"]);
  const input = Uint8Array.of(1, 2, 3);
  expect(await port.open("blobs/get", input).next()).toEqual({ body: Uint8Array.of(3) });
  expect(original.calls).toEqual([["blobs/get", input]]);
  await expect(port.open("blobs/delete", input).next()).rejects.toBe(refused);
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
  const authority = { ...transports, objects(): never { effects++; throw new Error("unexpected producer"); } };
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
