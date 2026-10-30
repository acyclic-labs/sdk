import { expect, test } from "bun:test";
import { initializeClientQueries } from "../src/client-query.js";
import { AgentGuest, WasmAgentProviders, WasmAgentServer, WasmSessionAuthority, WasmStream } from "../generated/wasm/acyclic_harness_wasm.js";
import { agentsFixture } from "./browser-agents-fixture.mjs";
import { EventAgentHost, type EventHostAccess, type StreamLedger, type EventStorage } from "../src/event-host.js";

async function setup() {
  const id = "00112233-4455-6677-8899-aabbccddeeff";
  const agent = `harness/agents/${"01".repeat(16)}`;
  const fixture = await agentsFixture(id, async function* () {}, initializeClientQueries);
  const keys = await crypto.subtle.generateKey("Ed25519", true, ["sign", "verify"]) as CryptoKeyPair;
  const key = [...new Uint8Array(await crypto.subtle.exportKey("raw", keys.publicKey))];
  const trust = { trust: new TextEncoder().encode(JSON.stringify({ environment: "test", environment_keys: [key],
    account: "account", issuer_keys: [key], minimum_key_epoch: 1, revoked_key_ids: [], revoked_block_ids: [] })),
    audience: "workers:/test", holder: "account" };
  const authority = new WasmSessionAuthority(new TextDecoder().decode(trust.trust), trust.audience, trust.holder);
  return { agent, fixture, authority, trust };
}

test("portable host disposal ends pending storage controls and refuses new work", async () => {
  const { agent, fixture, authority } = await setup();
  const operations = {
    validateAdopt: (host: WasmAgentServer) => host.validateAdopt(agent),
    read: (host: WasmAgentServer) => host.read(agent, "status", 0n, 1),
    pause: (host: WasmAgentServer) => host.pause(agent, true),
    archive: (host: WasmAgentServer) => host.archive(agent, "archive"),
    drain: (host: WasmAgentServer) => host.drain(agent),
    create: (host: WasmAgentServer) => host.create("new"),
  };
  try {
    for (const [name, operation] of Object.entries(operations)) {
      const entered = Promise.withResolvers<void>();
      let calls = 0;
      const wait = () => { ++calls; entered.resolve(); return new Promise<never>(() => {}); };
      const stream = WasmStream.connectProvider({ dispatch: wait, open: wait });
      const objects = { length: wait, read: wait, put: wait };
      const providers = WasmAgentProviders.injected(stream, objects, false, fixture.options);
      const guest = AgentGuest.open(providers, fixture.options, fixture.owner, fixture.registry);
      const host = WasmAgentServer.open(providers, fixture.options, fixture.owner, fixture.registry,
        authority, { manifest: guest.manifest(), onRecord: () => { throw new Error("unexpected guest delivery"); },
          policy: () => true, admitted: async () => {} });
      try {
        if (name !== "create") await host.adopt(agent);
        const outcome = operation(host).then(
          () => new Error(`storage control ${name} unexpectedly completed`), error => error);
        await Promise.race([entered.promise, outcome.then(error => { throw error; })]);
        host.close();
        const error = await outcome;
        expect(error).toBeInstanceOf(Error);
        expect(error.code).toBe("unavailable");
        expect(error.message).toContain("unavailable");
        const before = calls;
        await expect(host.create("new")).rejects.toThrow("unavailable");
        await expect(host.adopt(agent)).rejects.toThrow("unavailable");
        await expect(operation(host)).rejects.toThrow("unavailable");
        expect(calls).toBe(before);
      } finally {
        host.close(); host.free(); guest.free(); providers.free(); stream.free();
      }
    }
  } finally { authority.free(); fixture.free(); }
}, 10_000);

test("event owner loss releases metadata, placement and queued waits", async () => {
  const { fixture, authority, trust } = await setup();
  try {
    for (const phase of ["alarm", "flush", "placement"]) {
      const entered = Promise.withResolvers<void>();
      const producer = Promise.withResolvers<void>();
      const values = new Map<string, string>();
      const calls = { alarm: 0, flush: 0, placement: 0, write: 0, provider: 0 };
      const wait = async (step: string) => {
        if (step === phase) { entered.resolve(); await producer.promise; }
      };
      const storage: EventStorage = {
        get: key => values.get(key), entries: () => [...values], transaction: action => action(),
        put: (key, value) => { ++calls.write; values.set(key, value); },
        alarm: async () => { ++calls.alarm; await wait("alarm"); },
        flush: async () => { ++calls.flush; await wait("flush"); },
      };
      const unexpected = async (): Promise<never> => {
        ++calls.provider;
        throw new Error("provider must not run after the pending placement owner closes");
      };
      const ledger: StreamLedger = {
        register: async () => { ++calls.placement; await wait("placement"); return { admitted: true }; },
        dispatch: unexpected, open: unexpected,
        objectLength: unexpected, objectRead: unexpected, objectPut: unexpected,
      };
      const host = new EventAgentHost(storage, ledger, phase, fixture.options, fixture.owner, fixture.registry,
        { trust, credential: new Uint8Array(), policy: () => true });
      const result = (operation: Promise<unknown>) => operation.then(
        () => new Error(`${phase} unexpectedly completed`), error => error);
      try {
        const pending = result(host.create("retained-intent"));
        await Promise.race([entered.promise, pending.then(error => { throw error; })]);
        const queued = result(host.create("retained-intent"));
        const wake = result(host.wake());
        const before = { ...calls };
        host.close();
        // The admitted primitive remains pending; loss of its owner releases
        // both this request and commands waiting behind it.
        for (const error of await Promise.all([pending, queued, wake])) {
          expect(error).toBeInstanceOf(Error);
          expect(error.code).toBe("unavailable");
        }
        expect(calls).toEqual(before);
        await expect(host.create("new-intent")).rejects.toThrow("unavailable");
        await expect(host.read("status")).rejects.toThrow("unavailable");
        expect(calls).toEqual(before);
      } finally { host.close(); producer.resolve(); }
    }
  } finally { authority.free(); fixture.free(); }
}, 10_000);

test("failed delivery ends current waiters and preserves recovery without reviving disposal", async () => {
  const { agent, fixture, authority } = await setup();
  let entered = Promise.withResolvers<void>();
  let calls = 0;
  const wait = () => { ++calls; entered.resolve(); return new Promise<never>(() => {}); };
  const stream = WasmStream.connectProvider({ dispatch: wait, open: wait });
  const providers = WasmAgentProviders.injected(stream, { length: wait, read: wait, put: wait }, false, fixture.options);
  const guest = AgentGuest.open(providers, fixture.options, fixture.owner, fixture.registry);
  const host = WasmAgentServer.open(providers, fixture.options, fixture.owner, fixture.registry,
    authority, { manifest: guest.manifest(), onRecord: () => { throw new Error("unexpected guest delivery"); },
      policy: () => true, admitted: async () => {} });
  const outcome = (pending: Promise<unknown>) => pending.then(
    () => new Error("pending storage unexpectedly completed"), error => error);
  try {
    await host.adopt(agent);
    const first = outcome(host.read(agent, "status", 0n, 1));
    await Promise.race([entered.promise, first.then(error => { throw error; })]);
    host.deliveryFailed();
    expect((await first).code).toBe("unavailable");
    entered = Promise.withResolvers<void>();
    const second = outcome(host.read(agent, "status", 0n, 1));
    await Promise.race([entered.promise, second.then(error => { throw error; })]);
    expect(calls).toBe(2);
    host.close();
    expect((await second).code).toBe("unavailable");
    host.deliveryFailed();
    await expect(host.read(agent, "status", 0n, 1)).rejects.toThrow("unavailable");
    expect(calls).toBe(2);
  } finally {
    host.close(); host.free(); guest.free(); providers.free(); stream.free();
    authority.free(); fixture.free();
  }
}, 10_000);

test("cancelled queued Session open retains its predecessor's admission fence", async () => {
  const { fixture, authority, trust } = await setup();
  const entered = Promise.withResolvers<void>();
  const producer = Promise.withResolvers<void>();
  const values = new Map<string, string>();
  let pending = true;
  let alarms = 0;
  let overtook = false;
  const storage: EventStorage = {
    get: key => values.get(key), entries: () => [...values], transaction: action => action(),
    put: (key, value) => { values.set(key, value); },
    alarm: async () => { if (++alarms > 1 && pending) overtook = true; },
    flush: async () => { entered.resolve(); await producer.promise; pending = false; },
  };
  const refused = new Error("original placement producer refused");
  const unexpected = async (): Promise<never> => { throw refused; };
  const ledger: StreamLedger = { register: unexpected, dispatch: unexpected, open: unexpected,
    objectLength: unexpected, objectRead: unexpected, objectPut: unexpected };
  const host = new EventAgentHost(storage, ledger, "ordered", fixture.options, fixture.owner, fixture.registry,
    { trust, credential: new Uint8Array(), policy: () => true });
  const outcome = (promise: Promise<unknown>) => promise.then(() => undefined, error => error);
  try {
    const first = outcome(host.create("ordered"));
    await entered.promise;
    const abort = new AbortController();
    const cancelled = outcome(host.grpcSession(async () => null, abort.signal));
    const successor = outcome(host.wake());
    abort.abort();
    expect((await cancelled).code).toBe("unavailable");
    // Cancellation completes while the original admitted write is still held.
    // Its successor must retain that write's ordering fence.
    producer.resolve();
    expect(await first).toBe(refused);
    expect((await successor).code).toBe("unavailable");
    expect(overtook).toBe(false);
    expect(alarms).toBe(2);
  } finally { host.close(); producer.resolve(); authority.free(); fixture.free(); }
});
test("session trust is plain public data, verified in the host module and fenced on owner loss", async () => {
  const { fixture, authority, trust } = await setup();
  const unexpected = async (): Promise<never> => { throw new Error("no storage or provider work is expected"); };
  const storage: EventStorage = { get: () => undefined, entries: () => [], transaction: action => action(),
    put: () => undefined, alarm: unexpected, flush: unexpected };
  const ledger: StreamLedger = { register: unexpected, dispatch: unexpected, open: unexpected,
    objectLength: unexpected, objectRead: unexpected, objectPut: unexpected };
  const open = (descriptor: typeof trust & Partial<Pick<EventHostAccess["trust"], "changed">>) =>
    new EventAgentHost(storage, ledger, "trust", fixture.options, fixture.owner, fixture.registry,
      { trust: descriptor, credential: new Uint8Array(), policy: () => true });
  const encode = (value: unknown) => new TextEncoder().encode(JSON.stringify(value));
  const snapshot = JSON.parse(new TextDecoder().decode(trust.trust));
  type Registration = PromiseWithResolvers<Uint8Array> & { current: Uint8Array; signal: AbortSignal };
  const publications = () => {
    const registrations: Registration[] = [];
    let registered = Promise.withResolvers<void>();
    return {
      registrations, next: () => registered.promise,
      changed(current: Uint8Array, signal: AbortSignal) {
        const registration = { ...Promise.withResolvers<Uint8Array>(), current, signal };
        registrations.push(registration);
        registered.resolve();
        registered = Promise.withResolvers<void>();
        return registration.promise;
      },
    };
  };
  const aborted = (signal: AbortSignal) => new Promise(resolve => signal.addEventListener("abort", resolve, { once: true }));
  try {
    // Only the public snapshot fields decode: a smuggled secret or non-UTF-8 bytes refuse.
    expect(() => open({ ...trust, trust: encode({ ...snapshot, signing_key: Array(32).fill(1) }) })).toThrow();
    expect(() => open({ ...trust, trust: Uint8Array.of(0xff) })).toThrow();
    const owner = publications();
    const host = open({ ...trust, changed: owner.changed });
    try {
      expect(owner.registrations.map(r => r.current)).toEqual([trust.trust]);
      // A monotone revocation applies and re-registers from that publication.
      const revoked = encode({ ...snapshot, revoked_key_ids: ["revoked"] });
      const registered = owner.next();
      owner.registrations[0]!.resolve(revoked);
      await registered;
      expect(owner.registrations[1]!.current).toBe(revoked);
      // Un-revoking is not monotone: the host fences instead of keeping either trust.
      const fenced = aborted(owner.registrations[1]!.signal);
      owner.registrations[1]!.resolve(trust.trust);
      await fenced;
      await expect(host.create("after-fence")).rejects.toThrow("unavailable");
    } finally { host.close(); }
    // Producer loss closes the host the same way.
    const lost = publications();
    const orphan = open({ ...trust, changed: lost.changed });
    try {
      const fenced = aborted(lost.registrations[0]!.signal);
      lost.registrations[0]!.reject(new Error("trust owner lost"));
      await fenced;
      await expect(orphan.create("after-loss")).rejects.toThrow("unavailable");
    } finally { orphan.close(); }
  } finally { authority.free(); fixture.free(); }
}, 10_000);
