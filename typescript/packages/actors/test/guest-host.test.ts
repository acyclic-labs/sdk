import { expect, test } from "bun:test";
import { GuestActivation } from "../src/guest-host.js";
import { defineActor, GUEST_ABI_VERSION, type GuestContext } from "../src/guest.js";

test("typed guests preserve original contexts, quiesced checkpoint cuts and terminal failure", async () => {
  const entered = Promise.withResolvers<void>(), release = Promise.withResolvers<void>();
  const capturing = Promise.withResolvers<void>(), publish = Promise.withResolvers<void>();
  const account = Array.from(new TextEncoder().encode("account"));
  const context: GuestContext<Record<string, never>> = {
    env: {}, clock: () => 0n, deliveryId: null, admittedIdentity: null,
    actor: { id: "one", account, namespace: "counter", class: null },
    retryKey() { throw new Error("no effect in this history"); },
  };
  const codec = {
    identity: { name: "counter", version: "1" },
    readable: [{ name: "counter", version: "1" }],
    encode: (value: { count: number }) => new Uint8Array([value.count]),
    decode: (value: { bytes: Uint8Array }) => ({ count: value.bytes[0] ?? 0 }),
  };
  let restores = 0, nextCalls = 0;
  const actor = defineActor<{ count: number }, Record<string, never>>({
    abiVersion: GUEST_ABI_VERSION, codec,
    restore(checkpoint) { restores++; return checkpoint === null ? { count: 0 } : codec.decode(checkpoint); },
    event(state) { state.with(value => { value.count++; return undefined; }); return null; },
    async checkpoint(state) {
      let count = 0;
      state.with(value => { count = value.count; return undefined; });
      capturing.resolve(); await publish.promise;
      return { codec: codec.identity, bytes: codec.encode({ count }) };
    },
    async fetch(state, request, current) {
      const path = new URL(request.url).pathname;
      if (path === "/held") { entered.resolve(); await release.promise; }
      if (path === "/next") nextCalls++;
      if (path === "/fail") throw new Error("guest handler failed");
      if (path === "/nested") state.with(() => { state.with(() => undefined); return undefined; });
      let count = 0;
      state.with(value => { count = ++value.count; return undefined; });
      return Response.json({ count, delivery: current.deliveryId });
    },
  });
  const retained = { codec: codec.identity, bytes: new Uint8Array([7]) };
  const activation = new GuestActivation(actor, retained, context);
  retained.bytes[0] = 99;
  const held = activation.fetch(new Request("https://example.test/held"), { ...context, deliveryId: "held" });
  await entered.promise;
  const capture = activation.checkpoint();
  const next = activation.fetch(new Request("https://example.test/next"), { ...context, deliveryId: "next" });
  release.resolve();
  expect(await (await held).json()).toEqual({ count: 8, delivery: "held" });
  await capturing.promise;
  expect(nextCalls).toBe(0);
  publish.resolve();
  expect((await capture).bytes).toEqual(new Uint8Array([8]));
  expect(await (await next).json()).toEqual({ count: 9, delivery: "next" });
  expect(restores).toBe(1);
  expect(() => activation.fetch(new Request("https://example.test/"), {
    ...context, actor: { ...context.actor, id: "other" },
  })).toThrow("actor differs");
  await expect(activation.fetch(new Request("https://example.test/fail"), context)).rejects.toThrow("guest handler failed");
  await expect(activation.fetch(new Request("https://example.test/next"), context)).rejects.toThrow("activation");
  await expect(activation.checkpoint()).rejects.toThrow("activation");
  expect(nextCalls).toBe(1);
  expect(() => new GuestActivation({ ...actor, abiVersion: GUEST_ABI_VERSION + 1 }, null, context)).toThrow("guest ABI");
  const nested = new GuestActivation(actor, null, context);
  await expect(nested.fetch(new Request("https://example.test/nested"), context)).rejects.toThrow("nested guest state");
  const fresh = new GuestActivation(actor, null, context);
  expect(await (await fresh.fetch(new Request("https://example.test/next"), context)).json()).toEqual({ count: 1, delivery: null });
});
