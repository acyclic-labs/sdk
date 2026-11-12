import { expect, test } from "bun:test";
import { Buffer } from "node:buffer";
import { readFileSync } from "node:fs";
import { GuestActivation } from "../src/guest-host.js";
import { defineActor, GUEST_ABI_VERSION, type GuestContext, type GuestEvent } from "../src/guest.js";

async function json(response: Awaited<ReturnType<GuestActivation<unknown, unknown>["fetch"]>>) {
  if (!(response instanceof Response)) throw new Error("expected buffered response");
  return response.json();
}

test.each(["Uint8Array", "Buffer"])("typed guests preserve original contexts, quiesced checkpoint cuts and terminal failure (%s)", async backing => {
  const bytes = (value: number[]) => backing === "Buffer" ? Buffer.from(value) : new Uint8Array(value);
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
    encode: (value: { count: number }) => bytes([value.count]),
    decode: (value: { bytes: Uint8Array }) => ({ count: value.bytes[0] ?? 0 }),
  };
  let restores = 0, nextCalls = 0;
  let capturedSource: Uint8Array | undefined;
  const actor = defineActor<{ count: number }, Record<string, never>>({
    abiVersion: GUEST_ABI_VERSION, codec,
    restore(checkpoint) { restores++; return checkpoint === null ? { count: 0 } : codec.decode(checkpoint); },
    event(state) { state.with(value => { value.count++; return undefined; }); return null; },
    async checkpoint(state) {
      let count = 0;
      state.with(value => { count = value.count; return undefined; });
      capturing.resolve(); await publish.promise;
      capturedSource = codec.encode({ count });
      return { codec: codec.identity, bytes: capturedSource };
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
  const retained = { codec: codec.identity, bytes: bytes([7]) };
  for (const version of ["0", "2", "unknown"]) {
    expect(() => new GuestActivation(actor, { codec: { ...codec.identity, version }, bytes: bytes([]) }, context))
      .toThrow("incompatible checkpoint codec");
  }
  expect(restores).toBe(0);
  const activation = new GuestActivation(actor, retained, context);
  retained.bytes[0] = 99;
  const held = activation.fetch(new Request("https://example.test/held"), { ...context, deliveryId: "held" });
  await entered.promise;
  const capture = activation.checkpoint();
  const next = activation.fetch(new Request("https://example.test/next"), { ...context, deliveryId: "next" });
  release.resolve();
  expect(await json(await held)).toEqual({ count: 8, delivery: "held" });
  await capturing.promise;
  expect(nextCalls).toBe(0);
  publish.resolve();
  const checkpoint = await capture;
  expect(checkpoint.bytes).toEqual(new Uint8Array([8]));
  capturedSource![0] = 77;
  expect(checkpoint.bytes).toEqual(new Uint8Array([8]));
  checkpoint.bytes[0] = 88;
  expect(capturedSource![0]).toBe(77);
  expect(await json(await next)).toEqual({ count: 9, delivery: "next" });
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
  expect(await json(await fresh.fetch(new Request("https://example.test/next"), context))).toEqual({ count: 1, delivery: null });
  const wrongCapture = new GuestActivation({ ...actor, checkpoint: () => ({ codec: { ...codec.identity, version: "2" }, bytes: bytes([]) }) }, null, context);
  await expect(wrongCapture.checkpoint()).rejects.toMatchObject({ type: "incompatibleCodec" });
});

const encoder = new TextEncoder();
// Rust-captured `actors::guest::wire` frames: an alarm delivery for actor
// (account, namespace, codec) and the transition that keeps its checkpoint and alarm.
const fixture = (name: string) => new Uint8Array(readFileSync(new URL(`../../../../rust/codec-fuzz/corpus/actor_codecs/${name}`, import.meta.url)));

function recordingActor(identity: { name: string; version: string }) {
  const seen: { event: GuestEvent; deliveryId: string | null; now: bigint; key: string }[] = [];
  let restores = 0;
  const actor = defineActor<{ bytes: Uint8Array; count: number }, Record<string, never>>({
    abiVersion: GUEST_ABI_VERSION,
    codec: { identity, encode: value => value.bytes, decode: value => ({ bytes: value.bytes, count: 0 }) },
    restore(checkpoint) { restores++; return { bytes: checkpoint?.bytes ?? new Uint8Array(), count: 0 }; },
    event(state, event, context) {
      seen.push({ event, deliveryId: context.deliveryId, now: context.clock(), key: context.retryKey("send") });
      if (event.type !== "stream") return null;
      let count = 0;
      state.with(value => { count = ++value.count; value.bytes = Uint8Array.of(count); return undefined; });
      return { type: "reply", payload: Uint8Array.of(count, 0xff) };
    },
    checkpoint(state) {
      let bytes: Uint8Array = new Uint8Array();
      state.with(value => { bytes = value.bytes; return undefined; });
      return { codec: identity, bytes };
    },
    fetch() { return new Response(null); },
  });
  return { actor, seen, restores: () => restores };
}

const guestContext = (id: string, namespace: string): GuestContext<Record<string, never>> => ({
  env: {}, clock: () => 0n, deliveryId: null, admittedIdentity: null,
  actor: { id, account: Array.from(encoder.encode("account")), namespace, class: null },
  retryKey() { throw new Error("no delivery"); },
});

test("onRecord answers a Rust-captured delivery through the Rust wire codec, refusing non-canonical frames first", async () => {
  const delivery = fixture("delivery"), captured = fixture("transition");
  const reservations = { name: "acyclic.actors.reservations", version: "1" };
  const retained = { codec: reservations, bytes: encoder.encode(`{"capacity":7,"used":0,"generation":0,"active":{}}`) };
  const { actor, seen, restores } = recordingActor(reservations);
  const activation = new GuestActivation(actor, retained, guestContext("codec", "namespace"));
  // Strictness is the Rust decoder's (covered by its own tests); an unknown field 15 is refused before the heap.
  await expect(activation.onRecord(Uint8Array.from([...delivery, 0x78, 0x01]))).rejects.toThrow("non-canonical guest delivery");
  expect(restores()).toBe(0);
  // Alarm keep at tag 4, the empty effect commit and its fixed idempotency key, as Rust writes them.
  const expected = Uint8Array.from([...captured.subarray(0, 0x5e), 0x3a, 0x1e, ...captured.subarray(captured.length - 30)]);
  expect(await activation.onRecord(delivery)).toEqual(expected);
  expect(await activation.onRecord(delivery)).toEqual(expected);
  expect(restores()).toBe(1);
  expect(seen).toEqual([1, 2].map(() => ({ event: { type: "alarm" }, deliveryId: "alarm:1", now: 42n, key: "guest-v1:7:alarm:1:send" })));
  for (const [id, namespace] of [["other", "namespace"], ["codec", "other"]] as const) {
    await expect(new GuestActivation(actor, retained, guestContext(id, namespace)).onRecord(delivery))
      .rejects.toThrow("delivery actor differs from activation");
  }
  await expect(new GuestActivation(actor, null, guestContext("codec", "namespace")).onRecord(delivery))
    .rejects.toThrow("delivery checkpoint differs from activation");
});

test("onRecord projects a record delivery onto the stream event and commits its reply with the next checkpoint", async () => {
  const counter = { name: "counter", version: "1" };
  const shared = JSON.parse(readFileSync(new URL("../../../../rust/crates/actors/tests/fixtures/guest-record.json", import.meta.url), "utf8"));
  const delivery = Uint8Array.from(shared.delivery);
  const { actor, seen } = recordingActor(counter);
  const activation = new GuestActivation(actor, null, guestContext("one", "counter"));
  expect(await activation.onRecord(delivery)).toEqual(Uint8Array.from(shared.transition));
  expect(seen).toEqual([{
    event: shared.event,
    deliveryId: "record:inbox:5", now: 1700n, key: "guest-v1:14:record:inbox:5:send",
  }]);
  // The heap now extends an unpublished transition: a retry of the original delivery needs a fresh activation.
  await expect(activation.onRecord(delivery)).rejects.toThrow("delivery checkpoint differs from activation");
  const aborted = new AbortController();
  aborted.abort(new Error("host gave up"));
  await expect(new GuestActivation(actor, null, guestContext("one", "counter")).onRecord(delivery, aborted.signal))
    .rejects.toThrow("host gave up");
});

test("onRecord cancellation fences an in-flight guest and its retained heap", async () => {
  const shared = JSON.parse(readFileSync(new URL("../../../../rust/crates/actors/tests/fixtures/guest-record.json", import.meta.url), "utf8"));
  const { actor } = recordingActor({ name: "counter", version: "1" });
  let enter!: () => void, finish!: () => void;
  const entered = new Promise<void>(resolve => { enter = resolve; });
  const finished = new Promise<void>(resolve => { finish = resolve; });
  const activation = new GuestActivation({ ...actor, async event() {
    enter();
    await finished;
    return null;
  } }, null, guestContext("one", "counter"));
  const cancellation = new AbortController();
  const pending = activation.onRecord(Uint8Array.from(shared.delivery), cancellation.signal);
  await entered;
  cancellation.abort(new Error("ownership revoked"));
  await expect(pending).rejects.toMatchObject({ code: "unavailable" });
  finish();
  await expect(activation.checkpoint()).rejects.toMatchObject({ code: "unavailable" });
});