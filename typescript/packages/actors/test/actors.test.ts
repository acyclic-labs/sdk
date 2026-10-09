import { describe, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { create, fromBinary, toBinary, type MessageShape } from "@bufbuild/protobuf";
import { ActorId, CodeSha256, CurrentHeadMarker, PositiveU64, ActorsClient, type ActorsRustBinding, type OperationEvent, type semantic } from "../src/index.js";
import { ActorsService, AddSubscriptionRequestSchema, InspectActorRequestSchema, InspectActorResponseSchema, SubscriptionStartSchema } from "../generated/proto/actors/v1/actors_pb.js";
import { ACTORS_OPERATION_NAMES } from "../src/generated/actors-service.js";

function errorCode(error: unknown): string | undefined {
  if (typeof error !== "object" || error === null) return undefined;
  if ("message" in error && typeof error.message === "string") {
    try {
      const metadata: unknown = JSON.parse(error.message);
      if (typeof metadata === "object" && metadata !== null && "code" in metadata && typeof metadata.code === "string") {
        return metadata.code;
      }
    } catch {
      // Native N-API errors use their status code when no structured metadata exists.
    }
  }
  return "code" in error && typeof error.code === "string" ? error.code : undefined;
}

describe("Rust-backed Actors client", () => {
  test("rejects private CA configuration on the browser WASM path before initialization", () => {
    const entry = new URL("../src/client.ts", import.meta.url).href;
    const probe = `
      import assert from "node:assert/strict";
      globalThis.process = undefined;
      const { ActorsClient } = await import(${JSON.stringify(entry)});
      const client = new ActorsClient({ endpoint: "https://actors.example.test", token: "test", caCertificate: new Uint8Array([1]) });
      await assert.rejects(client.transport, { code: "configuration", message: "WASM Actors transport cannot configure a private CA certificate" });
    `;
    const result = spawnSync(process.execPath, ["--eval", probe], { encoding: "utf8" });
    expect(result.stderr).toBe("");
    expect(result.status).toBe(0);
  });
  test("exposes nominal constructors from the package entrypoint", async () => {
    const actorId = await ActorId("actor-a");
    const digest = await CodeSha256(new Uint8Array([1, ...new Uint8Array(31)]));
    const positive = await PositiveU64(1n);
    const currentHead = await CurrentHeadMarker(true);
    expect(actorId).toBe("actor-a");
    expect(digest).toHaveLength(32);
    expect(positive).toBe(1n);
    expect(currentHead).toBe(true);
  });

  test("rejects out-of-range positive integers at the Rust bridge", async () => {
    for (const [value, code] of [
      [-1n, "not_positive"],
      [0n, "invalid_argument"],
      [18_446_744_073_709_551_616n, "not_positive"],
    ] as const) {
      try {
        await PositiveU64(value);
        throw new Error(`PositiveU64 unexpectedly accepted ${value}`);
      } catch (error) {
        expect(errorCode(error)).toBe(code);
      }
    }
    expect(await PositiveU64(18_446_744_073_709_551_615n)).toBe(18_446_744_073_709_551_615n);
  });

  test("installs every public operation from the maintained service descriptor", () => {
    expect(ACTORS_OPERATION_NAMES).toEqual(ActorsService.methods.map(method => method.localName));
    const client = new ActorsClient({ endpoint: "https://actors.example.test", token: "secret", binding: { connect: async () => ({ transport: "test" }) } });
    for (const operation of ACTORS_OPERATION_NAMES) expect(typeof client[operation]).toBe("function");
  });

  test("encodes semantic oneof requests and preserves optional response presence", async () => {
    let inspected: MessageShape<typeof InspectActorRequestSchema> | undefined;
    let subscribed: MessageShape<typeof AddSubscriptionRequestSchema> | undefined;
    const events: OperationEvent[] = [];
    const binding: ActorsRustBinding = {
      connect: async () => ({
        transport: "test",
        inspectActor: async request => {
          inspected = fromBinary(InspectActorRequestSchema, request);
          return new Uint8Array();
        },
        addSubscription: async request => {
          subscribed = fromBinary(AddSubscriptionRequestSchema, request);
          return new Uint8Array();
        },
      }),
    };
    const client = new ActorsClient({ endpoint: "https://actors.example.test", token: "secret", binding, observer: { onOperation: event => events.push(event) } });

    await client.inspectActor({ actorId: "actor-a" as semantic.ActorId });
    await client.addSubscription({
      actorId: "actor-a" as semantic.ActorId,
      subscription: { subscriptionId: "subscription-a", streamPath: "events/a", start: { start: { case: "currentHead", value: true } }, placementAnchor: false },
      idempotencyKey: "subscribe-a",
    });

    expect(inspected?.actorId).toBe("actor-a");
    expect(subscribed?.subscription?.start?.start.case).toBe("currentHead");
    expect(subscribed?.subscription?.start?.start.value).toBe(true);
    expect(events.map(event => event.op)).toEqual(["inspectActor", "addSubscription"]);
    expect(events.every(event => event.requestBytes !== undefined)).toBe(true);
  });

  test("preserves absence of optional scalar response properties", async () => {
    const client = new ActorsClient({
      endpoint: "https://actors.example.test",
      token: "secret",
      binding: {
        connect: async () => ({
          transport: "test",
          inspectActor: async () => toBinary(InspectActorResponseSchema, create(InspectActorResponseSchema, {
            actor: {
              actorId: "actor-a",
              codeSha256: new Uint8Array([1, ...new Uint8Array(31)]),
              homeRegion: "eu",
              state: 0,
              subscriptions: [],
              checkpointEpoch: 0n,
              configurationRevision: 0n,
            },
          })),
        }),
      },
    });

    const response = await client.inspectActor({ actorId: "actor-a" as semantic.ActorId });
    expect(Object.hasOwn(response.actor!, "checkpointUnixMillis")).toBe(false);
    expect(response.actor?.checkpointUnixMillis).toBeUndefined();
  });

  test("keeps subscription-start alternatives owned by their oneof", () => {
    expect(SubscriptionStartSchema.fields.every(field => field.oneof !== undefined)).toBe(true);
  });

  test("snapshots requests before connecting and exposes AbortSignal cancellation", async () => {
    let releaseConnect!: (client: Awaited<ReturnType<ActorsRustBinding["connect"]>>) => void;
    let inspected: MessageShape<typeof InspectActorRequestSchema> | undefined;
    const binding: ActorsRustBinding = {
      connect: () => new Promise(resolve => { releaseConnect = resolve; }),
    };
    const client = new ActorsClient({ endpoint: "https://actors.example.test", token: "secret", binding });
    const request = { actorId: "before" as semantic.ActorId };
    const pending = client.inspectActor(request);
    request.actorId = "after" as semantic.ActorId;
    releaseConnect({
      transport: "test",
      inspectActor: async encoded => {
        inspected = fromBinary(InspectActorRequestSchema, encoded);
        return new Uint8Array();
      },
    });
    await pending;
    expect(inspected?.actorId).toBe("before");

    let finish!: (value: Uint8Array) => void;
    const cancellable = new ActorsClient({
      endpoint: "https://actors.example.test",
      token: "secret",
      binding: { connect: async () => ({ transport: "test", inspectActor: () => new Promise(resolve => { finish = resolve; }) }) },
    });
    const controller = new AbortController();
    const call = cancellable.inspectActor({ actorId: "actor-a" as semantic.ActorId }, { signal: controller.signal });
    controller.abort();
    await expect(call).rejects.toMatchObject({ code: "cancelled" });
    finish?.(new Uint8Array());
  });

  test("observes connection failure and includes the pending connection duration", async () => {
    const events: OperationEvent[] = [];
    let elapsed = 0;
    const client = new ActorsClient({
      endpoint: "https://actors.example.test", token: "secret",
      observer: { onOperation: event => events.push(event) },
      binding: { connect: async () => {
        const start = performance.now();
        await new Promise(resolve => setTimeout(resolve, 20));
        elapsed = performance.now() - start;
        throw Object.assign(new Error("connect rejected"), { code: "unavailable" });
      } },
    });
    await expect(client.inspectActor({ actorId: "actor-a" as semantic.ActorId })).rejects.toMatchObject({ code: "unavailable" });
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({ family: "actors", op: "inspectActor", ok: false, code: "unavailable" });
    expect(events[0]!.durationMs).toBeGreaterThanOrEqual(elapsed);
    expect(events[0]!.requestBytes).toBeGreaterThan(0);
    expect(events[0]!.responseBytes).toBeUndefined();
  });

  test("does not cancel a shared pending connection for another caller", async () => {
    let releaseConnect!: (client: Awaited<ReturnType<ActorsRustBinding["connect"]>>) => void;
    let connectionSignal!: AbortSignal;
    const binding: ActorsRustBinding = {
      connect: (_endpoint, _token, signal) => {
        connectionSignal = signal!;
        return new Promise(resolve => { releaseConnect = resolve; });
      },
    };
    const events: OperationEvent[] = [];
    const client = new ActorsClient({ endpoint: "https://actors.example.test", token: "secret", binding, observer: { onOperation: event => events.push(event) } });
    const firstController = new AbortController();
    const secondController = new AbortController();
    const first = client.inspectActor({ actorId: "first" as semantic.ActorId }, { signal: firstController.signal });
    const second = client.inspectActor({ actorId: "second" as semantic.ActorId }, { signal: secondController.signal });

    const waitStarted = performance.now();
    await new Promise(resolve => setTimeout(resolve, 20));
    const waited = performance.now() - waitStarted;
    firstController.abort();
    await expect(first).rejects.toMatchObject({ code: "cancelled" });
    expect(connectionSignal.aborted).toBe(false);

    releaseConnect({ transport: "test", inspectActor: async () => new Uint8Array() });
    await second;
    expect(connectionSignal.aborted).toBe(false);
    expect(events.map(event => [event.ok, event.code])).toEqual([[false, "cancelled"], [true, undefined]]);
    expect(events.every(event => event.durationMs >= waited)).toBe(true);
  });

  test("reconnects after the final pending connection waiter aborts", async () => {
    let connects = 0;
    let releaseSecond!: (client: Awaited<ReturnType<ActorsRustBinding["connect"]>>) => void;
    const binding: ActorsRustBinding = {
      connect: (_endpoint, _token, signal) => {
        connects += 1;
        if (connects === 2) return new Promise(resolve => { releaseSecond = resolve; });
        return new Promise((_resolve, reject) => {
          signal?.addEventListener("abort", () => setTimeout(() => reject(new Error("late connect abort")), 25), { once: true });
        });
      },
    };
    const client = new ActorsClient({ endpoint: "https://actors.example.test", token: "secret", binding });
    const controller = new AbortController();
    const first = client.inspectActor({ actorId: "first" as semantic.ActorId }, { signal: controller.signal });
    controller.abort();
    await expect(first).rejects.toMatchObject({ code: "cancelled" });

    const second = client.inspectActor({ actorId: "second" as semantic.ActorId });
    expect(connects).toBe(2);
    releaseSecond({ transport: "test", inspectActor: async () => new Uint8Array() });
    await second;
  });
});
