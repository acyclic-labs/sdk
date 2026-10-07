import { describe, expect, test } from "bun:test";
import { fromBinary, type MessageShape } from "@bufbuild/protobuf";
import { ActorsClient, type ActorsRustBinding, type OperationEvent, type semantic } from "../src/index.js";
import { ActorsService, AddSubscriptionRequestSchema, InspectActorRequestSchema } from "../generated/proto/actors/v1/actors_pb.js";
import { ACTORS_OPERATION_NAMES } from "../src/generated/actors-service.js";

describe("Rust-backed Actors client", () => {
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

  test("does not cancel a shared pending connection for another caller", async () => {
    let releaseConnect!: (client: Awaited<ReturnType<ActorsRustBinding["connect"]>>) => void;
    let connectionSignal!: AbortSignal;
    const binding: ActorsRustBinding = {
      connect: (_endpoint, _token, signal) => {
        connectionSignal = signal!;
        return new Promise(resolve => { releaseConnect = resolve; });
      },
    };
    const client = new ActorsClient({ endpoint: "https://actors.example.test", token: "secret", binding });
    const firstController = new AbortController();
    const secondController = new AbortController();
    const first = client.inspectActor({ actorId: "first" as semantic.ActorId }, { signal: firstController.signal });
    const second = client.inspectActor({ actorId: "second" as semantic.ActorId }, { signal: secondController.signal });

    firstController.abort();
    await expect(first).rejects.toMatchObject({ code: "cancelled" });
    expect(connectionSignal.aborted).toBe(false);

    releaseConnect({ transport: "test", inspectActor: async () => new Uint8Array() });
    await second;
    expect(connectionSignal.aborted).toBe(false);
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
