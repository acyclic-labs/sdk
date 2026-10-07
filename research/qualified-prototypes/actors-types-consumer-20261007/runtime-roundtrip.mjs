import assert from "node:assert/strict";
import { ActorsClient } from "@acyclic-labs/actors";
import {
  AddSubscriptionRequestSchema,
  AddSubscriptionResponseSchema,
  CheckpointActorRequestSchema,
  CheckpointActorResponseSchema,
  CreateActorRequestSchema,
  CreateActorResponseSchema,
  InspectActorRequestSchema,
  InspectActorResponseSchema,
  InvokeActorRequestSchema,
  InvokeActorResponseSchema,
  RemoveSubscriptionRequestSchema,
  RemoveSubscriptionResponseSchema,
  ResumeSubscriptionRequestSchema,
  ResumeSubscriptionResponseSchema,
  UpdateActorRequestSchema,
  UpdateActorResponseSchema,
} from "@acyclic-labs/actors/proto";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";

const digest = new Uint8Array(32).fill(1);
const limits = { handlerTimeoutMillis: 1n, memoryBytes: 2n, checkpointBytes: 3n };
const currentHead = { start: { case: "currentHead", value: true } };
const schemas = {
  createActor: [CreateActorRequestSchema, CreateActorResponseSchema],
  updateActor: [UpdateActorRequestSchema, UpdateActorResponseSchema],
  inspectActor: [InspectActorRequestSchema, InspectActorResponseSchema],
  addSubscription: [AddSubscriptionRequestSchema, AddSubscriptionResponseSchema],
  removeSubscription: [RemoveSubscriptionRequestSchema, RemoveSubscriptionResponseSchema],
  resumeSubscription: [ResumeSubscriptionRequestSchema, ResumeSubscriptionResponseSchema],
  checkpointActor: [CheckpointActorRequestSchema, CheckpointActorResponseSchema],
  invokeActor: [InvokeActorRequestSchema, InvokeActorResponseSchema],
};
const seen = [];

const binding = {
  async connect() {
    return Object.fromEntries(Object.entries(schemas).map(([operation, [input, output]]) => [
      operation,
      async bytes => {
        const request = fromBinary(input, bytes);
        seen.push(operation);
        if (operation === "addSubscription") {
          assert.equal(request.subscription.start.start.case, "currentHead");
          assert.equal(request.subscription.start.start.value, true);
        }
        if (operation === "invokeActor") {
          assert.deepEqual(request.body, new Uint8Array([4, 5]));
          assert.equal(request.headers[0].value, "application/json");
        }
        const response = operation === "invokeActor"
          ? { status: 201, body: new Uint8Array([8, 9]), headers: [{ name: "location", value: "/result" }] }
          : {};
        return toBinary(output, create(output, response));
      },
    ]));
  },
};

const client = new ActorsClient({ endpoint: "https://actors.example.test", token: "fixture", binding });
await client.createActor({ codeSha256: digest, homeRegion: "eu", bindings: [], limits, subscriptions: [], idempotencyKey: "create" });
await client.updateActor({ actorId: "actor-a", codeSha256: digest, bindings: [], limits, expectedConfigurationRevision: 0n, idempotencyKey: "update" });
await client.inspectActor({ actorId: "actor-a" });
await client.addSubscription({ actorId: "actor-a", subscription: { subscriptionId: "sub", streamPath: "events", start: currentHead, placementAnchor: false }, idempotencyKey: "add" });
await client.removeSubscription({ actorId: "actor-a", subscriptionId: "sub", idempotencyKey: "remove" });
await client.resumeSubscription({ actorId: "actor-a", subscriptionId: "sub", idempotencyKey: "resume" });
await client.checkpointActor({ actorId: "actor-a", idempotencyKey: "checkpoint" });
const invoke = await client.invokeActor({ actorId: "actor-a", method: "POST", url: "/invoke", body: new Uint8Array([4, 5]), headers: [{ name: "content-type", value: "application/json" }] });

assert.deepEqual(seen, Object.keys(schemas));
assert.equal(invoke.status, 201);
assert.deepEqual(invoke.body, new Uint8Array([8, 9]));
assert.deepEqual(invoke.headers, [{ name: "location", value: "/result" }]);
console.log(`Actors semantic/wire roundtrip passed for ${seen.length} operations`);
