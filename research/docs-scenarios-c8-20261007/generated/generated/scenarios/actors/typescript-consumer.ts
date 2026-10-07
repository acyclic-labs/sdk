// Generated from Rust scenario actors/typescript-consumer.
// Rust output SHA256: sha256:ece2cee6ef8b6565a9c4e9cd9dfb40e09be6c56562ac0bb02436c51fa05bcf17
import { ActorLimitsSchema, BindingSchema, CreateActorRequestSchema, SubscriptionSpecSchema, SubscriptionStartSchema } from "@acyclic-labs/actors/proto";
import { create, toBinary } from "@bufbuild/protobuf";

const request = create(CreateActorRequestSchema, {
  codeSha256: Uint8Array.from([17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17]),
  homeRegion: "eu",
  bindings: [create(BindingSchema, { name: "database", capability: "read", resource: "tenant-db" })],
  limits: create(ActorLimitsSchema, {
    handlerTimeoutMillis: BigInt("1000"),
    memoryBytes: BigInt("1024"),
    checkpointBytes: BigInt("1024"),
  }),
  subscriptions: [create(SubscriptionSpecSchema, { subscriptionId: "events", streamPath: "agents/a/events", start: create(SubscriptionStartSchema, { start: { case: "cursor", value: BigInt("0") } }), placementAnchor: true })],
  idempotencyKey: "create-typescript-consumer",
});
const encoded = toBinary(CreateActorRequestSchema, request);
if (encoded.length === 0) throw new Error("Rust Actors request encoded to an empty payload");
console.log(JSON.stringify({ homeRegion: request.homeRegion, encodedBytes: encoded.length }));
