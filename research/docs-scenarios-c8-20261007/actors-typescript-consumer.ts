// Generated from Rust scenario actors/typescript-consumer.
// Rust output SHA256: sha256:6140cd4fa38bf36917d263e40a5ae09036dc4f234d739d26a06ef0bcfe19edce
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
