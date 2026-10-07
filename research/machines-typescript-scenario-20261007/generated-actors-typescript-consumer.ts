// Generated from Rust scenario actors/typescript-consumer.
// Rust output SHA256: sha256:252356320c11e7ede9d50c3ea96164ff646dd28825fbf24685a483805169b162
import { CreateActorRequestSchema } from "@acyclic-labs/actors/proto";
import { create, toBinary } from "@bufbuild/protobuf";

const request = create(CreateActorRequestSchema, {
  codeSha256: Uint8Array.from([17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17]),
  homeRegion: "eu",
  bindings: [],
  limits: {
    handlerTimeoutMillis: BigInt("1000"),
    memoryBytes: BigInt("1024"),
    checkpointBytes: BigInt("1024"),
  },
  subscriptions: [],
  idempotencyKey: "create-typescript-consumer",
});
const encoded = toBinary(CreateActorRequestSchema, request);
if (encoded.length === 0) throw new Error("Rust Actors request encoded to an empty payload");
console.log(JSON.stringify({ homeRegion: request.homeRegion, encodedBytes: encoded.length }));
