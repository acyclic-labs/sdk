import type {
  ActorId,
  CodeSha256,
  CreateActorRequest,
  CreateActorResponse,
  PositiveU64,
  SubscriptionStart,
} from "@acyclic-labs/actors/types";

const plainId = "actor-1";
// @ts-expect-error semantic ActorId must come from the Rust validator.
const badActorId: ActorId = plainId;

const rawDigest = new Uint8Array(32);
// @ts-expect-error raw bytes are not the branded CodeSha256 semantic value.
const badDigest: CodeSha256 = rawDigest;

// @ts-expect-error PositiveU64 is nominal and cannot be supplied as arbitrary bigint.
const badPositive: PositiveU64 = 0n;

// @ts-expect-error the semantic oneof admits the literal true payload only.
const falseCurrentHead: SubscriptionStart = {
  start: { case: "currentHead", value: false },
};

// @ts-expect-error the semantic oneof is required after Rust ingress validation.
const missingStart: SubscriptionStart = {};

declare const request: CreateActorRequest;
// @ts-expect-error semantic request collections must be immutable at the public boundary.
request.bindings.push({ name: "storage", capability: "read", resource: "bucket/a" });

declare const response: CreateActorResponse;
// @ts-expect-error semantic response records must be immutable to consumers.
response.actor = null;

void [badActorId, badDigest, badPositive, falseCurrentHead, missingStart, request, response];
