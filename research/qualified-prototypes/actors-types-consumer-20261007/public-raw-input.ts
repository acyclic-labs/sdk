import { semantic } from '@acyclic-labs/actors';

// A real installed consumer starts with ordinary runtime values.
const actorId: string = 'actor-a';
const codeSha256 = new Uint8Array(32);
const positiveLimit: bigint = 1n;

// These assignments intentionally have no casts or platform-specific imports.
// They should compile if the public package offers Rust-backed branded factories.
const inspect: semantic.InspectActorRequest = { actorId };
const create: semantic.CreateActorRequest = {
  codeSha256,
  homeRegion: 'eu',
  bindings: [],
  limits: {
    handlerTimeoutMillis: positiveLimit,
    memoryBytes: positiveLimit,
    checkpointBytes: positiveLimit,
  },
  subscriptions: [],
  idempotencyKey: 'request-1',
};

void inspect;
void create;
void semantic;
