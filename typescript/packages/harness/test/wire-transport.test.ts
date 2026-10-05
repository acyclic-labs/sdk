import { expect, test } from "bun:test";
import { create } from "@bufbuild/protobuf";
import {
  CancelResponseSchema,
  CompletionState,
  DeliverySchema,
  ObserveRequestSchema,
  OperationStatusSchema,
  ResumeRequestSchema,
} from "../generated/proto/harness/v2/harness_pb.js";
import { HandshakeRequestSchema, HandshakeResponseSchema } from "../generated/proto/protocol/v1/protocol_pb.js";
import {
  EmbeddedWireTransport,
  RustBrowserWireTransport,
  WireError,
  type WireTransport,
} from "../src/index.js";

const negotiation = create(HandshakeRequestSchema, {
  protocol: {
    version: "2",
    descriptorDigest: "8efc8c682b2ba1025b1221dd203685bdf999d04e87568fad3acdf0e428bd84cf",
  },
  required: { capabilities: [] },
});
const handshake = create(HandshakeResponseSchema, {
  protocol: negotiation.protocol,
  supported: { capabilities: [] },
});
const resume = create(ResumeRequestSchema, {});
const delivery = create(DeliverySchema, {
  authority: { kind: 2, id: "conversation-1" },
  generation: "generation-1",
  fromRevision: 0n,
  throughRevision: 0n,
  live: true,
});

test("Rust browser transport is the public remote transport", () => {
  const transport: WireTransport = new RustBrowserWireTransport(
    "https://example.test",
    "token",
    negotiation,
  );
  expect(transport).toBeInstanceOf(RustBrowserWireTransport);
});

test("embedded host preserves the generated delivery and Rust validation boundary", async () => {
  const embedded = new EmbeddedWireTransport({
    async handshake() { return handshake; },
    async *replay() { yield delivery; },
    async submit() {},
    async observe(request) {
      return create(OperationStatusSchema, {
        protocol: request.protocol,
        owner: request.owner,
        operation: { operationId: request.operationId },
        state: CompletionState.RUNNING,
      });
    },
    async cancel(request) {
      return create(CancelResponseSchema, {
        operation: { operationId: request.operationId, idempotencyKey: request.idempotencyKey },
        status: {
          protocol: request.protocol,
          owner: request.owner,
          operation: { operationId: request.operationId },
          state: CompletionState.CANCELLED,
        },
      });
    },
  }, negotiation);
  const connection = await embedded.connect(resume);
  expect((await connection[Symbol.asyncIterator]().next()).value).toEqual(delivery);
  const observed = await connection.observe(create(ObserveRequestSchema, {
    owner: { kind: 5, id: "owner" },
    operationId: "01010101-0101-0101-0101-010101010101",
    scope: { id: "control", capabilities: ["operation:observe"], issuer: "runtime", proof: new Uint8Array(32) },
  }));
  expect(observed.state).toBe(CompletionState.RUNNING);
});

test("embedded control rejects a mismatched operation status", async () => {
  const embedded = new EmbeddedWireTransport({
    async handshake() { return handshake; },
    async *replay() {},
    async submit() {},
    async observe(request) {
      return create(OperationStatusSchema, {
        protocol: request.protocol,
        owner: request.owner,
        operation: { operationId: "02020202-0202-0202-0202-020202020202" },
        state: CompletionState.RUNNING,
      });
    },
    async cancel() { return create(CancelResponseSchema); },
  }, negotiation);
  const connection = await embedded.connect(resume);
  await expect(connection.observe(create(ObserveRequestSchema, {
    owner: { kind: 5, id: "owner" },
    operationId: "01010101-0101-0101-0101-010101010101",
    scope: { id: "control", capabilities: ["operation:observe"], issuer: "runtime", proof: new Uint8Array(32) },
  }))).rejects.toBeInstanceOf(WireError);
});

test("Rust browser transport rejects an aborted connection before loading a client", async () => {
  const controller = new AbortController();
  controller.abort(new Error("cancelled"));
  await expect(new RustBrowserWireTransport("https://example.test", "token", negotiation)
    .connect(resume, controller.signal)).rejects.toThrow("cancelled");
});
