import { describe, expect, test } from "bun:test";
import { create, InvokeActorRequestSchema, type RustOwnedPublicInvokeActorRequest } from "../src/index.js";
import { createActorsGrpcClient } from "../src/grpc.js";

const clientOptions = { endpoint: "http://127.0.0.1:9", token: "fixture-token" };

describe("Actors gRPC Rust-owned admission", () => {
  test("rejects invalid invocation fields before opening the application RPC", () => {
    const client = createActorsGrpcClient(clientOptions);
    const emptyActor = create(InvokeActorRequestSchema, { actorId: "", method: "run" }) as unknown as RustOwnedPublicInvokeActorRequest;
    const emptyMethod = create(InvokeActorRequestSchema, { actorId: "actor-1", method: "" }) as unknown as RustOwnedPublicInvokeActorRequest;

    expect(() => client.invokeActor(emptyActor)).toThrow(TypeError);
    expect(() => client.invokeActor(emptyMethod)).toThrow(TypeError);
  });
});
