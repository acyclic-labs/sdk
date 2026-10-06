import { describe, expect, test } from "bun:test";
import { create } from "@bufbuild/protobuf";
import { InvokeActorRequestSchema, type RustOwnedPublicInvokeActorRequest } from "../src/index.js";
import { createActorsGrpcClient } from "../src/grpc.js";

const clientOptions = { endpoint: "https://127.0.0.1:9", token: "fixture-token" };

describe("Actors gRPC Rust-owned admission", () => {
  test("rejects invalid invocation fields before opening the application RPC", async () => {
    const client = createActorsGrpcClient(clientOptions);
    const emptyActor = create(InvokeActorRequestSchema, { actorId: "", method: "run" }) as unknown as RustOwnedPublicInvokeActorRequest;
    const invalidMethod = create(InvokeActorRequestSchema, { actorId: "actor-1", method: "GET\r\nX: forged" }) as unknown as RustOwnedPublicInvokeActorRequest;

    await expect(client.invokeActor(emptyActor)).rejects.toThrow(TypeError);
    await expect(client.invokeActor(invalidMethod)).rejects.toThrow(TypeError);
  });

  test("defers endpoint validation until the first RPC", async () => {
    const client = createActorsGrpcClient({ ...clientOptions, endpoint: "http://127.0.0.1:9" });
    const request = create(InvokeActorRequestSchema, { actorId: "actor-1", method: "run" }) as unknown as RustOwnedPublicInvokeActorRequest;

    await expect(client.invokeActor(request)).rejects.toThrow(TypeError);
  });

  test("defers credential validation until the first RPC", async () => {
    const client = createActorsGrpcClient({ ...clientOptions, token: "fixture\r\nforged" });
    const request = create(InvokeActorRequestSchema, { actorId: "actor-1", method: "run" }) as unknown as RustOwnedPublicInvokeActorRequest;

    await expect(client.invokeActor(request)).rejects.toThrow(TypeError);
  });
});
