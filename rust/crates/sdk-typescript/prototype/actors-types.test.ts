// Generated compile contract for the Rust-owned actors facade.
import { createActorsClient, type RustOwnedInvoker } from "./actors-metadata";
import type { CreateActorRequest, CreateActorResponse } from "../../../../typescript/packages/actors/generated/proto/actors/v1/actors_pb.js";

const invoker: RustOwnedInvoker = {
  invoke<TRequest, TResponse>(_method: unknown, _request: TRequest): Promise<TResponse> {
    throw new Error("compile-only");
  },
};
const client = createActorsClient(invoker);
const request = {} as CreateActorRequest;
const typedResult: Promise<CreateActorResponse> = client.createActor(request);
void typedResult;
// @ts-expect-error request fields and message shape are generated and must reject arbitrary objects.
void client.createActor({ madeUpField: true });
// @ts-expect-error a response cannot be assigned to a different generated message type.
const wrongResult: Promise<CreateActorRequest> = client.createActor(request);
void wrongResult;
