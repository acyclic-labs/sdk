// Generated compile contract for the Rust-owned workers facade.
import { createWorkersClient, type RustOwnedInvoker } from "./workers-metadata";
import type { PublishVersionRequest, PublishVersionResponse } from "../../../../typescript/packages/workers/generated/proto/workers/v1/workers_pb.js";

const invoker: RustOwnedInvoker = {
  invoke<TRequest, TResponse>(_method: unknown, _request: TRequest): Promise<TResponse> {
    throw new Error("compile-only");
  },
};
const client = createWorkersClient(invoker);
const request = {} as PublishVersionRequest;
const typedResult: Promise<PublishVersionResponse> = client.publishVersion(request);
void typedResult;
// @ts-expect-error request fields and message shape are generated and must reject arbitrary objects.
void client.publishVersion({ madeUpField: true });
// @ts-expect-error a response cannot be assigned to a different generated message type.
const wrongResult: Promise<PublishVersionRequest> = client.publishVersion(request);
void wrongResult;
