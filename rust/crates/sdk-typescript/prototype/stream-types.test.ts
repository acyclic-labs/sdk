// Generated compile contract for the Rust-owned stream facade.
import { createStreamClient, type RustOwnedInvoker } from "./stream-metadata";
import type { InspectIdempotencyRequest, InspectIdempotencyResponse } from "../../../../typescript/packages/stream/generated/proto/stream/v2/stream_pb.js";

const invoker: RustOwnedInvoker = {
  invoke<TRequest, TResponse>(_method: unknown, _request: TRequest): Promise<TResponse> {
    throw new Error("compile-only");
  },
};
const client = createStreamClient(invoker);
const request = {} as InspectIdempotencyRequest;
const typedResult: Promise<InspectIdempotencyResponse> = client.inspectIdempotency(request);
void typedResult;
// @ts-expect-error request fields and message shape are generated and must reject arbitrary objects.
void client.inspectIdempotency({ madeUpField: true });
// @ts-expect-error a response cannot be assigned to a different generated message type.
const wrongResult: Promise<InspectIdempotencyRequest> = client.inspectIdempotency(request);
void wrongResult;
