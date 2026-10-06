// Generated compile contract for the Rust-owned objects facade.
import { createObjectsClient, type RustOwnedInvoker } from "./objects-metadata";
import type { CreateBucketRequest, Bucket } from "../../../../typescript/packages/objects/generated/proto/objects/v2/objects_pb.js";

const invoker: RustOwnedInvoker = {
  invoke<TRequest, TResponse>(_method: unknown, _request: TRequest): Promise<TResponse> {
    throw new Error("compile-only");
  },
};
const client = createObjectsClient(invoker);
const request = {} as CreateBucketRequest;
const typedResult: Promise<Bucket> = client.createBucket(request);
void typedResult;
// @ts-expect-error request fields and message shape are generated and must reject arbitrary objects.
void client.createBucket({ madeUpField: true });
// @ts-expect-error a response cannot be assigned to a different generated message type.
const wrongResult: Promise<CreateBucketRequest> = client.createBucket(request);
void wrongResult;
