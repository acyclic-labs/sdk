import type { Context, CreateContextRequest, InferenceTransportKind, Item } from "../src/index.js";
import { INFERENCE_HANDSHAKE, INFERENCE_OPERATIONS, INFERENCE_REMOTE_POLICY } from "../src/generated-client.js";

// The service returns generated Item messages. A caller's input subtype cannot
// be recovered from a revision or asserted across a server-side mutation.
declare const context: Context;
const items: Promise<readonly Item[]> = context.items();
void items;

// @ts-expect-error Context has no caller-chosen item subtype.
type ForgedContext = Context<Item & { readonly trusted: true }>;
void (undefined as unknown as ForgedContext);

const handshakeRoute: "/v1/sdk/inference/handshake" = INFERENCE_HANDSHAKE.route;
const handshakeVersion: "inference.customer.v1" = INFERENCE_HANDSHAKE.version;
const transport: "http" = INFERENCE_REMOTE_POLICY.transport.native[0].kind;
const operationRpc: "inference.customer.v1.ContextsService/Create" =
  INFERENCE_OPERATIONS["inference.customer.v1.ContextsService/Create"].rpc;
void handshakeRoute;
void handshakeVersion;
void transport;
void operationRpc;

// @ts-expect-error Rust's installed transport policy does not expose gRPC for Inference.
const unsupportedTransport: InferenceTransportKind = "grpc";
void unsupportedTransport;

// @ts-expect-error operation identities are generated closed keys, not arbitrary strings.
void INFERENCE_OPERATIONS["inference.customer.v1.ContextsService/Unknown"];

// @ts-expect-error protobuf request fields are Rust-generated and reject handwritten fields.
const forgedRequest: CreateContextRequest = { madeUpField: true };
void forgedRequest;
