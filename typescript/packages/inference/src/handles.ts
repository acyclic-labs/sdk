import { create } from "@bufbuild/protobuf";
import { RequestIdentitySchema, type RequestIdentity } from "../generated/proto/inference/v1/inference_pb.js";
import type { InferenceClient } from "./index.js";
import { InferenceProtocolError } from "./contract.js";
import { INFERENCE_FIXED_WIDTHS } from "../generated/widths.js";

declare const inferenceBrand: unique symbol;
export type ContextRevision = Uint8Array & { readonly [inferenceBrand]: "ContextRevision" };
export type RunId = Uint8Array & { readonly [inferenceBrand]: "RunId" };
/** The released v1 protocol uses the stable Run identity as its recovery operation identity. */
export type OperationId = RunId;
export type WarmCommitment = Uint8Array & { readonly [inferenceBrand]: "WarmCommitment" };
export type ItemId = Uint8Array & { readonly [inferenceBrand]: "ItemId" };
export type ExecutionProfile = Uint8Array & { readonly [inferenceBrand]: "ExecutionProfile" };
export function contextRevision(value: Uint8Array): ContextRevision { return brandedBytes<ContextRevision>(value, "context revision", INFERENCE_FIXED_WIDTHS.contextRevision); }
export function runId(value: Uint8Array): RunId { return brandedBytes<RunId>(value, "run ID", INFERENCE_FIXED_WIDTHS.runId); }
export function operationId(value: Uint8Array): OperationId { return runId(value); }
export function warmCommitment(value: Uint8Array): WarmCommitment { return brandedBytes<WarmCommitment>(value, "warm commitment", INFERENCE_FIXED_WIDTHS.warmCommitment); }
/** The protobuf contract leaves item identity bytes opaque and unconstrained. */
export function itemId(value: Uint8Array): ItemId { if (!(value instanceof Uint8Array)) throw new TypeError("item ID must be a Uint8Array"); return Uint8Array.from(value) as ItemId; }
export function executionProfile(value: Uint8Array): ExecutionProfile { return brandedBytes<ExecutionProfile>(value, "execution profile", INFERENCE_FIXED_WIDTHS.executionProfile); }
/** Retains one client-instance identity while preparing explicit request identities. */
export class Inference {
  readonly #clientInstance = randomIdentity();
  constructor(readonly client: InferenceClient) {}
  static async fromEnv(environment: Readonly<Record<string, string | undefined>> = runtimeEnvironment()): Promise<Inference> { const endpoint = requiredEnvironment(environment, "ACYCLIC_INFERENCE_ENDPOINT"); const token = requiredEnvironment(environment, "ACYCLIC_API_KEY"); const { HttpInferenceTransport, InferenceClient } = await import("./index.js"); return new Inference(new InferenceClient(new HttpInferenceTransport(endpoint, () => ({ authorization: `Bearer ${token}` })))); }
  identity(): RequestIdentity { return create(RequestIdentitySchema, { clientInstance: this.#clientInstance.slice(), requestId: randomIdentity() }); }
}

function brandedBytes<Brand extends Uint8Array>(value: Uint8Array, name: string, length: number): Brand { if (!(value instanceof Uint8Array) || value.byteLength !== length) throw new TypeError(`${name} must be exactly ${length} bytes`); return Uint8Array.from(value) as Brand; }
const randomIdentity = (): Uint8Array => crypto.getRandomValues(new Uint8Array(INFERENCE_FIXED_WIDTHS.requestId));
const runtimeEnvironment = (): Readonly<Record<string, string | undefined>> => (globalThis as typeof globalThis & { process?: { env?: Readonly<Record<string, string | undefined>> } }).process?.env ?? {};
function requiredEnvironment(environment: Readonly<Record<string, string | undefined>>, name: string): string { const value = environment[name]; if (!value?.trim()) throw new InferenceProtocolError(`${name} is required`); return value; }
