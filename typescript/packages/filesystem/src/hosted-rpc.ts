import { type Client, ConnectError, Code } from "@connectrpc/connect";
import { type FilesystemService, OperationOptionsSchema } from "../generated/proto/filesystem/v2/filesystem_pb.js";
import { create } from "@bufbuild/protobuf";
import { type JoinOptions } from "./contracts.js";

export type HostedFsErrorCode =
  | "closed" | "invalid_response" | "limit" | "protocol" | "response_too_large" | "unsupported"
  | "cancelled" | "invalid_argument" | "deadline_exceeded" | "not_found" | "already_exists"
  | "permission_denied" | "resource_exhausted" | "failed_precondition" | "aborted" | "out_of_range"
  | "internal" | "unavailable" | "data_loss" | "unauthenticated" | "unknown";

export class HostedFsError extends Error {
  constructor(readonly code: HostedFsErrorCode, message: string) {
    super(message);
    this.name = "HostedFsError";
  }
}

export interface HostedClient {
  readonly rpc: Client<typeof FilesystemService>;
  readonly maximumResponseBytes: number;
  readonly maximumTransactionMutations: number;
  readonly maximumPageItems: number;
  readonly s3Credentials: boolean;
  readonly sourceReconciliation: boolean;
  closed: boolean;
}

export function operation(idempotencyKey?: Uint8Array) {
  const identity = idempotencyKey === undefined ? globalThis.crypto.getRandomValues(new Uint8Array(16)) : Uint8Array.from(idempotencyKey);
  requireBytes(identity, 16, "idempotency key");
  return create(OperationOptionsSchema, { idempotencyKey: identity });
}

export function assertOpen(client: HostedClient): void {
  if (client.closed) throw new HostedFsError("closed", "hosted filesystem is closed");
}

export function requirePageBound(client: HostedClient, value: number, name: string): void {
  if (value > client.maximumPageItems) {
    throw new HostedFsError("limit", `${name} exceeds the negotiated page limit`);
  }
}

export function requireResponseBound(client: HostedClient, value: bigint, name: string): void {
  if (value <= 0n || value > BigInt(client.maximumResponseBytes)) {
    throw new HostedFsError("limit", `${name} exceeds the negotiated response limit`);
  }
}

export function requireName(value: string): void { if (value.length === 0) throw new RangeError("name must be non-empty"); }

export function positiveSafeInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) throw new RangeError(`${name} must be positive`);
}

export function positiveU32(value: number, name: string): void {
  if (!Number.isInteger(value) || value <= 0 || value > 0xffff_ffff) throw new RangeError(`${name} must be a positive u32`);
}

export function positiveU64(value: bigint, name: string): void {
  if (value <= 0n || value > 0xffff_ffff_ffff_ffffn) throw new RangeError(`${name} must be a positive u64`);
}

export function nonnegativeU64(value: bigint, name: string): void {
  if (value < 0n || value > 0xffff_ffff_ffff_ffffn) throw new RangeError(`${name} must be a u64`);
}

export function required<T>(value: T | undefined, name: string): T {
  if (value === undefined) throw new HostedFsError("invalid_response", `${name} is absent`);
  return value;
}

export function requireBytes(value: Uint8Array, length: number, name: string): void {
  if (!(value instanceof Uint8Array) || value.byteLength !== length) {
    throw new HostedFsError("invalid_response", `${name} must contain ${length} bytes`);
  }
}

export function exactBytes(value: Uint8Array, length: number, name: string): Uint8Array {
  requireBytes(value, length, name);
  return Uint8Array.from(value);
}

export function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  if (left.length !== right.length) return false;
  let difference = 0;
  for (let index = 0; index < left.length; index++) difference |= left[index]! ^ right[index]!;
  return difference === 0;
}

export async function call<T>(request: Promise<T>): Promise<T> {
  try { return await request; }
  catch (error) {
    if (error instanceof HostedFsError) throw error;
    if (error instanceof ConnectError) {
      const codes: Record<Code, HostedFsErrorCode | undefined> = {
        [Code.Canceled]: "cancelled",
        [Code.Unknown]: undefined,
        [Code.InvalidArgument]: "invalid_argument",
        [Code.DeadlineExceeded]: "deadline_exceeded",
        [Code.NotFound]: "not_found",
        [Code.AlreadyExists]: "already_exists",
        [Code.PermissionDenied]: "permission_denied",
        [Code.ResourceExhausted]: "resource_exhausted",
        [Code.FailedPrecondition]: "failed_precondition",
        [Code.Aborted]: "aborted",
        [Code.OutOfRange]: "out_of_range",
        [Code.Unimplemented]: "unsupported",
        [Code.Internal]: "internal",
        [Code.Unavailable]: "unavailable",
        [Code.DataLoss]: "data_loss",
        [Code.Unauthenticated]: "unauthenticated",
      };
      throw new HostedFsError(codes[error.code] ?? "unknown", error.rawMessage);
    }
    throw error;
  }
}

export function boundedFetch(send: typeof globalThis.fetch, maximumBytes: number): typeof globalThis.fetch {
  return async (input, init) => {
    // Never forward the bearer token to a redirect target.
    const response = await send(input, { ...init, redirect: "error" });

    const declared = response.headers.get("content-length");
    if (declared !== null && /^\d+$/.test(declared) && BigInt(declared) > BigInt(maximumBytes)) {
      await response.body?.cancel();
      throw new HostedFsError("response_too_large", "filesystem response exceeds its local bound");
    }
    if (response.body === null) return response;
    let received = 0;
    const bounded = response.body.pipeThrough(new TransformStream<Uint8Array, Uint8Array>({
      transform(chunk, controller) {
        received += chunk.byteLength;
        if (received > maximumBytes) {
          controller.error(new HostedFsError("response_too_large", "filesystem response exceeds its local bound"));
        } else {
          controller.enqueue(chunk);
        }
      },
    }));
    return new Response(bounded, response);
  };
}

export function validateMergeLimits(value: Pick<JoinOptions, "maximumGenerations" | "maximumChanges" | "maximumConflicts">): void {
  positiveU32(value.maximumGenerations, "maximum generations");
  positiveU32(value.maximumChanges, "maximum changes");
  positiveU32(value.maximumConflicts, "maximum conflicts");
}
