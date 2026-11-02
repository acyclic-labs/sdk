import { publicGrpcErrorCode, publicHttpErrorCode } from "../generated/wasm/acyclic_stream_wasm.js";
import { StreamError, type StreamFailureCode } from "./types.js";

/** Project canonical service codes through the Rust-owned public vocabulary. */
export function streamFailure(rawCode: string, message: string, operation: string, status?: number): StreamError | undefined {
  const code = publicHttpErrorCode(rawCode, publicRoute(operation));
  return code === undefined ? undefined : new StreamError(code as StreamFailureCode, message, status);
}

export function grpcStreamError(status: number, message: string, operation: string): StreamError {
  const code = publicGrpcErrorCode(status, message, publicRoute(operation));
  return new StreamError((code ?? "unavailable") as StreamFailureCode, message);
}

function publicRoute(operation: string): string {
  return operation === "read_commit" ? "commits/read" : operation;
}

export function streamError(error: unknown, operation: string): Error {
  if (error instanceof StreamError) return error;
  let message: string;
  let rawCode: unknown;
  try {
    const detail = typeof error === "object" && error !== null ? (error as { readonly message?: unknown }).message : undefined;
    message = typeof detail === "string" ? detail : String(error);
  } catch {
    message = "Stream operation failed";
  }
  try {
    rawCode = typeof error === "object" && error !== null ? (error as { readonly code?: unknown }).code : undefined;
  } catch {
    rawCode = undefined;
  }
  const failure = typeof rawCode === "string"
    ? streamFailure(rawCode, message, operation)
    : undefined;
  return failure ?? (error instanceof Error ? error : new Error(message));
}
