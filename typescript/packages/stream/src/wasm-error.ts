import { is_stream_error_code, type StreamErrorCode } from "../generated/wasm/acyclic_stream_wasm.js";
import { StreamError, type StreamFailureCode } from "./types.js";

export function streamError(error: unknown, operation: string): Error {
  if (error instanceof StreamError) return error;
  let message: string;
  let rawCode: unknown;
  try {
    message = error instanceof Error ? error.message : String(error);
  } catch {
    message = "Stream operation failed";
  }
  try {
    rawCode = typeof error === "object" && error !== null ? (error as { readonly code?: unknown }).code : undefined;
  } catch {
    rawCode = undefined;
  }
  if (typeof rawCode !== "string" || !is_stream_error_code(rawCode)) {
    return error instanceof Error ? error : new Error(message);
  }
  let code: StreamFailureCode = rawCode as StreamErrorCode;
  if (rawCode === "not_found") code = operation === "read_commit" ? "commit_not_found" : "stream_not_found";
  else if (rawCode === "already_exists") code = "destination_exists";
  else if (rawCode === "capacity" && operation === "session") code = "capacity_exhausted";
  else if (rawCode === "prefix_not_retained" && operation === "commit") code = "invalid_argument";
  return new StreamError(code, message);
}
