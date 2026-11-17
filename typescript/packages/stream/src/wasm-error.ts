import { StreamError, type StreamFailureCode, type StreamRefusal } from "./types.js";

const knownCodes = new Set<StreamFailureCode>([
  "invalid_path", "invalid_argument", "limit_exceeded", "not_found", "already_exists",
  "prefix_not_retained", "out_of_range", "idempotency_mismatch", "capacity", "access_denied",
  "unavailable", "hierarchy_changed", "deadline_elapsed", "unsupported", "stream_not_found",
  "commit_not_found", "destination_exists", "capacity_exhausted", "configuration", "invalid_cursor",
  "invalid_page", "invalid_response", "provider_mismatch", "response_too_large", "transport", "revision_foreign",
  "failed",
]);

export function streamError(error: unknown, operation: string): Error {
  if (error instanceof StreamError) return error;
  let code: unknown;
  let message: string;
  if (typeof error === "object" && error !== null) {
    code = "code" in error ? (error as { readonly code?: unknown }).code : undefined;
    message = "message" in error && typeof (error as { readonly message?: unknown }).message === "string"
      ? (error as { readonly message: string }).message : String(error);
  } else message = String(error);
  if (typeof code !== "string") return new StreamError("unavailable", message);
  let mappedCode = code;
  if (mappedCode === "not_found") mappedCode = operation === "read_commit" ? "commit_not_found" : "stream_not_found";
  else if (mappedCode === "already_exists") mappedCode = "destination_exists";
  else if (mappedCode === "capacity") mappedCode = "capacity_exhausted";
  else if (mappedCode === "prefix_not_retained" && operation === "commit") mappedCode = "invalid_argument";
  const raw = typeof error === "object" && error !== null ? (error as { readonly refusal?: unknown }).refusal : undefined;
  const refusal = typeof raw === "object" && raw !== null ? raw as StreamRefusal : undefined;
  return new StreamError(knownCodes.has(mappedCode as StreamFailureCode) ? mappedCode as StreamFailureCode : "unavailable", message, undefined, refusal);
}
