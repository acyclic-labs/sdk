import { ErrorCode } from "../generated/proto/harness/v1/harness_pb.js";

/** HTTP status for a wire error code. UNSPECIFIED maps to 500. */
export function httpStatusForError(code: ErrorCode): number {
  switch (code) {
    case ErrorCode.NOT_FOUND: return 404;
    case ErrorCode.CONFLICT: return 409;
    case ErrorCode.UNSUPPORTED: return 422;
    case ErrorCode.INVALID: return 400;
    case ErrorCode.UNAUTHORIZED: return 403;
    case ErrorCode.STORAGE: return 503;
    case ErrorCode.INDETERMINATE: return 503;
    default: return 500;
  }
}

/** Inverse of {@link httpStatusForError}; ambiguous statuses read as INDETERMINATE. */
export function errorCodeFromHttpStatus(status: number): ErrorCode | undefined {
  switch (status) {
    case 404: return ErrorCode.NOT_FOUND;
    case 409: return ErrorCode.CONFLICT;
    case 422: return ErrorCode.UNSUPPORTED;
    case 400: return ErrorCode.INVALID;
    case 403: return ErrorCode.UNAUTHORIZED;
    case 503: return ErrorCode.INDETERMINATE;
    default: return undefined;
  }
}

/** gRPC status code for a wire error code. UNSPECIFIED maps to INTERNAL (13). */
export function grpcCodeForError(code: ErrorCode): number {
  switch (code) {
    case ErrorCode.NOT_FOUND: return 5;
    case ErrorCode.CONFLICT: return 10;
    case ErrorCode.UNSUPPORTED: return 12;
    case ErrorCode.INVALID: return 3;
    case ErrorCode.UNAUTHORIZED: return 7;
    case ErrorCode.STORAGE: return 14;
    case ErrorCode.INDETERMINATE: return 14;
    default: return 13;
  }
}

/** Inverse of {@link grpcCodeForError}; ambiguous codes read as INDETERMINATE. */
export function errorCodeFromGrpcCode(code: number): ErrorCode | undefined {
  switch (code) {
    case 5: return ErrorCode.NOT_FOUND;
    case 10: return ErrorCode.CONFLICT;
    case 12: return ErrorCode.UNSUPPORTED;
    case 3: return ErrorCode.INVALID;
    case 7: return ErrorCode.UNAUTHORIZED;
    case 14: return ErrorCode.INDETERMINATE;
    default: return undefined;
  }
}
