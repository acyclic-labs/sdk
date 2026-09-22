import { expect, test } from "bun:test";
import { ErrorCode } from "../src/proto.js";
import {
  errorCodeFromGrpcCode,
  errorCodeFromHttpStatus,
  grpcCodeForError,
  httpStatusForError,
} from "../src/index.js";

const vector = JSON.parse(
  await Bun.file(new URL("../../../../conformance/vectors/harness/error-mapping-v1.json", import.meta.url)).text(),
) as { version: number; mappings: Array<{ code: string; http_status: number; grpc_code: number }> };

const named: Record<string, ErrorCode> = {
  NOT_FOUND: ErrorCode.NOT_FOUND,
  CONFLICT: ErrorCode.CONFLICT,
  UNSUPPORTED: ErrorCode.UNSUPPORTED,
  INVALID: ErrorCode.INVALID,
  UNAUTHORIZED: ErrorCode.UNAUTHORIZED,
  STORAGE: ErrorCode.STORAGE,
  INDETERMINATE: ErrorCode.INDETERMINATE,
};

test("error status table matches the conformance vector", () => {
  expect(vector.mappings.length).toBe(Object.keys(named).length);
  for (const mapping of vector.mappings) {
    const code = named[mapping.code];
    expect(code).toBeDefined();
    expect(httpStatusForError(code)).toBe(mapping.http_status);
    expect(grpcCodeForError(code)).toBe(mapping.grpc_code);
    expect(httpStatusForError(errorCodeFromHttpStatus(mapping.http_status)!)).toBe(mapping.http_status);
    expect(grpcCodeForError(errorCodeFromGrpcCode(mapping.grpc_code)!)).toBe(mapping.grpc_code);
  }
  expect(httpStatusForError(ErrorCode.UNSPECIFIED)).toBe(500);
  expect(grpcCodeForError(ErrorCode.UNSPECIFIED)).toBe(13);
  expect(errorCodeFromHttpStatus(418)).toBeUndefined();
  expect(errorCodeFromGrpcCode(2)).toBeUndefined();
});
