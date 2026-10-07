# Native error alias audit (research only)

Exact source under review: `Q:\sdk\work\stream-main-port`, revision `1a3ec9481b6d19722bbc23fbef1f34e52657b355`. No production files were changed.

## Current mappings

- Rust N-API emits wire codes from `rust/crates/stream-napi/src/lib.rs::stream_error`: `not_found`, `already_exists`, `capacity`, and other base codes.
- `typescript/packages/stream/src/native.ts::nativeError` maps `not_found` by operation, `already_exists` to `destination_exists`, `capacity` to `capacity_exhausted`, and commit-only `prefix_not_retained` to `invalid_argument`.
- `typescript/packages/stream/src/memory.ts::streamError` repeats the `not_found`, `already_exists`, and commit-only `prefix_not_retained` aliases but currently does not map `capacity` to `capacity_exhausted`; Memory can therefore expose the raw `capacity` code while native/gRPC/HTTP expose the public alias.
- `grpc.ts` has transport-specific Connect status mapping. `http.ts` already delegates public code projection to the Rust WASM `publicHttpErrorCode` export, with only the public alias allow-list in TypeScript.

## Smallest follow-up candidate

Extract one TypeScript helper for Rust wire-code to public-code mapping, parameterized by operation, and use it from `native.ts` and `memory.ts`. The helper should cover the four existing operation-sensitive aliases and preserve unknown-code fallback to `unavailable`. Add a focused provider-parity test for `capacity` and `read_commit`/`not_found`.

Do not move Connect status handling into this helper: gRPC receives protocol statuses rather than N-API/WASM wire codes. Do not replace HTTP's Rust `publicHttpErrorCode` authority with a TypeScript table.

A second low-risk cleanup is replacing `native.ts`'s locally maintained known-code `Set` with the Rust WASM `is_stream_error_code` validator plus a small public-alias check, matching `memory.ts`; this should follow the shared mapper so aliases remain explicit.