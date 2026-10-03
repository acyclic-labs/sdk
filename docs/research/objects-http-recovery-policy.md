# Objects HTTP recovery policy

The current Rust Objects HTTP API does not support endpoint pools, endpoint rotation, or automatic retry. This is a deliberate transport boundary, not an omitted browser wrapper feature.

## Source evidence

`rust/crates/objects/src/v2/http.rs` defines `HttpObjects` with one `endpoint: Url`. Its `post` method joins one route to that URL and calls `reqwest::Client::send()` once. There is no preferred-endpoint state, attempt deadline, retry classifier, or alternate endpoint list.

The two streaming operations cannot safely acquire implicit replay without a different API contract:

- `put_stream` and `upload_part_stream` consume a caller-owned `UploadBody` once. Retrying after an uncertain response would require buffering or reopening the source, neither of which is part of `UploadBody`.
- `get_stream` returns a live response stream. It validates the header and remaining byte count but has no resumable range cursor. Restarting a failed stream would risk duplicate bytes to the caller.

Unary mutation messages may carry a caller-owned idempotency identity. Rust validates and persists that identity in the Objects provider, but the HTTP adapter does not silently replay the operation. A caller can explicitly reconcile an uncertain result with the same identity. Reads and conditional operations likewise remain single-attempt HTTP calls.

This differs from `rust/crates/stream/src/grpc.rs`, whose native gRPC API explicitly accepts up to 16 HTTPS endpoints and owns bounded endpoint rotation and cursor-preserving follow recovery. That Stream-specific contract cannot be projected onto Objects HTTP without adding an endpoint-pool API, replayability rules for every route, and a resumable streaming protocol.

## Browser consequence

The browser Objects entrypoint accepts one `endpoint` and selects the Rust-emitted browser HTTP option. Browser `fetch` cancellation is supported and qualified separately in `docs/research/objects-browser-http-qualification.md`. Browser recovery is therefore intentionally not claimed. Adding an `endpoint[]` option only to the TypeScript facade would create a second policy and would not make streamed upload or download replay safe.

The endpoint-alone default remains the stable compatibility path. A future Rust-owned endpoint-pool API could be added only with explicit route-level replay classification, bounded attempt/deadline metadata, and a resumable read contract; no such API exists in the current Objects v2 protocol.

## Verification

The Rust HTTP fixture and client conformance sources were inspected at the frozen source revision used by the browser qualification. The existing Rust and installed browser checks remain the evidence for the current boundary:

```text
cd typescript/packages/objects
node --test test/v2-conformance.mjs
5 passed, 0 failed

bun test test
6 pass, 0 fail
```

The focused Rust HTTP test invocation was started with `cargo test --manifest-path rust/crates/objects/Cargo.toml --features http --locked --offline --lib v2::http_tests`; concurrent workspace builds held the Cargo package lock during this review, so no additional terminal result is claimed here. No product or TypeScript files were changed for this policy decision.
