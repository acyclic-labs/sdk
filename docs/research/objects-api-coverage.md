# Objects API coverage review

This review compares the Rust Objects v2 authority with the Rust-owned HTTP
guide, the generated TypeScript route facade, and the checked-in OpenAPI
projection. It was run against the isolated SDK worktree on 2026-10-04.

## Route and source coverage

The canonical inventory in
`rust/crates/objects/src/v2/mod.rs` (`HTTP_ROUTES`) contains 13 operations:

| Area | Operations |
| --- | --- |
| Buckets | `create`, `head`, `delete` |
| Objects | `put`, `get`, `head`, `delete`, `list` |
| Multipart | `create`, `upload-part`, `list-parts`, `complete`, `abort` |

The following independently checked projections each contain the same 13
paths and operation identities:

- `docs/objects-v2-http.md` route table: 13 rows.
- `typescript/packages/objects/src/generated-client.ts`: 13 entries in
  `OBJECTS_METHODS`.
- `research/additional-languages/target/bash/objects.json`: 13 OpenAPI paths.

The generated TypeScript facade has the same path, RPC, request, response, and
stream direction metadata as the Rust inventory. The extra `operationId` text
match in that file is the interface declaration, not an additional route.
There is no missing public Objects operation in these projections.

The Rust HTTP fixture exercises every entry in `HTTP_ROUTES` and asserts that
each route was called in
`rust/crates/objects/src/v2/http_tests.rs`.

## OpenAPI streaming projection defect and correction

The checked-in OpenAPI artifact currently describes all three streaming
operations as ordinary JSON bodies and responses:

```text
/v2/objects/objects/put          request application/json   ndjson=false
/v2/objects/objects/get          request application/json   ndjson=false
/v2/objects/multipart/upload-part request application/json  ndjson=false
```

The source of this output is the generic branch in
`rust/crates/sdk-openapi-prototype/src/lib.rs`: it always creates an
`application/json` request and response, then adds
`x-acyclic-http-streaming.ndjson: false` for non-Stream streaming RPCs.

That metadata disagreed with the authoritative Objects contract:

- `docs/objects-v2-http.md` requires `application/x-ndjson` for streaming
  bodies and documents the frame grammar.
- `rust/crates/objects/src/v2/http.rs` sends and validates
  `application/x-ndjson` for `put_stream`, `upload_part_stream`, and
  `get_stream`.
- `rust/crates/objects/src/v2/http_tests.rs` returns the NDJSON media type,
  splits frames across arbitrary transport chunks, and rejects wrong media,
  malformed, truncated, and oversized responses.

This was a generated-contract defect rather than a missing Objects route. An
OpenAPI consumer using only the artifact could select JSON framing and fail
against the Rust gateway even though the generated TypeScript package uses the
correct Rust/WASM HTTP adapter.

The Rust-owned OpenAPI projection now derives the media type by direction for
the Objects family. PUT and multipart upload-part requests emit
`application/x-ndjson` with JSON success responses; GET requests emit JSON with
an `application/x-ndjson` success response. The streaming extension is
`ndjson:true` for all three operations, while unary routes remain JSON.

The projection also exposes the Rust HTTP envelope rules in
`x-acyclic-objects-streaming`: request and response record types, header/body/
completion ordering, the `GetObjectResponse.error` terminal trailer, bounded
JSON/body frame sizes, pre-stream `ErrorDetail` responses, mutation identity
fields, and cancellation behavior. The default OpenAPI error response now
references the Rust `ErrorDetail` schema instead of an untyped default.

The focused regression
`objects_export_preserves_all_routes_external_timestamp_and_stream_direction`
asserts all three streaming directions and a unary bucket route. A disposable
generated output was also inspected and produced exactly these media sets:

```text
/v2/objects/objects/put           request x-ndjson  response json
/v2/objects/objects/get           request json      response x-ndjson
/v2/objects/multipart/upload-part request x-ndjson  response json
/v2/objects/buckets/create        request json      response json
```

## Follow-up for generated artifacts

The additional-language artifact owner should regenerate
`research/additional-languages/target/bash/objects.json` and add a regression
check against these emitted media types. The generic projection remains
unchanged for families whose HTTP projection is JSON polling or another
framing.

## Verification commands

The route comparison and artifact inspection used the checked-in Rust source,
generated facade, guide, and OpenAPI JSON. The existing Rust Objects HTTP
conformance test covers all routes and streaming semantics. The installed
browser package receipt is recorded in
`docs/research/objects-browser-http-qualification.md`; it independently
passed the Rust fixture upload/download, authentication, idempotency conflict,
and caller cancellation checks.
