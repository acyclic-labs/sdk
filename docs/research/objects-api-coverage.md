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

The metadata is source-bound: record names and frame oneofs are checked against
the supplied `ContractSpec`, the decoded body-frame ceiling is read from the
Rust `ObjectsLimit` enum, and the 128 KiB encoded-record ceiling is exposed as
`OBJECTS_HTTP_JSON_FRAME_BYTES` in the Rust contract metadata. A mutation vector
changes the modeled frame field and limit and verifies that the envelope
metadata disappears or changes accordingly; this prevents the projection from
silently preserving stale hand-authored transport facts.

The product-side Rust HTTP gateway now exposes the same two limits from
`acyclic_objects::v2`: `HTTP_JSON_FRAME_BYTES` is the encoded JSON/NDJSON
record ceiling and `HTTP_BODY_FRAME_BYTES` is the decoded body-frame ceiling
derived from `ObjectsLimit::MaxBodyFrameBytes`. Native HTTP framing, upload
chunking, response validation, the conformance fixture, and the WASM adapter
all consume these product constants. The WASM boundary exposes
`objects_v2_http_json_frame_bytes()` and
`objects_v2_http_body_frame_bytes()` so a browser package can consume the
compiled Rust values instead of reproducing them in TypeScript. The standalone
contract-wire/OpenAPI prototype retains its model-side
`OBJECTS_HTTP_JSON_FRAME_BYTES` because it cannot depend on the publishable
Objects crate; the OpenAPI metadata test and product Rust tests keep the two
contract boundaries reviewable.

The Rust JSON encoder has golden vectors for the actual streaming messages:
`PutObjectRequest.body` serializes bytes `[0, 255]` as `{"body":"AP8="}`,
`PutObjectRequest.complete` serializes as `{"complete":true}`, and a
`GetObjectResponse.error` record preserves the request id while emitting the
enum error code as a JSON string. These vectors are checked against the same
descriptor-driven encoder used by the HTTP gateway, not a hand-written OpenAPI
fixture.

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

## Installed package qualification and source receipt

On 2026-10-04, a fresh `wasm-release` build was generated with
`node scripts/build-objects-wasm.mjs`, followed by `npm pack --ignore-scripts
--json` for `@acyclic-labs/objects@0.2.0`. The archive was installed into a
disposable consumer and exercised through the public `@acyclic-labs/objects/http`
and `@acyclic-labs/objects/proto` entrypoints. The consumer uploaded and
downloaded a 135,000-byte value, observed five upload records, and verified
the reconstructed bytes and Rust-owned frame boundaries with `bun smoke.mjs`.

The reproducibility receipt is:

| Item | SHA-256 / revision |
| --- | --- |
| Rust SDK worktree revision observed at build | `47646eb57b45436109d86728c489a3b6d08a8f53` |
| `acyclic-labs-objects-0.2.0.tgz` | `913129b534c92695addde4ad6a66e7e58d3da546fbaa7f306761e47da71af285` |
| `rust/crates/objects/src/v2/mod.rs` | `d4f953c58807d7da3edca7f8597a8b662bd4e454cba174fb5130d890be009a7d` |
| `rust/crates/objects/src/v2/http.rs` | `9af62917ed403d265c5d4b0bb80ab5e2eab454401ee38a2d1ad7ca4102651393` |
| `rust/crates/objects/src/v2/response.rs` | `c6912f504c4846755ea86d9e95b7067c1430d5f966a829d6c79160c31a5949ba` |
| `rust/crates/objects/src/generated/acyclic-objects-v2.bin` | `601b092b87d702b9aad1c0fa1615af623f19c59067ea9c52e2cb61db34994b1b` |
| `rust/crates/objects-wasm/src/v2.rs` | `693f9d80f06698b23a3d0081ff589b49714e713c591527ad4b7b4c5c029af8fb` |
| `typescript/packages/objects/src/v2-http.ts` | `8c3f23747ef1ff12b58ce6b672bab9536c64e06f1c1ce6803926074493403388` |

The archive SHA-512 is
`36908f3eb71977c2f3a845ee9ced47dbcd71505e76e3cdefa6c8a6adeb3bd94d834e3af075742c4e674467410fff6e2969f8484a81f13d7eba96b7392dda489c`.
The generated WASM bindings expose both frame-limit accessors in the archive;
the TypeScript adapter consumes those accessors and generated operation
streaming metadata rather than maintaining a second route or limit policy.
