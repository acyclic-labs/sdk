# Objects v2 Rust contract model

Objects v2 is authored in
[`rust/crates/sdk-contract-wire/src/objects.rs`](../../rust/crates/sdk-contract-wire/src/objects.rs).
The model is the source input for descriptor, protobuf, HTTP, SDK, and
documentation exporters. The existing Objects v1 descriptor and generated
bindings remain archived compatibility fixtures; they are not inputs to active
generation and must not be regenerated from this model.

## Wire inventory

`OBJECTS_V2` emits `objects/v2/objects.proto` in package
`acyclic.objects.v2`, with proto3 syntax, the
`google/protobuf/timestamp.proto` dependency, and the exact Go package option
`github.com/acyclic-labs/sdk/go/gen/objects/v2;objectsv2`.

The model retains the complete v2 surface:

- 36 messages, including the nested `ObjectMetadata.UserEntry` map entry;
- `ObjectsLimit` values 0, 256, 1024, 2048, 1000, 65536, and 10000;
- `ErrorCode` values 0 through 11 with their published names;
- `BucketsService`, `ObjectsService`, and `MultipartService`, with 13 RPCs;
- explicit field numbers, scalar/reference types, cardinality, and protobuf
  JSON names;
- `ObjectMetadata.expires_unix_seconds` and `InclusiveRange.end` as explicit
  proto3 optional fields with synthetic oneofs;
- the `Preconditions.condition`, `PutObjectRequest.frame`,
  `ByteRange.selection`, `GetObjectResponse.frame`, and
  `UploadPartRequest.frame` oneofs; and
- the map key/value types and nested map-entry identity.

Objects v2 has no reserved field names or ranges and no custom validation
options in its published descriptor. If a future revision adds one, the Rust
model must represent its extension identity and wire value before any emitted
artifact changes. The archived Objects v1 contract remains a separate identity;
v2 fields must not be assigned by reinterpreting v1 tags.

## HTTP projection

`OBJECTS_ROUTES` is the Rust-owned 13-route projection. Every route is `POST`
under `/v2/objects/` and records its operation ID, complete protobuf RPC
identity, request/response messages, and operation prose. The route paths are:

| Operation | Path | Request | Response |
| --- | --- | --- | --- |
| `createBucket` | `/v2/objects/buckets/create` | `CreateBucketRequest` | `Bucket` |
| `headBucket` | `/v2/objects/buckets/head` | `HeadBucketRequest` | `Bucket` |
| `deleteBucket` | `/v2/objects/buckets/delete` | `DeleteBucketRequest` | `DeleteBucketResponse` |
| `putObject` | `/v2/objects/objects/put` | `PutObjectRequest` frames | `ObjectInfo` |
| `getObject` | `/v2/objects/objects/get` | `GetObjectRequest` | `GetObjectResponse` frames |
| `headObject` | `/v2/objects/objects/head` | `HeadObjectRequest` | `HeadObjectResponse` |
| `deleteObject` | `/v2/objects/objects/delete` | `DeleteObjectRequest` | `DeleteObjectResponse` |
| `listObjects` | `/v2/objects/objects/list` | `ListObjectsRequest` | `ListObjectsResponse` |
| `createMultipart` | `/v2/objects/multipart/create` | `CreateMultipartRequest` | `MultipartUpload` |
| `uploadPart` | `/v2/objects/multipart/upload-part` | `UploadPartRequest` frames | `UploadedPart` |
| `listParts` | `/v2/objects/multipart/list-parts` | `ListPartsRequest` | `ListPartsResponse` |
| `completeMultipart` | `/v2/objects/multipart/complete` | `CompleteMultipartRequest` | `ObjectInfo` |
| `abortMultipart` | `/v2/objects/multipart/abort` | `AbortMultipartRequest` | `AbortMultipartResponse` |

Streaming direction remains a wire property: PUT and upload-part are
client-streaming, GET is server-streaming, and the other operations are unary.
The HTTP table does not replace the gRPC service descriptor or infer streaming
behavior from URL shape.

## Compatibility and qualification

The model descriptor is checked byte-for-byte against the immutable
source-info-free Objects v2 fixture in
`rust/crates/sdk-contract-wire/tests/fixtures/objects-v2.descriptor.bin`.
The semantic test separately checks the package, dependency, message and enum
counts, map entry, optional presence, field tags, RPC identities, and stream
directions. Route tests check all 13 paths and bind each route to the matching
service method.

The active exporter emits the Rust-rendered `.proto` and descriptor. A
qualification run should independently compile that emitted source with the
pinned protoc toolchain and compare the resulting descriptor semantically and,
where the toolchain is deterministic, byte-for-byte. The command must never
read `proto/objects/v2/objects.proto` to recover a missing Rust field or route.
The checked-in protobuf can be used only as the one-time compatibility oracle;
the archived Objects v1 descriptor is immutable evidence and must remain
unchanged.

## Independent conformance review

The Objects-specific integration suite in
`rust/crates/sdk-contract-wire/tests/objects_model.rs` covers the deployed
descriptor and generated SDK surface without reading the active protobuf
source. The v2 fixture has SHA-256
`1968e12e89d38f7076b9aee559c815748858c156401a56ba53e615def49fe86f`.
The archived v1 descriptor remains pinned by
`compatibility/objects/v1/manifest.json` at descriptor SHA-256
`4701187ac8ca87325d42aee0f99c7826ebb63ec2be0ae98c4768006d4ff31a51`; the
review verifies its package is `acyclic.objects.v1` and keeps it distinct from
the active v2 package.

The descriptor review asserts the exact `go_package`, an empty
`uninterpreted_option` list, and empty reserved ranges/names for every message
and enum. This checks for accidental custom or unknown options and for a
future wire change that silently consumes a previously reserved tag. The
generated Rust message fixture is checked for all 36 top-level messages, the
map and optional attributes, nested oneofs, and all three generated Tonic
clients with their 13 RPC methods.

OpenAPI qualification is currently a cross-crate blocker. The prototype at
`rust/crates/sdk-openapi-prototype/src/lib.rs:9` imports only the Workers docs
tables, and its package-specific docs branch at line 139 handles only
`acyclic.workers.v1`; there is no Objects projection yet. The existing
prototype test run therefore cannot qualify Objects and currently has unrelated
Workers failures for missing `SelectDeploymentResponse.deployment` field
documentation. Once the common renderer is complete, an Objects OpenAPI test
must assert all 13 routes, operation IDs, schemas, field descriptions, and the
streaming rejection policy for PUT, GET, and multipart upload.

The proto documentation regression test also records source gaps precisely.
The current renderer's hard-coded `ErrorCode` text differs from the Rust-owned
`OBJECTS_ENUM_DOCS` entry, so the test reports missing documentation for
`ErrorCode` before it can reach the field checks. After that mismatch is
corrected, `objects_proto()` reaches `CreateBucketRequest.mutation` (the
renderer reports this at `rust/crates/sdk-contract-wire/src/lib.rs:1375`).
The current Objects field table additionally lacks the nested map-entry names
`key` and `value`, plus `total`, so those entries must be added to the
Rust-owned docs table before generated proto/OpenAPI documentation can
qualify. The test intentionally fails until those missing entries are
supplied; no placeholder prose is accepted.
