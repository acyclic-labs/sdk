# Objects v2 HTTP contract

Objects v2 exposes logical tenant bucket names and object keys. Placement,
generations and service retention claims are private. This gateway and the
S3-compatible HTTP binding use the same owning authority and storage model.
This gateway uses Protobuf JSON, rather than S3 wire syntax.

The authoritative route inventory is `acyclic_objects::v2::HTTP_ROUTES`. Every
route is POST under `/v2/objects/`, with a bearer credential on every request.
Production endpoints use HTTPS; local fixtures may use loopback HTTP. Redirects
must not forward credentials. No client automatically retries a mutation.

| Route | Request | Success response |
| --- | --- | --- |
| buckets/create | CreateBucketRequest | Bucket |
| buckets/head | HeadBucketRequest | Bucket |
| buckets/delete | DeleteBucketRequest | DeleteBucketResponse |
| objects/put | PutObjectRequest frames | ObjectInfo |
| objects/get | GetObjectRequest | GetObjectResponse frames |
| objects/head | HeadObjectRequest | HeadObjectResponse |
| objects/delete | DeleteObjectRequest | DeleteObjectResponse |
| objects/list | ListObjectsRequest | ListObjectsResponse |
| multipart/create | CreateMultipartRequest | MultipartUpload |
| multipart/upload-part | UploadPartRequest frames | UploadedPart |
| multipart/list-parts | ListPartsRequest | ListPartsResponse |
| multipart/complete | CompleteMultipartRequest | ObjectInfo |
| multipart/abort | AbortMultipartRequest | AbortMultipartResponse |

## Encoding and streaming

Unary bodies use `application/json` and canonical `acyclic.objects.v2` Protobuf
JSON. Success is status 200. Unsigned 64-bit integers use decimal strings;
bytes use base64; timestamps use the standard Protobuf timestamp mapping.
The Rust descriptor codec governs validation and conversion.

Streaming bodies use `application/x-ndjson`. Each line is exactly one canonical
frame followed by LF. CRLF is accepted. JSON strings must escape embedded
newlines. Empty lines and trailing non-frame data are invalid. A JSON frame is
at most 128 KiB, including its terminator. Each decoded body frame is at most
65,536 bytes. Transport chunk boundaries need not match frames.

PUT and upload-part require exactly one header first, then body frames, then
exactly one `{"complete":true}` frame and clean request EOF. The completion
frame is emitted only after the body source finishes successfully. Missing or
false completion, duplicate completion, or any frame after completion is invalid.
EOF alone does not authorize publication: cancellation can appear as clean EOF
to a server. A successful upload returns a unary response. Incomplete, malformed,
or oversized requests must not publish bytes. The same completion rule applies
to the native gRPC upload messages; S3 uses its own body completion rules.
The complete decoded upload is at most 5 GiB; clients and service authorities
may impose smaller explicit bounds. Conditions are evaluated at publication,
including multipart completion, never only at upload creation.

GET requires exactly one metadata header first, followed by body frames for that
complete representation. EOF succeeds only when the decoded length equals
ObjectInfo.size, or the selected ContentRange length. Clients check the caller's
allocation bound before collecting bytes. A second header, malformed range,
oversized frame, excess bytes or premature EOF fails. A terminal `error` frame
carries ErrorDetail if a semantic failure occurs after the response starts;
it always fails the read and no subsequent frame is valid. gRPC may instead
return the same detail in its terminal status.

## Errors

Before opening a streaming response, errors use a bounded JSON ErrorDetail body.
Unspecified or unknown codes are not accepted as canonical semantic failures.
Request IDs are customer diagnostics and contain no topology.

| Semantic code | HTTP status |
| --- | --- |
| INVALID_ARGUMENT | 400 |
| NOT_FOUND | 404 |
| ALREADY_EXISTS, IDEMPOTENCY_MISMATCH | 409 |
| PRECONDITION_FAILED | 412 |
| QUOTA_EXCEEDED | 413 |
| UNSUPPORTED | 501 |
| UNAVAILABLE | 503 |
| ACCESS_DENIED | 403; missing/invalid authentication may use 401 |
| RANGE_NOT_SATISFIABLE | 416 |
| NOT_MODIFIED | 304, without a body |

Unknown transport failures map to UNAVAILABLE. Bounded response rejection maps
to QUOTA_EXCEEDED. Malformed successful responses fail as UNAVAILABLE. A 304
maps to NOT_MODIFIED without requiring a JSON body.

## Provider authority

The public `v2::request` helpers validate and calculate versioned BLAKE3 mutation
digests. Retry keys and upload frame boundaries are excluded. The digest binds
logical request fields, complete body length and complete body hash. Metadata
maps use canonical sorted keys. Part order and exact receipts are significant.
Authorization, current-value conditions, receipt replay, capacity, publication,
and durable service retention remain the owning provider's atomic responsibility.

Listings are eventual live lexical traversal. Cursors bind bucket, prefix,
delimiter and page size. A concurrent insertion before the cursor can be missed.
ETags are opaque and are not promised to be content checksums. S3 checksum
headers belong to the S3 binding; no checksum is hidden in user metadata.

This inventory does not establish Cloud gateway deployment or live acceptance.
