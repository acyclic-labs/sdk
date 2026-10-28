# Objects v1 HTTP contract

Objects v1 serves account-scoped, content-addressed blobs. The caller's
verified account is the whole namespace; placement and storage are private.
This gateway uses Protobuf JSON, rather than S3 wire syntax; the S3 binding is
a separate view over the same blobs.

The authoritative route inventory is `acyclic_objects::HTTP_ROUTES`. Every
route is POST under `/v1/objects/`, with a bearer credential on every request.
Production endpoints use HTTPS; local fixtures may use loopback HTTP. Redirects
must not forward credentials. No client automatically retries a mutation.

| Route | Request | Success response |
| --- | --- | --- |
| blobs/put | PutBlobRequest | PutResponse |
| blobs/put-manifest | PutManifestRequest | PutResponse |
| blobs/get | GetBlobRequest | GetBlobResponse |
| blobs/head | HeadBlobRequest | BlobInfo |
| blobs/delete | DeleteBlobRequest | DeleteBlobResponse |
| blobs/list | ListBlobsRequest | ListBlobsResponse |
| blobs/retain | RetainRequest | RetainResponse |
| blobs/release | ReleaseRequest | ReleaseResponse |
| blobs/collect | CollectRequest | CollectResponse |
| blobs/usage | UsageRequest | UsageResponse |

## Encoding

Requests and responses use `application/json` and canonical `acyclic.objects.v1`
Protobuf JSON, one message each. Success is status 200. Unsigned 64-bit
integers use decimal strings and bytes use base64. The Rust descriptor codec
governs validation and conversion.

## Outcomes

A put names its blob by the BLAKE3-256 digest of the body, which the service
recomputes; a mismatch is DIGEST_MISMATCH. `created` and `alreadyExists` are
durable and carry the verified digest and length. `unknown` means the outcome
could not be determined; repeating the identical put is always safe. Clients
check every reply against its request with `acyclic_objects::route::verify`
before exposing it: a put must name exactly the published digest and length,
a read must cover exactly the selected range of the requested digest, and a
page must stay ascending after its cursor within its limit. Any other reply
fails as UNAVAILABLE.

## Errors

Errors use a bounded JSON ErrorDetail body. Unspecified or unknown codes are not
accepted as canonical semantic failures. Without a detail, clients classify the
status: 400 INVALID_ARGUMENT, 401/403 ACCESS_DENIED, 404 NOT_FOUND, 409
CONFLICT, 413/429 QUOTA_EXCEEDED, 416 RANGE_NOT_SATISFIABLE, 501 UNSUPPORTED,
anything else UNAVAILABLE. Bounded response rejection maps to QUOTA_EXCEEDED.
Request IDs are customer diagnostics and contain no topology.

This inventory does not establish Cloud gateway deployment or live acceptance.
