# @acyclic-labs/objects changelog

## Unreleased

## 0.2.0 - Unreleased

- Expose the sole logical Objects v1 contract through canonical Rust, protobuf, HTTP, Node/Bun gRPC and browser exports. Remove historical implementations, readers and compatibility artifacts. Local roots with obsolete format discriminators fail closed without upgrade or overwrite.
- Export current clients at the default package entry point and `./v1`, `./v1/http`, `./v1/grpc` and `./v1/proto`. The candidate version is 0.2.0; released 0.1.5 remains unchanged.
- Enforce caller download limits while framing HTTP responses and cancel oversized selections before reading their bodies. Codec zero is invalid; ZSTD is 1 and NONE is 2.
- Bind the default v1 browser fetch receiver. Browser and Cloud acceptance require qualification of this candidate.

## 0.1.5 - 2026-09-25

- Aligns object clients with the qualified SDK 0.1.5 release.

## 0.1.4 - 2026-09-25

- Aligns object clients with the qualified SDK 0.1.4 release.

## 0.1.3 - 2026-09-25

- Rebuilds the object contracts and client from the qualified SDK 0.1.3 source.

## 0.1.2 - 2026-09-25

- Aligns the object storage contracts and client with the 0.1.2 SDK release.

## 0.1.1 - 2026-09-24

- First coordinated SDK release of immutable object versions, buckets,
  snapshots, and multipart uploads.
- Ships in-memory and authenticated HTTPS providers behind the same typed API.
