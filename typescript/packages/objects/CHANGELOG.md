# @acyclic-labs/objects changelog

## Unreleased

- Move the default, protobuf, HTTP and Node/Bun gRPC exports to logical Objects v2. Retire the v1 TypeScript clients, WASM projections and version/snapshot surface. This breaking source transition requires a new breaking package version before publication; released 0.1.5 remains unchanged.

- Enforce caller download limits while framing HTTP responses and cancel oversized selections before reading their bodies. Export v2 protobuf bindings at `./v2/proto`.

- Bind the default v2 browser fetch receiver and qualify the complete HTTP lifecycle in real Chrome.

- Retain `./v2`, `./v2/http`, `./v2/grpc` and `./v2/proto` aliases for consumers that adopted the unreleased logical contract before the canonical export transition.

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
