# @acyclic-labs/actors

Actors v1 generated public contract and authenticated Rust-backed client. An Actor has
an identity separate from any Stream, immutable code version, explicit bindings,
and independent Stream subscriptions. Subscription delivery is at least once;
safe cursor advancement is service-owned. Resuming a paused subscription may
replay external effects. The Cloud Actor service and public route are not yet
qualified against this contract.
Updating code and bindings replaces the full configuration by expected revision;
it does not advance or rewind subscription cursors. A paused subscription stays
paused until explicitly resumed after a compatible code update or migration.

The Rust crate `acyclic-actors` owns the executable contract and the generated
Protobuf descriptor remains the wire compatibility boundary. Import wire
schemas from `@acyclic-labs/actors/proto` when working at that boundary; the
semantic Rust declarations are available under the package's `semantic`
namespace and retain branded IDs, bigint counters, oneof presence, and readonly
client results.

`ActorsClient` selects the native Rust companion on Node/Bun when installed and
the packaged Rust/WASM bridge in browsers (or as the Node fallback). It accepts
`{ endpoint, token }` and exposes every operation from the maintained
`ActorsService` descriptor. The semantic TypeScript declarations, canonical
Protobuf input, Buf wire bindings, and typed facade are regenerated from Rust
with `bun run generate:client`; there is no separately authored contract or
hand-maintained operation list in the client. `HttpActorsClient`
remains a compatibility alias while callers move to the platform-neutral name.
Actor invocation includes request and response headers.

The installed native Rust companion trusts the operating system's roots
(on Linux, `SSL_CERT_FILE` and `SSL_CERT_DIR`). Native callers may provide
`caCertificate` as PEM bytes; this is **the only trust** for that connection,
not an addition to ambient roots. Browser and WASM-fallback calls keep browser
trust and reject caller-supplied CA configuration.

`bun run build` rebuilds the WASM bridge before TypeScript declarations. The
package tarball includes that bridge and the generated Protobuf artifacts; no
Cloud deployment is implied by local client qualification.
