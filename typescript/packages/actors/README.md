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
`{ endpoint, token }` and exposes every operation from generated Rust route
metadata. `HttpActorsClient` remains a compatibility alias while callers move
to the platform-neutral name. Actor invocation includes request and response
headers.

`bun run build` rebuilds the WASM bridge before TypeScript declarations. The
package tarball includes that bridge and the generated Protobuf artifacts; no
Cloud deployment is implied by local client qualification.
