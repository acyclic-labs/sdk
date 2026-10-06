# Rust-owned executable snippets

The SDK examples crate is a deliberately standalone workspace at
`rust/crates/sdk-examples`. It keeps the migration work reviewable while the
parent workspace is changing: it has its own manifest and does not require a
root `Cargo.toml` edit.

## Source of truth

Scenarios are typed Rust values in `src/lib.rs` and the family guide modules
under `src/*_scenarios.rs`. Each scenario has a stable ID, contract family,
title, and a typed operation. Renderers project those values into Rust, Python,
Go, TypeScript, and schema-only targets. JSON is an output format for a future docs
bundle, never an authoring input. Every emitted projection carries this public
metadata shape:

```text
id, family, title, language, source, validation
```

`source` points back to the Rust scenario registry. `validation` records the
level, status, and evidence. A capability status is also emitted separately:
`supported` means this checkout contains the client and validation path;
`schema-only` means the projection shows the Rust-owned wire shape without
claiming a language SDK that is not present. The bundle also records a hash of
the Rust source and every rendered file, so a generated directory can be
checked without trusting timestamps.

The registry covers two transport walkthroughs and source-owned family guides:

* **Actors create roundtrip:** generated Prost request, encode/decode, and the
  real `acyclic-actors::validate_create` validator.
* **Stream append/read:** a two-record append with an idempotency key and tail
  condition, followed by a bounded read from the real `MemoryStream` provider.

Filesystem, Harness, Inference, Machines, Objects, and Workers each keep their
executable scenario in a dedicated Rust module. Guide receipts record each
module path and content digest, then run the family scenario against its real
local provider or wire implementation.

The Rust tests execute both scenarios. The TypeScript projections use the
checked-in generated package APIs (`@acyclic-labs/stream`,
`@acyclic-labs/actors`, and `@bufbuild/protobuf`). The Python projections use
the generated `acyclic_sdk.generated` protobuf modules, and Go projections use
the generated `go/gen` packages. A projection is
`supported` when its package is present, but its receipt remains `pending`
until the actual language runtime compiles or executes it. Rust receipts run
the real implementations during bundle generation; Python receipts install
the wheel into a disposable environment; Go receipts compile the exact
rendered source in a temporary module against the generated package tree.
Each language receipt carries the Rust source revision, scenario source digest,
snippet path and digest, package artifact digest, and captured output digests.

## Validation and drift checks

Run from any checkout:

```text
cargo test --manifest-path rust/crates/sdk-examples/Cargo.toml --locked
cargo run --manifest-path rust/crates/sdk-examples/Cargo.toml --locked -- generate --request PATH
cargo run --manifest-path rust/crates/sdk-examples/Cargo.toml --locked -- check --request PATH
cargo run --manifest-path rust/crates/sdk-examples/Cargo.toml --locked -- fixtures generate --request PATH
cargo run --manifest-path rust/crates/sdk-examples/Cargo.toml --locked -- fixtures check --request PATH
```

The tests assert stable scenario IDs, source provenance, imports, capability
markers, and absence of placeholder output. They also execute the Actors wire
roundtrip and Stream append/read against the actual Rust implementations. A
bundle generation persists the rendered metadata, source-content hash, and
runtime receipts. `check` reruns the same validations and compares source and
rendered hashes, so changing a typed scenario fails a stale-bundle check rather
than silently maintaining a second hand-authored example.

The `fixtures` command emits `sdk-transport-fixtures-manifest.json` plus
canonical protobuf request bytes and expected semantic results. The initial
registry covers an Actors CreateActor unary request and a Stream append/read
walkthrough. Each entry records stable operation and message identities,
  request/result hashes, and a source-bound receipt that distinguishes Rust
  wire validation from local MemoryStream execution. The `fixture-server`
  binary provides a bounded loopback endpoint for language runners: it
  advertises ephemeral HTTP and tonic gRPC endpoints (the gRPC address is
  exported to `FIXTURE_GRPC_ADDRESS`), accepts canonical Actors Protobuf JSON,
  protobuf, and Stream protobuf bytes, and exits after its request budget or
  an explicit `/shutdown`. The tonic endpoints use the canonical Actors and
  Stream services, including unary CreateActor, append/read, follow
  cancellation, and idempotency recovery. The fixture bundle records this
  server contract while making no hosted service-availability claim. Language runners can
  consume these files and compare the same Rust-owned values instead of
  carrying their own TypeScript mocks or expected responses.

## OSS fit and bounded next steps

This small renderer complements rather than replaces broader generators:

* [rustdoc JSON RFC 2963](https://github.com/rust-lang/rfcs/blob/master/text/2963-rustdoc-json.md)
  can supply API prose and item links, but it does not model executable
  cross-language scenarios. Its output remains experimental in rustdoc.
* [OpenAPI Generator](https://openapi-generator.tech/docs/generators/) can
  consume a generated HTTP contract for remote clients, but examples still
  need scenario provenance and execution receipts.
* [Cloudflare Forge](https://github.com/cloudflare/forge) is an OpenAPI-first
  generator and documentation pipeline. It is a useful downstream experiment
  after Rust emits a pinned OpenAPI document; it is not a Rust source extractor.
* [NATS documentation](https://github.com/nats-io/nats.docs.v2) demonstrates a
  practical language-tab model where examples live beside client code. The
  typed registry here provides the missing single source and validation receipt.

The bundle command writes one machine-readable manifest and language files
under a generated directory, including the Rust revision and artifact hashes.
It invokes the same scenario registry, retains schema-only or pending statuses,
and compiles or executes each supported language against the matching package
artifact before a website preview consumes it. Regeneration must use the
current Rust checkout and input closure; receipts retain the observed revision
and digest rather than relabeling older evidence.
