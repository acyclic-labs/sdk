# Additional-language architecture decision

Recorded 2026-10-03 against the current Rust-source migration worktree.

## Decision

Use a small Rust-owned contract and generation stack as the migration baseline:

1. Rust contract metadata owns wire identities, field presence, oneofs, enum
   numbers, RPC streaming, HTTP routes, capabilities, validation annotations,
   and error identities.
2. The Rust exporters produce descriptor images and OpenAPI documents. Buf,
   protoc plugins, OpenAPI Generator, and target language runtimes consume
   those outputs; they do not author a second shared contract.
3. Native and WASM packages remain host adapters over Rust behavior. UniFFI,
   N-API, and WASM bindings are appropriate where a receipt proves the target
   boundary, but they do not replace the remote transport contract.
4. Each language is qualified from a pinned source projection and installed
   artifact. A bounded HTTP projection remains an HTTP qualification even when
   the language also has an independent gRPC runtime.

This is the smallest stack supported by current concrete evidence. The
contract and compatibility rationale is in
[`docs/research/rust-metadata.md`](../../docs/research/rust-metadata.md), and
the generation and documentation boundary is in
[`docs/research/rustdoc-generation.md`](../../docs/research/rustdoc-generation.md).

## Evidence used for the initial prototype set

The current working receipts select these concrete downstream prototypes:

| Surface | Current evidence | Scope |
| --- | --- | --- |
| Rust contract and wire validation | `rust/crates/sdk-contract-*` and `research/acceptance/` receipts | Descriptor identity, custom options, compatibility and Rust behavior |
| HTTP projections | `research/additional-languages/openapi-targets/bash-manifest.json`, `julia-manifest.json`, `rust-reqwest-manifest.json`, and `scala-receipt.json` | Installed or locally generated HTTP consumers bound to Rust OpenAPI projections |
| JVM transport | `docs/research/jvm-dotnet-receipt.json` | Installed Java transport and bounded consumer checks |
| Native/WASM adapters | `docs/research/embedded-bindings.md`, `docs/rustdoc-profiles.json`, and the language-specific receipts | Host boundaries over Rust packages; each profile retains its own toolchain and artifact identity |
| Additional package inventory | `languages/generation-targets.json` and `research/additional-languages/targets.json` | Per-language maturity, license scope, installability, evidence, and outstanding work |

These receipts are evidence for their listed operations and runtimes. They do
not promote a language to complete SDK coverage when streaming, recovery,
package distribution, or another family remains outside the receipt.

## Why the Rust contract layer remains custom

OpenAPI tools are useful HTTP emitters, but OpenAPI does not carry the full
protobuf contract: descriptor field numbers, proto3 presence, oneof indices,
custom options, RPC streaming flags, and the repository's validation metadata
must remain available for wire compatibility and generated policy. Buf and
protoc are downstream descriptor and plugin engines; they cannot infer those
identities from arbitrary Rust declarations. The custom Rust metadata layer
therefore emits both descriptor and OpenAPI projections from the same explicit
model.

Language facades may add idiomatic constructors and runtime plumbing, but
shared validation, route, error, retry, and capability semantics must be
generated or bound from Rust metadata. A target-specific adaptation is recorded
as such, with its source anchor and license scope, rather than treated as a new
contract author.

## Forge and the implementation-versus-roadmap boundary

Cloudflare Forge is retained as an optional downstream OpenAPI/docs/CLI
consumer. Its repository and launch material describe a TypeScript-centered
pipeline and future SDK/documentation surfaces; those advertised wrappers are
roadmap context until an implementation and artifact are pinned and tested.
Forge therefore cannot be the source of truth or a qualification substitute
for the Rust exporters. The project can consume Forge output after comparing
it with the Rust bundle, while the language matrix continues to use only
actual package receipts for qualification.

## Remote, native, and WASM selection

Remote SDKs use the Rust-owned capability and transport metadata to select a
qualified default where one exists, with an explicit override for callers who
need a particular transport. The selector must not silently change transport
or replay a non-idempotent operation after failure. HTTP-only projections such
as Bash, Julia, Rust reqwest, and Scala sttp remain explicit HTTP evidence;
they do not imply native gRPC or streaming recovery.

Native and WASM adapters are separate package surfaces because their runtime
constraints differ. A native or WASM receipt proves the listed host boundary
and operations only. It does not erase an HTTP target's limits or create
remote parity without a Rust-bound transport implementation and a matching
artifact receipt.

