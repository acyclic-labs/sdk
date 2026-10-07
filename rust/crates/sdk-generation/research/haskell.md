# Haskell and functional-language qualification

**Review scope:** the Rust-owned protobuf/service contract, the Haskell
prototype at
`worktrees/rust-sdk-docs-source/research/additional-languages/haskell-grapesy-prototype`,
and maintained upstream binding/generator sources. This is a qualification
note, not a claim that the prototype is a supported SDK.

## Decision

Haskell is a credible **remote protobuf candidate**. It is not currently a
qualified native Rust SDK. The prototype proves generated wire types and the
106-method/18-service type-level surface, but `proto-lens` types do not prove
the Rust-owned nominal validators, request-bound response invariants, or
canonical replay rules. The honest boundary is therefore:

* admit a remote Haskell lane only after its generated sources are pinned to
  the canonical descriptor and its semantic façade is exercised against Rust
  receipts;
* keep native Haskell out of the final supported set until an explicit,
  maintained Rust-to-Haskell generator exists; and
* treat `grpc-haskell` as prototype transport plumbing, not as a production
  qualification shortcut.

## Evidence and versions

The existing prototype has a reproducible, source-bound dependency record:

| component | pinned evidence in prototype | licence / qualification consequence |
| --- | --- | --- |
| `proto-lens` | 0.7.1.7 (`provenance.json` archive SHA-256 `5c9826a4...`); generated bindings use its lenses, field descriptors, and runtime | Google repository [LICENSE](https://github.com/google/proto-lens/blob/master/LICENSE) is BSD-style; the library is a maintained protobuf representation/codegen stack, not a Rust semantic-type generator |
| `proto-lens-protobuf-types` | 0.7.2.3, archive SHA-256 `06930638...` | supports the generated wire representation; no native Rust ABI |
| `proto-lens-runtime` | 0.7.0.8, archive SHA-256 `25a1508a...` | runtime encoding/decoding only |
| `proto-lens-protoc` | 0.9.0.1, archive SHA-256 `513e4338...` | the exact generator used by the prototype; regenerate from the Rust-owned `.proto` files, never from a copied hand-maintained schema |
| `grapesy` | 1.2.1, archive SHA-256 `be40dda8...` | prototype README records BSD-3-Clause; useful gRPC/TLS/reconnect/cancellation API, but transport behavior still needs receipt tests |
| `grpc-haskell` | no dependency in the current prototype; upstream repo inspected | upstream marks it “experimental” and “not ready for production use,” supports client-side RPC only, and explicitly does not generate stubs or protobuf serialization ([README](https://github.com/grpc/grpc-haskell)); Apache-2.0. Exclude from final qualification |

The prototype's other source-bound inputs are GHC 9.2.8, Cabal 3.10.2.1,
Rust proto SHA-256
`B7463189BA964E193D5EBD517AF7B609462F1BF84B6BAD9DCAE216389196D5B9`, and
the request-manifest SHA-256 recorded in `provenance.json`. These are useful
reproduction inputs; they are not a successful remote-conformance receipt.

## Strongest remote type boundary

`proto-lens` is a good fit for the wire contract. Its generated modules expose
typed scalar fields, `Maybe` presence lenses, generated enum representations,
and protobuf message/oneof descriptors. The prototype's
`Acyclic.Semantics` additionally models known oneof arms as GADT constructors
and keeps a raw-byte constructor only for forward-compatible unknown arms.
This is a stronger boundary than an untyped JSON or text client, and it can
represent `Word64`/`Int64`, optional present/absent values, signed oneofs, and
unknown enum numeric values without narrowing them first. See the
[proto-lens documentation](https://google.github.io/proto-lens/) and
[source repository](https://github.com/google/proto-lens).

That generated surface is still a wire surface. In particular, it cannot make
an arbitrary `Text` into the Rust `BucketName`, `ObjectKey`, `IdempotencyKey`,
digest, or validated timestamp newtype; it cannot establish that a response
identity belongs to its request; and it does not encode the Rust canonical
mutation/digest/replay policy. A supported Haskell client therefore needs a
small handwritten boundary module that calls the canonical validators and
returns typed failures before constructing generated requests. The module
must preserve unknown enum numbers and oneof arms rather than silently mapping
them to a default constructor.

The remote qualification proof should execute, against the Rust fixture, all
of the following: maximum unsigned 64-bit values; optional present versus
absent fields; unknown enum numbers; every signed oneof arm; invalid nominal
constructors; request/response identity; stream completion and cancellation;
and canonical replay/digest receipts. A compile-only 106-method proof or a
local round-trip is insufficient.

## Native Rust boundary

The official [UniFFI documentation](https://mozilla.github.io/uniffi-rs/)
describes its generated foreign bindings and supported language-specific
custom types ([custom-type guide](https://mozilla.github.io/uniffi-rs/latest/types/custom_types.html));
Haskell is not a supported target in that generator. Adding Haskell would
mean maintaining a UniFFI backend/templates and mapping its object ownership,
errors, callbacks, and streams. That is a new binding implementation, not a
qualification of the existing Rust contract.

The maintained escape hatch is a deliberately designed C ABI plus Haskell
FFI. [cbindgen](https://github.com/mozilla/cbindgen) (MPL-2.0) generates
C/C++ headers from a public C API and its documentation explicitly requires a
crate with that API; it does not expose Rust private validators or generate
Haskell. The [Rust C interoperability guide](https://doc.rust-lang.org/stable/embedded-book/interoperability/rust-with-c.html)
and [Haskell FFI specification](https://www.haskell.org/onlinereport/haskell2010/haskellch8.html)
make the ABI direction clear. A viable future bridge would need opaque
handles, explicit result/error codes, ownership and destructor functions, and
an explicit stream-cancellation contract. Until those are implemented and
tested, C ABI plus handwritten Haskell wrappers is excluded from the final
native SDK set because it would not preserve the Rust strongest types at the
language boundary.

## Other functional languages

No maintained official Rust generator for OCaml, F#, Elm, or another
functional target was found in the Rust-owned source or the maintained
binding sources reviewed here. A protobuf/OpenAPI client in one of those
languages could prove a remote wire contract, but it would have the same
semantic-wrapper gap as Haskell and would need an independently pinned,
maintained generator. Do not count an ad-hoc C-ABI wrapper or a direct gRPC
runtime as a qualified SDK.

## Minimal next qualification

1. Regenerate Haskell from the canonical descriptor with
   `proto-lens-protoc 0.9.0.1`, retaining the exact frozen package graph and
   source/archive hashes above.
2. Compile the generated consumer with the existing GHC 9.2.8/Cabal 3.10.2.1
   environment, then run the semantic, wire, canonical-replay, and remote
   runners rather than recording compile-only results.
3. Require a Rust-fixture receipt for the edge cases listed above and bind
   the receipt to the descriptor hash, generated-source hash, package hashes,
   and executed-runner logs.
4. Report the result as **remote protobuf qualified** only when those receipts
   pass. Keep native Haskell **unqualified** unless the explicit C ABI (or a
   maintained UniFFI Haskell backend) has the same proof.

No build, download, cache generation, source regeneration, or package
publication was performed for this review.
