# Rust metadata extraction and descriptor compatibility

## Decision

The Rust contract layer should own explicit wire metadata, while generated
protobuf descriptor images remain the compatibility artifact consumed by Buf
and its language plugins. A small Rust generator is required: none of the
candidate Rust metadata crates reverse-engineers complete protobuf descriptors
with field numbers, presence, oneofs, RPC streaming, and custom options.

The generator should expose a contract model or proc-macro attributes for
fully qualified names, tags, enum numbers, presence, oneof membership, RPC
input/output and streaming modes, HTTP paths, capabilities, and validation
annotations. It should emit deterministic `FileDescriptorSet`/`binpb` files,
OpenAPI projections, SDK metadata, and documentation inputs. Generated `.proto`
files may be emitted for review, but they must not be a second authored source.

Buf is the appropriate downstream engine: its image input is a compiled
`FileDescriptorSet`, and its plugin protocol already provides broad remote SDK
coverage. Keep descriptor images and digests for archived protocol versions.

## Existing compatibility boundary

The current contract is spread across `proto/**/*.proto`, generated Rust and
TypeScript, and checked-in descriptor images. Runtime protocol identities and
handshakes verify descriptor digests. The Rust validator therefore treats the
following as wire-significant:

- package, file, message, enum, service, and method identities;
- field numbers, labels, protobuf types, type references, defaults, JSON names,
  proto3 optional presence, and oneof indices;
- enum numeric values;
- RPC input/output names and client/server streaming flags;
- reserved ranges and names;
- custom options, including the validation extensions and `http_path` in
  `proto/validation/v1/options.proto`.

Source locations are excluded from semantic equality because comments and
provenance can change without changing the wire contract. Exact bytes and
BLAKE3 digests are always reported separately; the checker never updates a
stored digest.

## Candidate crates

| Tool | Suitable role | Boundary |
| --- | --- | --- |
| [Utoipa](https://docs.rs/utoipa/latest/utoipa/) | OpenAPI 3 projection and JSON HTTP schemas | Path/response annotations duplicate the contract and do not represent protobuf descriptors, RPC streaming, or custom options |
| [Schemars](https://docs.rs/schemars/latest/schemars/) | JSON Schema projection for data-only surfaces | Does not carry tags, oneofs, presence, enum wire numbers, RPCs, or protobuf extensions; schema layout must be pinned |
| [Specta](https://github.com/specta-rs/specta) | Auxiliary TypeScript/Swift/local data types | Exporters and language coverage vary; no descriptor or transport model |
| [Typeshare](https://github.com/1Password/typeshare) | Serialized data-only FFI types | No service, streaming, descriptor, or broad remote SDK model |
| [UniFFI](https://mozilla.github.io/uniffi-rs/) | Embedded Rust behavior for Swift/Kotlin/Python/Ruby | UDL would be another contract; use proc-macro metadata over Rust APIs and keep it separate from remote descriptor generation |
| [Prost](https://docs.rs/prost/latest/prost/) | Rust implementation of generated protobuf types | Generates Rust from protobuf; does not generate descriptors from arbitrary Rust types |
| [Buf](https://buf.build/docs/reference/images/) | Descriptor images, lint, breaking checks, and downstream plugins | Consumes the Rust-generated image; it is not the Rust contract authoring layer |
| [OpenAPI Generator](https://openapi-generator.tech/docs/generators/) | Broad HTTP SDK projection | OpenAPI cannot preserve gRPC/streaming/descriptor semantics |
| [Cloudflare Forge](https://github.com/cloudflare/forge) | Optional later OpenAPI/docs/CLI consumer | Current project is early-stage and focused on the `cf` CLI; it should not be foundational or become the new TypeScript contract source |

## Proc macros versus Rustdoc JSON

A custom proc macro or declarative registry is appropriate for wire metadata. It
can emit a `ContractDescriptor` implementation or inventory registration, but
wire identities must be explicit rather than inferred from Rust declaration
order. A dedicated crate-level registry is necessary because a proc macro does
not discover every item in a workspace by itself.

Rustdoc JSON is appropriate for API documentation, links, prose, and examples.
It does not reliably contain protobuf tags, custom options, RPC routes, or
streaming semantics. It is experimental and requires pinned nightly tooling;
keep its generation in a docs-only toolchain rather than the stable product
build. Rustdoc comments and doctests should remain the authored guide and
executable example source.

## Validator implementation

`rust/crates/sdk-contract-validation` is a standalone crate so it can be
verified before changing the root workspace manifest. It provides:

```text
cargo run --manifest-path rust/crates/sdk-contract-validation/Cargo.toml -- \
  --baseline old.binpb --candidate new.binpb
```

The library compares encoded `FileDescriptorSet` values and reports exact-byte
equality, both BLAKE3 digests, semantic compatibility, and typed differences.
The tests cover changed field tags and types, explicit presence, oneofs, enum
numbers, service streaming, reserved ranges, source locations, and custom
options. Malformed or ambiguous inputs are rejected: an empty descriptor set,
an unnamed file, duplicate descriptor identities, and duplicate field numbers
cannot become a false migration baseline. Unknown descriptor fields are
retained as incompatible changes, while repeated option values preserve their
wire order. This keeps the checker conservative when a newer compiler adds a
field or extension that this pinned `prost-types` release does not understand.

`prost-types` discards unknown extension fields when decoding option messages.
The validator therefore parses the original protobuf wire stream in parallel,
locates every descriptor's options field, and retains the raw fields in source
order for comparison. This retains unknown validation and HTTP-route
extensions without hard-coding every extension definition. Empty sets,
duplicate identities and field numbers, unknown direct descriptor fields, and
reordered option or unknown-field payloads fail closed. Exact descriptor bytes
remain available for digest and release checks, separately from semantic
compatibility.

The bounded custom-option proof is in
`rust/crates/sdk-contract-options`. Its typed Rust values cover validation
extensions 51001 through 51012, emit their raw proto2 fields, and emit the
matching `options.proto` source. `OptionSpec` is the shared identity table;
`RawOptions::try_push` checks that a target, extension number, scalar type, and
value agree and rejects duplicate assignments. The unchecked `push` method is
kept only for low-level wire fixtures, while `RawOptions::encode` validates
before emitting bytes.

The proof fixture demonstrates that decoding the descriptor through
`prost-types` loses the application options while the parallel raw wire still
retains every extension number and value. A vendored, locked `protoc`
compilation test accepts the emitted source and verifies the imported package
and service shape. The same pinned compiler builds the repository's core
`proto/validation/v1/options.proto` oracle; the generated Rust projection must
match that descriptor byte-for-byte, while source comments and source-info are
outside the comparison.

`RawOptions`/`RawOption` is the generic exporter boundary: Filesystem and
Harness models supply typed Rust values, and descriptor/proto exporters consume
the same explicit identities. Those models must use this path for options
51001--51012 rather than adding handwritten option declarations. The fixture
uses a `FilesystemRequest`, required `source` oneof, partial `HarnessOutcome`,
and `/v1/options/run` operation to exercise field, oneof, enum-value, and
method targets together. A cross-crate test runs the reusable compatibility
validator against a `prost`-normalized copy and rejects the lost custom fields.

## Validator metadata audit

The current Actors runtime validators already define the behavior that contract
metadata must describe. `validate_create` and `validate_update` require a
nonzero 32-byte code digest, nonempty identity and routing strings, positive
resource limits, bounded subscription and binding counts, unique names, and at
most one placement anchor. `validate_add_subscription` requires an actor
identity, idempotency key, and exactly one valid subscription start choice.

The current Rust contract model proves wire tags, presence, oneofs, and JSON
names, but its `FieldSpec` has no typed validation metadata, `MethodSpec` has no
error or capability metadata, and `RouteSpec` is separate from descriptor
options. That is a concrete Actors completeness gap: generation can reproduce
the wire shape while SDK validators and docs still miss the admission rules.
The migration should attach the existing rules as typed Rust metadata and have
the generated validators, schemas, route docs, and examples consume that
metadata. It should not translate validator algorithms into each SDK; remote
SDKs should carry generated validation hints and preserve server error
semantics, while embedded behavior remains bound to Rust.

The current Actors runtime image was compared with the Rust-emitted
source-info-free image:

```text
runtime: 07e8868acbbe1905a16626f2ab010fdcd57aebdfa391ad6d145cebab488a4972
rust:    137f65d9f07d88779d1e2ab1a92359486ddaa5dbbbc5f71669090cfe047cf28e
```

The runtime image is 11,815 bytes and the Rust image is 5,234 bytes. The
validator reports `semantic compatibility: compatible`; the exact difference
comes from compiler/source metadata, including Buf's optional image extension,
and source information. The two byte sequences therefore must not be treated
as interchangeable handshake identities. A runtime that computes its
`descriptor_digest` over the old image will reject a client advertising the
Rust image until the runtime descriptor artifact and advertised digest are
switched together. The semantic result is suitable for migration review, but
the exact digest gate remains mandatory for a release or handshake change.

## References

- [Buf inputs](https://buf.build/docs/reference/inputs/)
- [Buf generate](https://buf.build/docs/reference/cli/buf/generate/)
- [Rustdoc unstable features](https://doc.rust-lang.org/rustdoc/unstable-features.html)
- [Cloudflare Forge announcement](https://blog.cloudflare.com/forge-open-source-generation-pipeline/)
