# Workers Protify prototype qualification

Qualification date: 2026-10-07.

## Inputs

- Product source revision inspected: `371bb4170e16aca973176b6756a261ee5add7297`.
- Immutable Workers baseline copied to `fixtures/workers/v1/workers_descriptor.bin` from `rust/crates/workers/src/generated/acyclic-workers-v1.bin`.
- Baseline descriptor SHA-256: `851B6CD37B8CB4BAA6D3A111EFDAD655B89936B2E1057ECB74E62825715BD7D8`.
- Protify: `=0.1.4` from the existing local registry cache.
- Prost: `=0.14.4`.
- Prost-reflect: `=0.16.5`.
- Protoc: `libprotoc 36.2`, existing pinned tool at `Q:\sdk\work\tools\go-producer-pins-20261006\protoc-36.2-win64\bin\protoc.exe`.

## Commands and results

1. `cargo check --manifest-path Q:\sdk\work\workers-contract-prototype\Cargo.toml --offline -j 1` — **pass**.
2. `cargo run --manifest-path Q:\sdk\work\workers-contract-prototype\Cargo.toml --offline -j 1 --bin render -- Q:\sdk\work\workers-contract-prototype-output-20261007` — **pass**.
3. Pinned `protoc 36.2` rendered `workers/v1/workers.proto` into `workers-v1-rendered.bin` with source information — **pass**.
4. The isolated `compare` binary decoded both descriptor sets with `prost-reflect`, removed only source information and the exact known Buf file metadata extension `8042`, sorted top-level messages/enums by name, and compared all remaining descriptor fields and unknown fields — **pass**.

## Artifacts

The same generated artifacts are checked into `artifacts/workers/v1/` for review. The immutable baseline is checked into `fixtures/workers/v1/`.

- Rendered proto: `artifacts/workers/v1/workers.proto` (also emitted under the external output directory)
- Rendered descriptor: `artifacts/workers/v1/workers_descriptor.bin` (also emitted under the external output directory)
- Rendered proto SHA-256: `052964B593D1F624E8E71D9EED789044F52BA73F204487F15B82369A07576646`
- Rendered descriptor SHA-256: `B9975CB6DD2FA930544BC678D094B534D43948EF05A73BA9716A754923449230`
- Baseline descriptor SHA-256: `851B6CD37B8CB4BAA6D3A111EFDAD655B89936B2E1057ECB74E62825715BD7D8`

The raw bytes differ because Protify orders declarations canonically, protoc emits source information, and the archived descriptor carries the known Buf build metadata. Canonical descriptor equivalence passed while preserving field tags, cardinality, optional presence, oneof membership, enum values, service identities, reserved declarations, file options, and all unrelated unknown fields.

This qualifies the Workers schema shape for product migration to Protify. It does not migrate the product crate, transport facade, semantic validators, language bindings, or website generation.



