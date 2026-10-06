# Rust generator dependency audit

This document records the dependency boundary for the Rust owned SDK and documentation generator in this worktree. It is based on the current source and pinned manifests in this checkout.

## Current generation boundary

The generation command is the standalone crate [`rust/crates/sdk-generation`](../rust/crates/sdk-generation). Its lockfile currently contains 209 package records. The complete workspace lockfile contains 570 package records because it covers product, native, WASM, and code generation targets together.

The generation executable imports the shared Actors contract and generator source directly:

```rust
#[path = "../../actors/src/contract.rs"]
mod contract;
#[path = "../../actors/src/codegen.rs"]
mod actors_codegen;
```

These imports are part of the generation input and determine the required dependencies. The generator does not use a separately authored protobuf contract.

| Dependency | Authoritative source that uses it | Purpose |
| --- | --- | --- |
| `depinfo = 0.7.10` | `sdk-generation/src/main.rs` | Reads rustdoc dependency information so all crate owned Markdown is included and hashed. |
| `sdk-docs` | `sdk-generation/src/main.rs` | Converts pinned rustdoc JSON and source metadata into versioned docs data and writes the bundle. |
| `serde`, `serde_json` | `sdk-generation/src/main.rs` and `sdk-docs` | Generation manifests, docs data, schemas, and receipts. |
| `sha2` | `sdk-generation/src/main.rs` and `sdk-docs` | Source, rustdoc, tool, and artifact identities. |
| `protify = 0.1.4` | `actors/src/contract.rs` | Rust declarations, protobuf metadata, wire derives, and generated contract surface. |
| `ts-rs = 12.0.1` | `actors/src/contract.rs` | The contract's TypeScript type derivation used by the current generated surface. |
| `tonic-prost-build = 0.14.6` | `actors/src/codegen.rs` | Generates the maintained Rust transport facade from the rendered Rust owned contract. |
| `protoc-bin-vendored = 3.2.0` | `actors/src/codegen.rs` | Supplies the pinned protoc executable and include path for reproducible generation. |

All nine direct dependencies in [`sdk-generation/Cargo.toml`](../rust/crates/sdk-generation/Cargo.toml) are therefore required by the current source closure. Removing the four Actors codegen dependencies would break the imported contract or generated output. Moving that source closure to a dedicated generator package is a future architecture change that must preserve the same artifacts and hashes; it is not a dependency deletion.

## Protify feature selection

The current generator pins Protify with:

```toml
protify = { version = "=0.1.4", default-features = false, features = ["std"] }
```

This is the proven minimal feature selection for the imported Actors contract. Protify's default feature set additionally enables `regex`, `cel`, `chrono`, and `inventory`; the generator deliberately disables defaults and selects `std` only.

The cached Protify manifests show that its `std` feature does not enable CEL. The `cel` feature is separate and explicitly enables the CEL dependency plus CEL support in `proto-types` and the proc macro. The current generator does not request that feature. The lockfile contains a `cel` package record through the broader resolved package metadata, but that record alone does not establish that Protify's CEL feature is compiled.

Do not enable Protify defaults. Do not remove `protify` from the generator while the imported Actors contract remains in place.

## Documentation dependencies

The local [`sdk-docs`](../rust/crates/sdk-docs) crate is the Rustdoc owned documentation projection. Its source uses each declared dependency:

- `public-api` extracts public API signatures from rustdoc JSON.
- `rustdoc-types` is the pinned rustdoc JSON model.
- `schemars` emits schemas for the generated docs bundle.
- `semver` validates released documentation versions.
- `serde` and `serde_json` serialize the bundle and version index.
- `sha2` hashes source inputs and generated data.
- `tempfile` provides a safe temporary JSON file for `public-api` extraction.

The docs lock contains two rustdoc model versions because `public-api 0.52.2` depends on `rustdoc-types 0.59.0`, while this crate pins `rustdoc-types 0.60.0`. No compatible newer `public-api` release is available in the local cache. This duplicate is retained until a maintained compatible pair is selected and the extracted API output is requalified.

## Maintained OSS generator components

The current Rust generation stack uses maintained OSS components at pinned versions:

- Protify for Rust owned protobuf declarations and derives.
- Prost and Tonic build tooling through the Actors generator.
- Vendored protoc for reproducible protobuf compilation across release targets.
- `ts-rs` for the current TypeScript type projection.
- `public-api` and `rustdoc-types` for public API extraction from rustdoc JSON.
- Schemars for machine readable docs schemas.

The generation executable invokes these through its Rust source closure. The website consumes the resulting Rust generated docs bundle; it does not become another contract source.

## Feature minimality status

The following are confirmed from manifests and source inspection:

- `sdk-generation` already disables Protify default features and selects only `std`.
- The Actors contract and codegen source directly require Protify, TS-RS, Tonic Prost build, and vendored protoc.
- The docs crate uses all of its direct dependencies.
- No dependency can be removed from `sdk-generation` without changing the current source closure.

The following are candidates for later investigation and are **not completed optimizations**:

- `prost-build` defaults include formatting support; `tonic-prost-build` defaults include transport and Markdown cleanup. Individual build crates may be able to reduce those features if generated output remains byte identical.
- The Actors generator could eventually move to a dedicated `sdk-contract-codegen` package, separating generator-only dependencies from runtime consumers. This requires preserving the exact Rust contract, descriptors, TypeScript output, transport facade, provenance, and artifact hashes.
- The two rustdoc-types versions might be unified after a compatible `public-api` upgrade is available and its extracted signatures are requalified.
- Offline dependency metadata can distinguish optional lockfile package records from feature activated compilation edges more precisely than a lockfile package count.

None of these candidates should be described as done until the relevant generated outputs and source identities have been compared.

## Evidence identities

SHA-256 identities for the reviewed current tree:

```text
Cargo.toml                                      F48D5BD3D03513DBA45EC66DD01223CD9CBAAE02AC919503C864B68AD822F765
Cargo.lock                                      91FDBD0277AB71C696F6844AC30FACC57CCA35C5B7F3455E80A077A49FED819A
rust/crates/sdk-generation/Cargo.toml            37FED366C8C3601FBB2635D1E2EAFF1E1E20EDB7E062641E9CC12D7A4F8C8E0A
rust/crates/sdk-generation/Cargo.lock            BFDB958FAA52A6BD9A52A2A5915ADDD8E913FBB291A3013E6CC4171DC2FE3847
rust/crates/sdk-generation/src/main.rs           19079F8F27E61E2F3D960995AB6A383BA2271A9A799D9472B726E10E1106F0AF
rust/crates/sdk-docs/Cargo.toml                  D0A6436FEEE25E925FCE23AFEE58B9BFCE734FCDDDAFA1C1A690F9A8A7211A47
rust/crates/sdk-docs/Cargo.lock                  BA374B86A6AD72DE7261123816F92FCCB39546E8D64FB95A2FD9E317F4CA1A91
rust/crates/sdk-docs/src/lib.rs                  E3619C865698A33D8C9F6C8546574DA9DBDA4D7A3D154800D55C2DA04FA1058B
rust/crates/actors/Cargo.toml                    29403145B1FFA7BC81357098EF598C953A09B4CD1FA499EC102F8B537F29E5EE
rust/crates/actors/src/contract.rs               9FD71C3A6A6BCB7665E1FF6824852BAFFA56F7BDD5A583B793D42CCB763612FA
rust/crates/actors/src/codegen.rs                D83FB385F7924C7B938071E5B1AA8117399295D778AB1FB951F52B64BD982165
```

No build or download was performed for this audit.
