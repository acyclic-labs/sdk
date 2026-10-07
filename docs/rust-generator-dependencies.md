# Rust generator dependency audit

This document records the dependency boundary for the Rust owned SDK and documentation generator in the current worktree.

**Reviewed tree:** `C:\Users\varun\.codex\worktrees\rust-source-foundation\sdk`  
**Review date:** 2026-10-07  
**Method:** source and pinned manifest/lock inspection only. No build, download, or dependency resolution was run.

## Current graph

The complete workspace lock contains **570 package records**. The standalone [`rust/crates/sdk-generation/Cargo.lock`](../rust/crates/sdk-generation/Cargo.lock) now contains **312 package records**, up from 209 after the Actors dependency was added. The increase is real and is part of the current source graph; the earlier 209 figure is stale.

The current [`rust/crates/sdk-generation/Cargo.toml`](../rust/crates/sdk-generation/Cargo.toml) has these direct dependencies:

```toml
acyclic-actors = { path = "../actors", default-features = false }
depinfo = "=0.7.10"
protoc-bin-vendored = "=3.2.0"
protify = { version = "=0.1.4", default-features = false, features = ["std"] }
sdk-docs = { path = "../sdk-docs" }
serde = { version = "=1.0.228", features = ["derive"] }
serde_json = "=1.0.145"
sha2 = "=0.10.9"
tonic-prost-build = "=0.14.6"
ts-rs = "=12.0.1"
```

`sha2` is also used by the crate's build script to hash the compiled TypeScript source closure.

## Why the heavy dependencies are currently required

The generator binary imports the Actors implementation in two ways:

```rust
#[path = "../../actors/src/contract.rs"]
mod contract;
#[path = "../../actors/src/codegen.rs"]
mod actors_codegen;
```

The first imported file uses `protify::*` for the Rust contract declarations and `ts_rs::TS` with a `TS` derive. The second uses `tonic_prost_build` and `protoc_bin_vendored` to render the canonical Actors proto, descriptor, and transport facade. `main.rs` calls that imported generator directly in `generate_actors_contract_artifacts`.

The binary also depends on the real `acyclic-actors` crate API: `acyclic_actors::domain::export_typescript` emits the TypeScript artifact from the compiled Rust crate. That dependency was the source of the current 103-record lock increase. The lock now contains the `acyclic-actors` package and its own Protify, TS-RS, Tonic, and vendored-protoc edges in addition to the direct source-import edges.

| Dependency | Current owner/use | Current decision |
| --- | --- | --- |
| `acyclic-actors` | Real API for TypeScript export and compiled Actors source identity | Keep |
| `protify` | Source-included `actors/src/contract.rs` | Keep while path import remains |
| `ts-rs` | Source-included `actors/src/contract.rs` | Keep while path import remains |
| `tonic-prost-build` | Source-included `actors/src/codegen.rs` | Keep while path import remains |
| `protoc-bin-vendored` | Source-included `actors/src/codegen.rs` | Keep while path import remains |
| `depinfo` | Rustdoc dep-info parsing | Keep |
| `sdk-docs` | Rustdoc and source docs projection | Keep |
| `serde`, `serde_json` | Generation manifests and docs data | Keep |
| `sha2` | Source, tool, and artifact hashes | Keep |

There is no truthful direct-dependency deletion in the current source graph. Removing the four codegen dependencies without first removing the `#[path]` imports breaks compilation or generated output.

## Duplicate source graph and exact cleanup path

The same physical Actors contract is compiled once as part of `acyclic-actors` and again through the `#[path]` import in `sdk-generation`. The TypeScript projection already uses the real crate API, but the contract renderer and transport generator still use source paths because `actors/src/codegen.rs` is not a public module of `acyclic-actors`.

The maintainable cleanup is an API boundary change, followed by lock and output verification:

1. Expose a maintainer-only `codegen` API from the Actors source, or create a dedicated `acyclic-actors-codegen` package. The API must call the existing `acyclic_actors::contract::render_proto_files` implementation and retain the existing `tonic-prost-build` and vendored-protoc recipe.
2. Keep generator dependencies behind that codegen API/feature. The published runtime Actors crate should not gain unconditional generator dependencies merely to make the function public.
3. Change `sdk-generation` to call the real codegen API and retain `acyclic_actors::domain::export_typescript`; remove the two `#[path]` modules only after the new API emits the same descriptor, proto, transport facade, and TypeScript artifacts.
4. Once the path imports are gone, remove `protify`, `ts-rs`, `tonic-prost-build`, and `protoc-bin-vendored` from `sdk-generation`'s direct manifest. They will remain where the Actors contract and codegen actually own them.
5. Recreate the standalone generation lock and compare source closure, generated artifact hashes, provenance, and docs bundle hashes. Check the standalone workspace bootstrap explicitly; the existing path dependency arrangement must continue to resolve without requiring the root workspace to be bootstrapped first.

This is a proposed refactor, not a completed optimization. No source API move was made in this review.

## Protify feature selection

The current generator pins Protify with:

```toml
protify = { version = "=0.1.4", default-features = false, features = ["std"] }
```

This is the proven minimal feature selection for the imported Actors contract. Protify's `default` feature set additionally enables `regex`, `cel`, `chrono`, and `inventory`; the current generator disables defaults and selects `std` only.

The cached Protify manifests show that `std` does not enable CEL. The `cel` feature separately enables the CEL dependency plus CEL support in `proto-types` and the proc macro. The lock's `cel` package record is not sufficient evidence that the Protify CEL feature is compiled; it appears in the resolved package metadata through optional dependency declarations. No offline feature-resolved tree was run here.

Do not enable Protify defaults. Do not remove Protify from the current generator until the source-import refactor above has been completed and qualified.

## Documentation dependencies

The local [`sdk-docs`](../rust/crates/sdk-docs) crate uses every declared dependency:

- `public-api` extracts public API signatures from rustdoc JSON.
- `rustdoc-types` is the pinned rustdoc JSON model.
- `schemars` emits bundle schemas.
- `semver` validates released documentation versions.
- `serde` and `serde_json` serialize bundles and indexes.
- `sha2` hashes source inputs and generated data.
- `tempfile` provides temporary JSON files for public API extraction.

Its lock contains two rustdoc model versions because `public-api 0.52.2` requires `rustdoc-types 0.59.0`, while `sdk-docs` pins 0.60.0. No compatible newer `public-api` release is available in the local cache. Do not force unification without requalifying extracted signatures.

## Feature optimization candidates not yet verified

These are investigations only, not completed reductions:

- `prost-build 0.14.4` defaults to `format`, which enables `prettyplease` and `syn`; `cleanup-markdown` adds `pulldown-cmark` and `pulldown-cmark-to-cmark`.
- `tonic-prost-build 0.14.6` defaults to `transport` and `cleanup-markdown`.
- Individual workspace build crates may reduce these features if generated output remains byte identical. The current Actors generator uses the Tonic builder and must preserve its transport facade and descriptor output.
- The `sdk-generation` lock package count must not be used as a build-size claim until a feature-resolved offline metadata inspection is recorded.

## Evidence hashes

SHA-256 identities from the exact tree at review time:

```text
Cargo.lock                                      91FDBD0277AB71C696F6844AC30FACC57CCA35C5B7F3455E80A077A49FED819A
rust/crates/sdk-generation/Cargo.toml            37CC53839F4A984EEE55776A5ECE4BF58AE530E3DE1AF30EE2A1A750A952D11A
rust/crates/sdk-generation/Cargo.lock            81D412F70930A7FE7C3E8E0FAE37D20FE61D419E6C2948FAF91CF582EB276171
rust/crates/sdk-generation/src/main.rs           E699539F1E185D3BAA02A5DB48125DD746E6965215F83DD997AC392585DBF6DF
rust/crates/sdk-generation/build.rs              61EDFD5051788935531EAD3278AACF3948F6570541416D9CE6E9297E169DD4B0
rust/crates/actors/Cargo.toml                    29403145B1FFA7BC81357098EF598C953A09B4CD1FA499EC102F8B537F29E5EE
rust/crates/actors/src/lib.rs                    37BDD77E0AB5F4A6995E4DD03E4028CE4886FA0E6A2A06FCEFCC63CC8BCFA4CA
rust/crates/actors/src/contract.rs               505832C4DE1EB148AA773312C042748C09743C7F8FD44622F6B5B2B2A8F952F0
rust/crates/actors/src/codegen.rs                D83FB385F7924C7B938071E5B1AA8117399295D778AB1FB951F52B64BD982165
rust/crates/actors/src/domain.rs                  418620A3B3AAFB251E97013F3C3B12286D11316D0F1E9447FC2CB263BDADEC
rust/crates/actors/src/wire.rs                    88E74B1DFB9F7C4309A6DE8AEDCBF5EA53FC15D5439251CC914771F97A749115
rust/crates/sdk-docs/Cargo.toml                  D0A6436FEEE25E925FCE23AFEE58B9BFCE734FCDDDAFA1C1A690F9A8A7211A47
rust/crates/sdk-docs/Cargo.lock                  BA374B86A6AD72DE7261123816F92FCCB39546E8D64FB95A2FD9E317F4CA1A91
```

No build or download was performed for this update.
