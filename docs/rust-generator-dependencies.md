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
sdk-docs = { path = "../sdk-docs" }
serde = { version = "=1.0.228", features = ["derive"] }
serde_json = "=1.0.145"
sha2 = "=0.10.9"
```

`sha2` is also used by the crate's build script to hash the compiled TypeScript source closure.

## Current dependency boundary

`sdk-generation` uses the real `acyclic-actors` crate API for both generated
outputs: `acyclic_actors::contract::render_proto_files` renders the
Rust-owned intermediate proto, `acyclic_actors::FILE_DESCRIPTOR_SET` supplies
the descriptor compiled by the Actors build, and
`acyclic_actors::domain::export_typescript` emits the TypeScript artifact.
There are no source-path imports of the Actors contract or code generator and
no second Protify or tonic compilation in the standalone launcher.

The Actors crate remains the owner of the contract toolchain. Its manifest and
build dependencies pin Protify, ts-rs, tonic-prost-build, and vendored protoc;
those packages remain in the standalone lock transitively because the
`acyclic-actors` dependency must build its canonical wire types and embedded
descriptor. They are not direct `sdk-generation` dependencies.

| Dependency | Current owner/use | Current decision |
| --- | --- | --- |
| `acyclic-actors` | Public contract renderer, embedded descriptor, and TypeScript export | Keep |
| `protify` | Actors contract declarations and Actors build output | Keep as an Actors dependency; not direct here |
| `ts-rs` | Actors semantic TypeScript export | Keep as an Actors dependency; not direct here |
| `tonic-prost-build` | Actors build script's tonic facade and descriptor generation | Keep as an Actors build dependency; not direct here |
| `protoc-bin-vendored` | Actors build script's pinned protoc executable | Keep as an Actors build dependency; not direct here |
| `depinfo` | Rustdoc dep-info parsing | Keep |
| `sdk-docs` | Rustdoc and source docs projection | Keep |
| `serde`, `serde_json` | Generation manifests and docs data | Keep |
| `sha2` | Source, tool, and artifact hashes | Keep |

The generated proto and embedded descriptor are bundle artifacts. The
standalone launcher does not emit a second tonic Rust facade; the product
Actors crate continues to generate and consume its tonic facade from its own
`build.rs` and `OUT_DIR`.

## Protify feature selection

The Actors crate pins Protify with:

```toml
protify = { version = "=0.1.4", default-features = false, features = ["std"] }
```

This is the proven minimal feature selection for the Rust-owned Actors
contract. Protify's `default` feature set additionally enables `regex`, `cel`,
`chrono`, and `inventory`; the Actors crate disables defaults and selects
`std` only.

The cached Protify manifests show that `std` does not enable CEL. The `cel` feature separately enables the CEL dependency plus CEL support in `proto-types` and the proc macro. The lock's `cel` package record is not sufficient evidence that the Protify CEL feature is compiled; it appears in the resolved package metadata through optional dependency declarations. No offline feature-resolved tree was run here.

Do not enable Protify defaults. Do not add Protify as a direct dependency of
`sdk-generation`; the Actors crate remains the contract and toolchain owner.

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
- Individual workspace build crates may reduce these features if generated output remains byte identical. The Actors build uses the Tonic builder and must preserve its transport facade and descriptor output.
- The `sdk-generation` lock package count must not be used as a build-size claim until a feature-resolved offline metadata inspection is recorded.

## Source authority

The source manifests and lockfiles remain the dependency authority. This
document intentionally avoids recording mutable file hashes that become stale
when the generator source closure changes.
