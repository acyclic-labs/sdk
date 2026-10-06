# Rustdoc generation research

Research date: 2026-10-03. This note records the source-of-truth decision and
the constraints that the documentation generator must expose to its callers.

## Existing repository shape

The repository is already Rust-heavy and uses Rust-owned documentation in a
few useful ways:

- The root toolchain is pinned to Rust `1.98.1`.
- Public crates such as `acyclic-fs`, `acyclic-harness`, `acyclic-inference`,
  `acyclic-machines`, `acyclic-objects`, `acyclic-stream`, and
  `acyclic-workers` set `readme = "README.md"` and include those READMEs from
  their crate roots with `#![doc = include_str!("../README.md")]`.
- Harness has additional crate-owned Markdown guides under `rust/crates/harness/docs/`
  and includes them from module roots. Examples live beside the crate under
  `examples/` and are therefore a natural source for executable snippets.
- Generated protobuf sources and descriptor sets are included by Rust crates;
  they must remain provenance inputs, while generated bindings should not be
  hand-authored website content.

The prototype was run against the assigned branch and discovered 19 public
crate families after excluding private standalone `sdk-*` crates. Before that
filter it saw 24 Cargo manifests, 428 Rust/Markdown source files, and 4,934
conservative public declarations. Without compiled rustdoc JSON it emitted 137
unresolved-re-export diagnostics and marked every included crate
`analysis_mode = "source-fallback"`. This is useful coverage evidence, not a
claim that a line scanner has complete Rust semantics.

`RUSTDOCFLAGS=-D warnings cargo doc --workspace --no-deps --locked` currently
exposes a broken intra-doc link in `rust/crates/harness-codex/src/lib.rs`:
`CODEX_VERSION` is referenced from a crate-level doc comment but is not in the
resolved rustdoc namespace. The website gate should treat this as a real
source error. The normal repository lint only warns because the workspace sets
`missing_docs = "warn"`.

## Tool comparison

### Cloudflare Forge

[Forge](https://github.com/cloudflare/forge) is a schema-first OpenAPI surface
tool. Its current repository describes a TypeScript core, a TypeScript SDK
transformer, Astro/Fern documentation packages, and wrappers for Python, Go,
Java, PHP, C#, Ruby, Rust, and Swift. The [launch post](https://blog.cloudflare.com/forge-open-source-generation-pipeline/)
describes the project as early and says that SDKs and API documentation are
future surfaces beyond the existing `cf` CLI pipeline.

Forge is a useful downstream emitter once Rust emits a complete OpenAPI
document, but it cannot itself make Rust source authoritative: its input is
OpenAPI and its orchestration/runtime is TypeScript. Adopting it would retain
a TypeScript generator and would require an additional Rust-to-OpenAPI
contract. It should therefore remain an optional compatibility experiment,
not the core source-of-truth architecture.

### Rustdoc JSON

The [Rustdoc JSON RFC](https://github.com/rust-lang/rfcs/blob/master/text/2963-rustdoc-json.md)
explicitly targets machine consumers that need semantic API information,
centralized documentation, and language bindings. The current Cargo book says
`cargo doc --output-format json` is experimental, nightly-only, and requires
`-Z unstable-options`; the [rustdoc book](https://doc.rust-lang.org/nightly/rustdoc/unstable-features.html)
has the same qualification.

The [`rustdoc-types` definitions](https://github.com/rust-lang/rustdoc-types)
are also currently unstable and must match the JSON format. The type API
exposes a `FORMAT_VERSION`; JSON consumers must check it before deserializing.
The [docs.rs rustdoc JSON service](https://docs.rs/about/rustdoc-json) has
hosted JSON since 2025-05-23, but a downloaded artifact can have a different
format version or rustdoc toolchain from the source revision. It is suitable
for comparison and published-crate inspection, not as an unpinned build input.

The implemented path is an isolated documentation toolchain:

1. Pin the exact Rust toolchain in `rust-toolchain.toml` and keep the matching
   `rustdoc-types` release/commit in the docs generator's own manifest or tool
   metadata.
2. Run `cargo +<pinned-nightly> rustdoc --workspace --no-deps --locked -- -Z unstable-options --output-format json`.
3. Record the rustc/rustdoc version, rustdoc JSON `format_version`, source
   revision, feature set, target, and artifact digests in the bundle.
4. Fail closed on an unsupported format version. Keep the source scanner as a
   diagnostic fallback and label it `source-fallback`.

Use rustdoc JSON for public item identity, re-export resolution, links, docs,
attributes, and spans. Use crate Markdown and doc comments as the verbatim
website prose source. Use Rust examples/doctests as the executable snippet
source. This split avoids scraping unstable HTML while preserving authored
prose and compile-tested examples.

The unstable `--show-coverage --output-format json` path is a useful second
gate: require all public items selected for the website to have docs or an
explicit exclusion reason. It must be run by the same pinned toolchain and
treated as a diagnostic rather than a stable wire format.

## Bundle contract and reproducibility

`rust/crates/sdk-docs` is an isolated workspace crate and provides the first
bundle prototype. Its JSON contract contains:

- the schema version and exact source revision;
- each crate's package/crate name, publication status, analysis mode, and
  content digest;
- exact UTF-8 contents and BLAKE3 digest for crate Markdown, Rust source, and
  examples;
- public declarations with source paths, lines, doc text, generated-source
  markers, and conditional compilation markers;
- rustdoc JSON path, format version, item count, and digest when available;
- stable warning/error diagnostics for unresolved re-exports, unavailable
  rustdoc JSON, and source revision failures.

The output has no generation timestamp. Arrays are sorted, paths use `/`, and
the bundle digest is computed over the canonical payload before the digest
field is added. A website build can therefore key its cache and URL version on
`source_revision + bundle_blake3`; a language SDK build can use the same
identity for its generated package metadata.

The checked-in `docs/rustdoc-profiles.json` makes target and feature coverage
explicit across four profiles: `host-default`, `native-bindings`,
`host-capabilities`, and `wasm-bindings`. The first three use the portable
`host` target token; the docs tool resolves that token with the pinned
toolchain's `rustc -vV` result on the runner and records the resolved target in
each artifact receipt. `wasm-bindings` remains explicitly pinned to
`wasm32-unknown-unknown`. A profile is complete only when every listed package
has a non-empty source-bound rustdoc public graph; strict mode fails for a
missing package, source-fallback crate, or empty graph. The generator evaluates
each profile in its own temporary Cargo target directory, copies the JSON and
receipt into the shared output tree, and removes that disposable build
directory before continuing. This keeps host compilation portable across the
Linux, macOS, Windows, and Linux-arm64 qualification runners while preserving
the target and feature identity needed to distinguish host-specific graphs.

The CLI also emits a compact website projection with `--website-output`. Its
`sdk-reference-bundle.v1` envelope carries the exact source revision, bundle
digest, source state, channel, and profile statuses. Each family carries its
Rust-derived navigation identity, guides, examples, package installation
instructions, and resolved public items. The Svelte layer may render this
projection, but it does not author SDK reference content or generate a second
contract. A stale rustdoc artifact must be rejected before release: the
generator should compare the artifact's source revision/content binding with
the current crate inputs, and a changed public declaration must have a
negative coverage test that fails strict generation.

The prototype intentionally does not invent semantics for `cfg`, macro
expansion, re-exports, trait-associated items, or feature-selected modules.
Those become authoritative only after rustdoc JSON has been generated with the
documented feature set. Any fallback bundle must retain its diagnostics in the
website build so an incomplete source inventory cannot silently become a
published reference page.

## Decision

Use Rust source, crate Markdown, and Rust examples as the authored source of
truth; use pinned rustdoc JSON as the compiler-derived semantic index; and
generate website reference pages, snippets, OpenAPI/descriptor inputs, and
downstream SDK metadata from the resulting versioned bundle. Treat Cloudflare
Forge as an optional downstream OpenAPI emitter only after its output can be
verified against the Rust bundle. Do not make Forge or any other TypeScript
tool the canonical contract.
