# Language SDK generation

Rust owns public contracts, wire identities, validation, shared behavior,
language target metadata, package identities, and documentation inputs.
Edit `src/language_catalog.rs` to change a language target or generator pin.
`languages/generation-targets.json`, its validation schema, and
`languages/package-names.json` are generated projections.

The `sdk-generation` Rust executable provides `generate`, `check`, and `drift`
modes. Its `catalog` mode regenerates metadata without running downstream
language toolchains. Generation records source identity, pinned tool versions,
and artifact hashes. Drift checks reject edited or stale projections.

Remote SDKs consume Rust-generated descriptors and OpenAPI through pinned OSS
generators, with idiomatic public facades emitted from Rust descriptor and
semantic metadata. Complex embedded behavior executes in Rust through native
or WASM bindings. Presentation adapters consume those generated packages.

Package metadata describes available producers. Qualification requires actual
installed-package tests bound to the matching source and artifact identities;
metadata alone does not qualify a language. Remote and embedded capabilities
are tracked separately.

Run substantial downstream qualification locally on Windows, Linux, and macOS.
Ordinary CI runs the fast contract and drift checks. Release or explicit manual
qualification runs the broader language matrix.

Documentation references come from Rust comments. Guides belong to crates and
are included by rustdoc. Executable Rust-owned scenarios provide language
snippets, with consumer compilation or execution against matching packages.
The website renders generated bundles and selects released versions, defaulting
to the latest released documentation.
