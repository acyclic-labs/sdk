# sdk-generation

This standalone maintainer tool calls the existing Rust-owned `sdk-docs`
library directly against typed rustdoc JSON. The published Rust package set
comes from `release/cargo-crates.json` and is resolved through pinned Cargo
metadata; each package's library target (or its sole binary target) supplies
one documentation family. Release generation invokes the pinned Rustdoc stage
for every resolved package; preview generation accepts explicitly supplied
external JSON input.

The source closure is declared in `src/main.rs` and covers the workspace
manifest, active Cargo configuration, tracked Markdown, every published Rust
package root, sdk-docs, this launcher, all lockfiles, the release package
catalog, and the pinned toolchain. Release Rustdoc dep-info adds its
compiler-consumed Markdown files to that closure. The accepted rustdoc input
must contain exactly one typed family for every package in the catalog. The
launcher resolves the Git revision itself, hashes that source closure and the
rustdoc JSON files, calls the existing `sdk-docs` library, and writes a
versioned manifest. The same run calls the Actors crate's
`domain::export_typescript` stage, so `generated/typescript/actors` is emitted
from the Rust-owned semantic types and included in the artifact digest. Before
either operation, the launcher compares the configured checkout's hashed
compiled-generator inputs (the Actors contract/build sources, the package
catalog, sdk-docs inputs, and this crate's manifest, lockfile, build script,
and launcher sources) with the inputs compiled into this binary, so a bundle
cannot combine a rustdoc checkout with a stale TypeScript exporter,
documentation stage, or dependency lock. Release generation requires a clean
checkout; preview generation binds the working-tree digest. Release drift
reruns the pinned Rustdoc stage, while preview drift uses the supplied JSON
input. Release Rustdoc also emits its exact dep-info file; checkout Markdown
named there is added to the source digest, and missing or escaping Markdown
dependencies fail the stage.

The TypeScript stage also emits `generated/typescript/actors/types.ts`, a
deterministic barrel generated from the `.ts` modules that `ts-rs` produced.
The barrel is included in the artifact digest and is the package `./types`
entrypoint source.

```text
cargo +1.98.1 test --offline --locked
cargo +1.98.1 run --offline --locked -- generate \
  --root <checkout> --output <bundle> \
  --version <version> --channel release
cargo +1.98.1 run --offline --locked -- generate \
  --root <checkout> --rustdoc-json <json-or-dir> --output <bundle> \
  --version <version> --channel preview
```

This launcher is the generation authority for the Rust-owned documentation
families currently covered here (all eleven published docs families) and for
the Actors proto/descriptor, domain, and TypeScript stages. Other workspace
families still use their maintained legacy generators: the root Buf and
per-family scripts remain in the migration path, and Workers plus the other
domain contracts still have authored Rust/proto inputs outside this launcher.
The root commands below are thin bundle consumers. They verify or stage the
exact bundle supplied by this launcher and then reuse the existing workspace
pipeline; they do not create another generation authority. The `generate`
variant runs the legacy root generation first, then stages the Rust bundle, so
the bundle is the final checked-in Rust-owned output for this migration step:

```text
bun run build:with-rust-actors -- <bundle>  # stages with write, then runs build
bun run check:with-rust-actors -- <bundle>  # verifies with check, then runs check
bun run generate:with-rust-actors -- <bundle>  # runs root generation, then stages
bun run check:generated:with-rust-actors -- <bundle>  # verifies staging and generated outputs
```
