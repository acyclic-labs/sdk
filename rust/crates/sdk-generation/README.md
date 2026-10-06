# sdk-generation

This standalone maintainer tool calls the existing Rust-owned `sdk-docs`
library directly against typed rustdoc JSON produced by the Actors build.
Release generation invokes the pinned Rustdoc stage for the Actors package;
preview generation accepts an explicitly supplied external JSON input.

The source closure is declared in `src/main.rs` and covers the workspace
manifest, guide files, Actors, sdk-docs, this launcher, all lockfiles, and the
pinned toolchain. The accepted rustdoc input is exactly one typed
`acyclic_actors` family from the checked-in Actors crate. The public executable
Rust declarations, including `actors/src/wire.rs`, are the source authority;
current generated transport and descriptor files under `actors/src` remain
hashed migration inputs and are not independent authority. Proto files are not
accepted as a substitute source input. The launcher resolves the Git revision
itself, hashes that source closure and the external rustdoc JSON, calls the
existing `sdk-docs` library, and writes a versioned manifest. Release
generation requires a clean checkout; preview generation binds the working-tree
digest. Release drift reruns the pinned Rustdoc stage, while preview drift uses
the supplied JSON input. Release Rustdoc also emits its exact dep-info file;
checkout Markdown named there is added to the source digest, and missing or
escaping Markdown dependencies fail the stage.

```text
cargo +1.98.1 test --offline --locked
cargo +1.98.1 run --offline --locked -- generate \
  --root <checkout> --output <bundle> \
  --version <version> --channel release
cargo +1.98.1 run --offline --locked -- generate \
  --root <checkout> --rustdoc-json <json-or-dir> --output <bundle> \
  --version <version> --channel preview
```
