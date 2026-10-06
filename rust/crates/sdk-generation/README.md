# sdk-generation

This standalone maintainer tool calls the existing Rust-owned `sdk-docs`
library directly against typed rustdoc JSON produced by the Actors build.
Callers provide only the checkout, external rustdoc input, output directory,
package version, and channel.

The source closure is declared in `src/main.rs` and covers the workspace
manifest, guide files, Actors, sdk-docs, this launcher, all lockfiles, and the
pinned toolchain. Proto files are generated inputs and are intentionally
excluded from source authority. The launcher resolves the Git revision itself,
hashes that source closure and the external rustdoc JSON, invokes the fixed
docs command, and writes a versioned manifest. Release generation requires a
clean checkout; preview generation binds the working-tree digest. `drift`
invokes no stage.

```text
cargo +1.98.1 test --offline --locked
cargo +1.98.1 run --offline --locked -- generate \
  --root <checkout> --rustdoc-json <json-or-dir> --output <bundle> \
  --version <version> --channel release
```
