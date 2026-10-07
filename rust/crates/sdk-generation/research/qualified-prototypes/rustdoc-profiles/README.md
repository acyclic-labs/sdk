# Rust docs profile prototype

This isolated crate prototypes Rust-owned availability metadata for SDK
documentation. Cargo metadata supplies the feature names and the installed
target set supplies target validity. Each valid profile is expected to produce
one `rustdoc-types` 0.60 Rustdoc JSON receipt; a failed profile is a release
failure rather than a reason to silently omit items. Projection merging keeps
the full signature in the identity key and unions the profiles that actually
contained that item.

The owner resolver also reads Cargo package publication and dependency
metadata. A private WASM, N-API, or UniFFI binding is attached to its sole
published Rust dependency while retaining the binding package as the Rustdoc
receipt source. Ambiguous or ownerless private packages fail closed, so
language-facing Rust FFI types cannot disappear from the availability graph.

The crate is intentionally outside the SDK generator checkout. It has no
hand-maintained TypeScript or JSON API catalog and does not change the current
generation pipeline.

