# Filesystem native companion packages

The platform companion package matrix for `@acyclic-labs/fs` is owned by this
Rust source directory. The Rust TypeScript generator reads each target
manifest and emits the companion dependency set and target tuple into the
generated TypeScript package.

The native addon is built from `rust/crates/filesystem-napi`. Release tooling
places the resulting `acyclic-fs.node` beside each target manifest's loader.
