# Inference and Machines authority migration prototype

This directory records a bounded migration proof. It does not change either
product crate's build script or switch package ownership. The Rust model
generator remains the only input used to produce the isolated `.proto` and
descriptor artifacts in the proof.

The integration test at
`rust/crates/sdk-contract-wire/tests/authority_migration.rs` demonstrates
three checks:

1. model-generated Inference and Machines descriptors are semantically
   compatible with the descriptor inputs consumed by the current public Rust
   packages;
2. both generated descriptors remain compatible with the immutable archived
   handshake fixtures, including custom validation option bytes; and
3. mutations to generated `.proto` and descriptor outputs are rejected by
   `check` and restored by `generate` from the Rust model, without reading the
   authored `proto/` tree.

The archive SHA-256 gates are deliberately recorded in the test. They qualify
the existing compatibility bytes and make accidental fixture replacement
visible. Producer source locations, Buf image metadata, and protobuf field
ordering are treated as non-contract annotations by the semantic validator.

## Evidence boundary

The passing test is qualified evidence for descriptor and regeneration
authority. It does not claim that Inference or Machines product `build.rs`
ownership has migrated, nor does it replace the package owner's generated
Rust API compilation check. A follow-up migration can reuse this isolated
proof after the product build-script owner approves the switch and records a
real generated-source receipt.

Run the proof with:

```text
cargo test --locked --manifest-path rust/crates/sdk-contract-wire/Cargo.toml --test inference_model --test authority_migration
```
