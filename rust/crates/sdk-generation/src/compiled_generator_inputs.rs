//! Source files that define the compiled SDK generator's behavior.

/// Files whose contents or dependency selection can change generated
/// TypeScript or documentation output.
pub const PATHS: &[&str] = &[
    "rust/crates/actors/src/codegen.rs",
    "rust/crates/actors/src/client.rs",
    "rust/crates/actors/src/contract.rs",
    "rust/crates/actors/src/contract_definitions.rs",
    "rust/crates/actors/src/domain.rs",
    "rust/crates/actors/src/domain/kani_proofs.rs",
    "rust/crates/actors/src/grpc.rs",
    "rust/crates/actors/src/http.rs",
    "rust/crates/actors/src/wire.rs",
    "rust/crates/actors/Cargo.toml",
    "rust/crates/actors/build.rs",
    "rust/crates/actors/src/lib.rs",
    "rust/crates/sdk-docs/Cargo.toml",
    "rust/crates/sdk-docs/Cargo.lock",
    "rust/crates/sdk-docs/src/lib.rs",
    "rust/crates/sdk-docs/src/public_api.rs",
    "rust/crates/sdk-generation/Cargo.toml",
    "rust/crates/sdk-generation/Cargo.lock",
    "rust/crates/sdk-generation/build.rs",
    "rust/crates/sdk-generation/src/compiled_generator_inputs.rs",
    "rust/crates/sdk-generation/src/main.rs",
];
