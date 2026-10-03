//! Builds the canonical harness Protobuf messages and descriptor set, plus
//! the tonic service glue under the `grpc` feature.
//!
//! The Rust model descriptor is the default binding input. The immutable
//! handshake archive is copied separately to `OUT_DIR`; it is never replaced
//! by the source-info-free model descriptor. Both artifacts are committed
//! under `src/generated` so packaged builds do not depend on the private
//! nested contract workspace.

use prost::Message;

const MODEL_DESCRIPTOR_ENV: &str = "ACYCLIC_HARNESS_MODEL_DESCRIPTOR";
const ARCHIVED_DESCRIPTOR_ENV: &str = "ACYCLIC_HARNESS_ARCHIVED_DESCRIPTOR";
const MODEL_DESCRIPTOR: &[u8] = include_bytes!("src/generated/rust-model-harness-v2.bin");
const ARCHIVED_DESCRIPTOR: &[u8] = include_bytes!("src/generated/harness-archived-v2.bin");

// The model descriptor is deliberately source-info-free.  These Rust sources
// own the generation-only documentation overlay; track them explicitly so a
// changed model comment rebuilds both prost products once the shared overlay
// helper is consumed here.
const RUST_MODEL_SOURCES: &[&str] = &[
    "../sdk-contract-wire/src/bindings.rs",
    "../sdk-contract-wire/src/filesystem.rs",
    "../sdk-contract-wire/src/harness.rs",
    "../sdk-contract-wire/src/lib.rs",
    "../sdk-contract-wire/src/protocol.rs",
    "../sdk-contract-options/src/lib.rs",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/generated/rust-model-harness-v2.bin");
    println!("cargo:rerun-if-changed=src/generated/harness-archived-v2.bin");
    for source in RUST_MODEL_SOURCES {
        println!("cargo:rerun-if-changed={source}");
    }
    println!("cargo:rerun-if-env-changed={MODEL_DESCRIPTOR_ENV}");
    println!("cargo:rerun-if-env-changed={ARCHIVED_DESCRIPTOR_ENV}");
    let mut config = prost_build::Config::new();
    config.boxed(".acyclic.harness.v2.ClientFrame.command");
    config.enum_attribute(
        ".acyclic.harness.v2.ClientFrame.frame",
        "#[allow(clippy::large_enum_variant)]",
    );
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    let model_bytes = if let Some(path) = std::env::var_os(MODEL_DESCRIPTOR_ENV) {
        let path = std::path::PathBuf::from(path);
        println!("cargo:rerun-if-changed={}", path.display());
        std::fs::read(path)?
    } else {
        MODEL_DESCRIPTOR.to_vec()
    };
    let model_with_docs = acyclic_sdk_contract_wire::descriptor_set_with_docs(
        acyclic_sdk_contract_wire::BindingFamily::Harness,
        &model_bytes,
    )?;
    let descriptors = prost_types::FileDescriptorSet::decode(model_with_docs.as_slice())?;
    config.compile_fds(descriptors.clone())?;

    let archive = if let Some(path) = std::env::var_os(ARCHIVED_DESCRIPTOR_ENV) {
        let path = std::path::PathBuf::from(path);
        println!("cargo:rerun-if-changed={}", path.display());
        std::fs::read(path)?
    } else {
        ARCHIVED_DESCRIPTOR.to_vec()
    };
    std::fs::write(out_dir.join("harness_descriptor.bin"), archive)?;
    #[cfg(feature = "grpc")]
    {
        // Tonic service glue reuses the canonical messages generated above;
        // it lives in its own directory because it shares the package name.
        let out = std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("grpc");
        std::fs::create_dir_all(&out)?;
        let prost = tonic_prost_build::Config::new();
        let grpc = tonic_prost_build::configure()
            .build_client(true)
            .build_server(true)
            .out_dir(out)
            .extern_path(".acyclic.harness.v2", "crate::wire")
            .extern_path(".acyclic.protocol.v1", "crate::wire");
        grpc.compile_fds_with_config(descriptors, prost)?;
    }
    Ok(())
}
