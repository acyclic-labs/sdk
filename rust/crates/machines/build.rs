//! Generates Machines transport bindings from the Rust-model descriptor.

use prost::Message;

const MODEL_DESCRIPTOR: &str = "src/generated/acyclic-machines-v1.model.bin";
const MODEL_DESCRIPTOR_ENV: &str = "ACYCLIC_MACHINES_MODEL_DESCRIPTOR";
const ARCHIVED_DESCRIPTOR: &str = "src/generated/acyclic-machines-v1.bin";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let model_path = std::env::var_os(MODEL_DESCRIPTOR_ENV)
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(MODEL_DESCRIPTOR));
    let descriptors =
        prost_types::FileDescriptorSet::decode(std::fs::read(&model_path)?.as_slice())?;
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("wire");
    let messages_dir = out_dir.join("messages");
    let tonic_dir = out_dir.join("tonic");
    std::fs::create_dir_all(&messages_dir)?;

    let mut messages = prost_build::Config::new();
    messages.out_dir(&messages_dir);
    messages.compile_fds(descriptors.clone())?;

    if std::env::var_os("CARGO_FEATURE_GRPC").is_some() {
        std::fs::create_dir_all(&tonic_dir)?;
        tonic_prost_build::configure()
            .out_dir(&tonic_dir)
            .build_client(true)
            .build_server(true)
            .extern_path(".acyclic.machines.v1", "crate::wire")
            .compile_fds_with_config(descriptors, tonic_prost_build::Config::new())?;
    }

    println!("cargo:rerun-if-changed={MODEL_DESCRIPTOR}");
    println!("cargo:rerun-if-changed={}", model_path.display());
    println!("cargo:rerun-if-changed={ARCHIVED_DESCRIPTOR}");
    println!("cargo:rerun-if-env-changed={MODEL_DESCRIPTOR_ENV}");
    Ok(())
}
