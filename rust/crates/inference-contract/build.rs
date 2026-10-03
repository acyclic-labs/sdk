//! Generates transport-independent customer protobuf messages from the Rust-model descriptor.

use prost::Message;

const MODEL_DESCRIPTOR: &str = "inference_model_descriptor.bin";
const MODEL_DESCRIPTOR_ENV: &str = "ACYCLIC_INFERENCE_CONTRACT_MODEL_DESCRIPTOR";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let descriptor_path = std::env::var_os(MODEL_DESCRIPTOR_ENV)
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(MODEL_DESCRIPTOR));
    let descriptors =
        prost_types::FileDescriptorSet::decode(std::fs::read(&descriptor_path)?.as_slice())?;
    prost_build::Config::new().compile_fds(descriptors)?;
    println!("cargo:rerun-if-changed={MODEL_DESCRIPTOR}");
    println!("cargo:rerun-if-changed={}", descriptor_path.display());
    println!("cargo:rerun-if-env-changed={MODEL_DESCRIPTOR_ENV}");
    Ok(())
}
