//! Generates the customer transport from the Rust-model descriptor.

use prost::Message;

const MODEL_DESCRIPTOR: &str = "inference_model_descriptor.bin";
const DOC_DESCRIPTOR: &str = "inference_model_descriptor_docs.bin";
const MODEL_SOURCE: &str = "../sdk-contract-wire/src/inference.rs";
const MODEL_DESCRIPTOR_ENV: &str = "ACYCLIC_INFERENCE_MODEL_DESCRIPTOR";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let descriptor_path = std::env::var_os(MODEL_DESCRIPTOR_ENV)
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(DOC_DESCRIPTOR));
    let descriptors =
        prost_types::FileDescriptorSet::decode(std::fs::read(&descriptor_path)?.as_slice())?;
    let mut descriptors = descriptors;
    let control = prost_types::FileDescriptorSet::decode(
        acyclic_sdk_contract_wire::transport_control::control_descriptor().as_slice(),
    )?;
    descriptors.file.extend(control.file);
    let native_target = std::env::var("CARGO_CFG_TARGET_ARCH").as_deref() != Ok("wasm32");
    if std::env::var_os("CARGO_FEATURE_HOST").is_some() && native_target {
        tonic_prost_build::configure()
            .build_client(true)
            .build_server(true)
            .compile_fds_with_config(descriptors, tonic_prost_build::Config::new())?;
    } else {
        prost_build::Config::new().compile_fds(descriptors)?;
    }
    println!("cargo:rerun-if-changed={MODEL_DESCRIPTOR}");
    println!("cargo:rerun-if-changed={DOC_DESCRIPTOR}");
    println!("cargo:rerun-if-changed={MODEL_SOURCE}");
    println!("cargo:rerun-if-changed={}", descriptor_path.display());
    println!("cargo:rerun-if-env-changed={MODEL_DESCRIPTOR_ENV}");
    Ok(())
}
