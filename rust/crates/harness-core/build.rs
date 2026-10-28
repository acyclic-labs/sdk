//! Builds the canonical harness Protobuf messages and descriptor set.

#[path = "src/native_environment.rs"]
pub mod native_environment;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rustc-check-cfg=cfg(fuzzing)");
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // Crate-owned protocol sources make the published archive independently buildable.
    let proto = "proto/harness/v2/harness.proto";
    let include = "proto";
    println!("cargo:rerun-if-changed=proto");
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc);
    let descriptor = std::path::PathBuf::from(
        native_environment::variable("OUT_DIR").ok_or("OUT_DIR is unset")?,
    )
    .join("harness_descriptor.bin");
    config.file_descriptor_set_path(&descriptor);
    config.compile_protos(&[proto], &[include])?;
    let digest = blake3::hash(&std::fs::read(descriptor)?);
    println!(
        "cargo:rustc-env=HARNESS_DESCRIPTOR_DIGEST={}",
        digest.to_hex()
    );
    Ok(())
}
