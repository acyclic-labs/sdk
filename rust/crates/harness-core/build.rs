//! Builds the canonical harness Protobuf messages and descriptor set.

#[path = "src/native_environment.rs"]
pub mod native_environment;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rustc-check-cfg=cfg(fuzzing)");
    let compiler = acyclic_proto_build::Compiler::new()?;
    // Crate-owned protocol sources make the published archive independently buildable.
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let proto = root.join("proto/harness/v2/harness.proto");
    let include = root.join("proto");
    println!("cargo:rerun-if-changed={}", include.display());
    println!("cargo:rerun-if-env-changed=PROTOC");
    println!("cargo:rerun-if-env-changed=PROTOC_INCLUDE");
    println!(
        "cargo:rerun-if-changed={}",
        acyclic_proto_build::shared_protocols().display()
    );
    let mut config = prost_build::Config::new();
    if native_environment::variable("CARGO_FEATURE_MACHINES").is_some() {
        for identity in ["ProviderRef", "VolumeClass", "VolumeOwner", "VolumeRef"] {
            config.extern_path(
                format!(".acyclic.harness.v2.{identity}"),
                format!("::acyclic_machines::wire::selection::{identity}"),
            );
        }
    }
    config.protoc_executable(compiler.executable());
    let descriptor = std::path::PathBuf::from(
        native_environment::variable("OUT_DIR").ok_or("OUT_DIR is unset")?,
    )
    .join("harness_descriptor.bin");
    config.file_descriptor_set_path(&descriptor);
    config.compile_protos(
        &[proto],
        &[include, acyclic_proto_build::shared_protocols()],
    )?;
    let digest = blake3::hash(&std::fs::read(descriptor)?);
    println!(
        "cargo:rustc-env=HARNESS_DESCRIPTOR_DIGEST={}",
        digest.to_hex()
    );
    Ok(())
}
