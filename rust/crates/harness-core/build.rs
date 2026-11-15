//! Builds the canonical harness Protobuf messages and descriptor set.

#[path = "src/native_environment.rs"]
pub mod native_environment;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ACYCLIC_LOOM=1 (CONTRIBUTING.md) opts Loom models in; the feature alone never does.
    println!("cargo:rerun-if-env-changed=ACYCLIC_LOOM");
    if std::env::var_os("ACYCLIC_LOOM").is_some()
        && std::env::var_os("CARGO_FEATURE_MODEL_CHECKING").is_some()
    {
        println!("cargo:rustc-cfg=loom");
    }
    println!("cargo:rustc-check-cfg=cfg(fuzzing)");
    let compiler = acyclic_proto_build::Compiler::new()?;
    // Crate-owned protocol sources make the published archive independently buildable.
    let proto = "proto/harness/v2/harness.proto";
    let include = "proto";
    println!("cargo:rerun-if-changed=proto");
    println!("cargo:rerun-if-env-changed=PROTOC");
    println!("cargo:rerun-if-env-changed=PROTOC_INCLUDE");
    let mut config = prost_build::Config::new();
    for identity in ["ProviderRef", "VolumeClass", "VolumeOwner", "VolumeRef"] {
        config.extern_path(
            format!(".acyclic.harness.v2.{identity}"),
            format!("::acyclic_machines::wire::selection::{identity}"),
        );
    }
    config.protoc_executable(compiler.executable());
    let descriptor = std::path::PathBuf::from(
        native_environment::variable("OUT_DIR").ok_or("OUT_DIR is unset")?,
    )
    .join("harness_descriptor.bin");
    config.file_descriptor_set_path(&descriptor);
    config.compile_protos(
        &[proto],
        &[
            std::path::PathBuf::from(include),
            compiler.shared().to_owned(),
        ],
    )?;
    let digest = blake3::hash(&std::fs::read(descriptor)?);
    println!(
        "cargo:rustc-env=HARNESS_DESCRIPTOR_DIGEST={}",
        digest.to_hex()
    );
    Ok(())
}
