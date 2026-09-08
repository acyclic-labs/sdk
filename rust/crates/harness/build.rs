//! Builds the canonical harness Protobuf messages and descriptor set.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // This checked mirror is generated from the repository's canonical schema.
    // Keeping the build input inside the crate makes crates.io archives
    // independently buildable instead of depending on the monorepo layout.
    let proto = "proto/harness/v1/harness.proto";
    let include = "proto";
    println!("cargo:rerun-if-changed={proto}");
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc);
    config.boxed(".acyclic.harness.v1.ClientFrame.command");
    config.enum_attribute(
        ".acyclic.harness.v1.ClientFrame.frame",
        "#[allow(clippy::large_enum_variant)]",
    );
    config.file_descriptor_set_path(
        std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("harness_descriptor.bin"),
    );
    config.compile_protos(&[proto], &[include])?;
    Ok(())
}
