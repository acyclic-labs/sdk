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
    config.protoc_executable(&protoc);
    config.boxed(".acyclic.harness.v1.ClientFrame.command");
    config.enum_attribute(
        ".acyclic.harness.v1.ClientFrame.frame",
        "#[allow(clippy::large_enum_variant)]",
    );
    config.file_descriptor_set_path(
        std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("harness_descriptor.bin"),
    );
    config.compile_protos(&[proto], &[include])?;

    #[cfg(feature = "grpc")]
    {
        let grpc_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("grpc");
        std::fs::create_dir_all(&grpc_dir)?;
        let mut grpc_config = tonic_prost_build::Config::new();
        grpc_config.protoc_executable(protoc);
        tonic_prost_build::configure()
            .build_client(true)
            .build_server(true)
            .out_dir(grpc_dir)
            .extern_path(".acyclic.harness.v1", "::acyclic_harness::wire")
            .compile_with_config(grpc_config, &[proto], &[include])?;
    }
    Ok(())
}
