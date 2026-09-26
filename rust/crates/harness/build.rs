//! Builds the canonical harness Protobuf messages and descriptor set, plus
//! the tonic service glue under the `grpc` feature.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // This checked mirror is generated from the repository's canonical schema.
    // Keeping the build input inside the crate makes crates.io archives
    // independently buildable instead of depending on the monorepo layout.
    let proto = "proto/harness/v2/harness.proto";
    let include = "proto";
    println!("cargo:rerun-if-changed={proto}");
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc);
    config.boxed(".acyclic.harness.v2.ClientFrame.command");
    config.enum_attribute(
        ".acyclic.harness.v2.ClientFrame.frame",
        "#[allow(clippy::large_enum_variant)]",
    );
    config.file_descriptor_set_path(
        std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("harness_descriptor.bin"),
    );
    config.compile_protos(&[proto], &[include])?;
    #[cfg(feature = "grpc")]
    {
        // Tonic service glue reuses the canonical messages generated above;
        // it lives in its own directory because it shares the package name.
        let out = std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("grpc");
        std::fs::create_dir_all(&out)?;
        let mut prost = tonic_prost_build::Config::new();
        prost.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
        tonic_prost_build::configure()
            .build_client(true)
            .build_server(true)
            .out_dir(out)
            .extern_path(".acyclic.harness.v2", "crate::wire")
            .compile_with_config(prost, &[proto], &[include])?;
    }
    Ok(())
}
