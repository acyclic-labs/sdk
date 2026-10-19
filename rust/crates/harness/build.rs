//! Builds the canonical harness Protobuf messages and descriptor set, plus
//! the tonic service glue under the `grpc` feature.

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
    config.boxed(".acyclic.harness.v2.ClientFrame.command");
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
    #[cfg(feature = "grpc")]
    {
        // Tonic service glue reuses the canonical messages generated above;
        // it lives in its own directory because it shares the package name.
        let out = std::path::PathBuf::from(
            native_environment::variable("OUT_DIR").ok_or("OUT_DIR is unset")?,
        )
        .join("grpc");
        std::fs::create_dir_all(&out)?;
        let mut prost = tonic_prost_build::Config::new();
        prost.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
        tonic_prost_build::configure()
            .build_client(true)
            .build_server(true)
            .out_dir(out)
            .extern_path(".acyclic.harness.v2", "crate::wire")
            .extern_path(".acyclic.protocol.v1", "crate::wire")
            .compile_with_config(prost, &[proto], &[include])?;
    }
    Ok(())
}
