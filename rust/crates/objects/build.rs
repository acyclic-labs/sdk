//! Generates the public Objects gRPC client and server from the canonical SDK schema.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let include = protoc_bin_vendored::include_path()?;
    let mut prost = prost_build::Config::new();
    prost.protoc_executable(protoc);
    let output = std::path::PathBuf::from(
        std::env::var_os("OUT_DIR").ok_or("Cargo did not provide OUT_DIR")?,
    );
    prost.file_descriptor_set_path(output.join("acyclic-objects-v1.bin"));
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_with_config(
            prost,
            &[std::path::Path::new(
                "../../../proto/objects/v1/objects.proto",
            )],
            &[std::path::Path::new("../../../proto"), include.as_path()],
        )?;
    println!("cargo:rerun-if-changed=../../../proto/objects/v1/objects.proto");
    Ok(())
}
