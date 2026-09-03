//! Generates the public hierarchical Stream gRPC client and server.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let include = protoc_bin_vendored::include_path()?;
    let mut prost = tonic_prost_build::Config::new();
    prost.protoc_executable(protoc);
    prost.bytes([".acyclic.stream.v2"]);
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_with_config(
            prost,
            &[std::path::Path::new("proto/stream/v2/stream.proto")],
            &[std::path::Path::new("proto"), include.as_path()],
        )?;
    println!("cargo:rerun-if-changed=proto/stream/v2/stream.proto");
    Ok(())
}
