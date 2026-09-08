//! Generates tonic service glue while reusing canonical Harness message types.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "../../../proto/harness/v1/harness.proto";
    let include = "../../../proto";
    let mut prost = tonic_prost_build::Config::new();
    prost.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .extern_path(".acyclic.harness.v1", "::acyclic_harness::wire")
        .compile_with_config(prost, &[proto], &[include])?;
    println!("cargo:rerun-if-changed={proto}");
    Ok(())
}
