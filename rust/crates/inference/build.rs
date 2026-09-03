//! Generates the exact public Inference transport from its canonical schema.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let mut prost = tonic_prost_build::Config::new();
    prost.protoc_executable(protoc);
    prost.bytes(["."]);
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_with_config(
            prost,
            &[
                "../../../proto/harness/v1/harness.proto",
                "../../../proto/inference/v1/inference.proto",
            ],
            &["../../../proto"],
        )?;
    println!("cargo:rerun-if-changed=../../../proto/harness/v1/harness.proto");
    println!("cargo:rerun-if-changed=../../../proto/inference/v1/inference.proto");
    Ok(())
}
