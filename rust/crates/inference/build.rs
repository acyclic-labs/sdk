//! Generate the public inference contract from the crate-owned protocol.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let grpc = std::env::var_os("CARGO_FEATURE_HOST").is_some();
    acyclic_proto_build::compile(
        acyclic_proto_build::config()?,
        acyclic_proto_build::services(grpc),
        &["proto/inference/v1/inference.proto"],
        "inference_descriptor.bin",
    )
}
