//! Generate the public machines contract from the crate-owned protocol.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let grpc = std::env::var_os("CARGO_FEATURE_GRPC").is_some();
    acyclic_proto_build::compile(
        acyclic_proto_build::config()?,
        acyclic_proto_build::services(grpc),
        &["proto/machines/v1/machines.proto"],
        "acyclic-machines-v1.bin",
    )
}
