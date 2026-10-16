//! Generate the public objects contract from the crate-owned protocol.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let grpc = std::env::var_os("CARGO_FEATURE_GRPC").is_some() && std::env::var("CARGO_CFG_TARGET_ARCH")? != "wasm32";
    acyclic_proto_build::compile(
        acyclic_proto_build::config("acyclic-objects-v1.bin")?,
        acyclic_proto_build::services(grpc),
        &["proto/objects/v1/objects.proto"],
    )
}
