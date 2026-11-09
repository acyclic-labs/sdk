//! Generate the public inference contract from the crate-owned protocol.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let compiler = acyclic_proto_build::Compiler::new()?;
    let grpc = std::env::var_os("CARGO_FEATURE_HOST").is_some();
    compiler.compile(
        compiler.config(),
        acyclic_proto_build::services(grpc),
        &["proto/inference/v1/inference.proto"],
        "inference_descriptor.bin",
    )
}
