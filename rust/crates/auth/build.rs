//! Generate the public auth contract from the crate-owned protocol.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let grpc = false;
    acyclic_proto_build::compile(
        acyclic_proto_build::config()?,
        acyclic_proto_build::services(grpc),
        &["proto/auth/v1/auth.proto"],
        "acyclic-auth-v1.bin",
    )
}
