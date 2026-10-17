//! Generate the public objects contract from the crate-owned protocol.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rustc-check-cfg=cfg(objects_loom)");
    let grpc = std::env::var_os("CARGO_FEATURE_GRPC").is_some()
        && std::env::var("CARGO_CFG_TARGET_ARCH")? != "wasm32";
    acyclic_proto_build::compile(
        acyclic_proto_build::config()?,
        acyclic_proto_build::services(grpc),
        &["proto/objects/v1/objects.proto"],
        "acyclic-objects-v1.bin",
    )
}
