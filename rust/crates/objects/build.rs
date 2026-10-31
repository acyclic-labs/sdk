//! Generate the public objects contract from the crate-owned protocol.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let native = std::env::var("CARGO_CFG_TARGET_ARCH")? != "wasm32";
    let feature = |name: &str| std::env::var_os(format!("CARGO_FEATURE_{name}")).is_some();
    let grpc = feature("GRPC") && native;
    println!("cargo::rustc-check-cfg=cfg(objects_client, objects_server)");
    // Wire decoding for remote clients; wire serving for gRPC and binary adapters.
    if feature("BINARY") || (grpc || feature("HTTP")) && native {
        println!("cargo::rustc-cfg=objects_client");
    }
    if grpc || feature("JSON") || feature("BINARY") {
        println!("cargo::rustc-cfg=objects_server");
    }
    // Bodies share the caller's buffer end to end instead of copying it.
    let mut config = acyclic_proto_build::config()?;
    config.bytes([
        ".acyclic.objects.v1.PutBlobRequest.body",
        ".acyclic.objects.v1.GetBlobResponse.body",
    ]);
    acyclic_proto_build::compile(
        config,
        acyclic_proto_build::services(grpc),
        &["proto/objects/v1/objects.proto"],
        "acyclic-objects-v1.bin",
    )
}
