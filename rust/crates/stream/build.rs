//! Generates the public hierarchical Stream gRPC client and server.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rustc-check-cfg=cfg(acyclic_stream_loom)");
    println!("cargo:rustc-check-cfg=cfg(acyclic_loom)");
    let mut prost = acyclic_proto_build::config()?;
    prost.bytes([".acyclic.stream.v1"]);
    let grpc = std::env::var_os("CARGO_FEATURE_GRPC").is_some();
    acyclic_proto_build::compile(
        prost,
        acyclic_proto_build::services(grpc)
            .build_transport(std::env::var("CARGO_CFG_TARGET_ARCH").as_deref() != Ok("wasm32"))
            .build_client(grpc || std::env::var_os("CARGO_FEATURE_WASM").is_some()),
        &[
            "proto/stream/v1/stream.proto",
            "proto/stream/v1/session.proto",
        ],
        "acyclic-stream-v1.bin",
    )
}
