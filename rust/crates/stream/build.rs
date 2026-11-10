//! Generates the public hierarchical Stream gRPC client and server.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let compiler = acyclic_proto_build::Compiler::new()?;
    println!("cargo:rustc-check-cfg=cfg(loom)");
    println!("cargo:rustc-check-cfg=cfg(loom)");
    let mut prost = compiler.config();
    prost.bytes([".acyclic.stream.v1"]);
    let grpc = std::env::var_os("CARGO_FEATURE_GRPC").is_some();
    compiler.compile(
        prost,
        acyclic_proto_build::services(grpc)
            .build_transport(std::env::var("CARGO_CFG_TARGET_ARCH").as_deref() != Ok("wasm32"))
            .build_client(grpc || std::env::var_os("CARGO_FEATURE_WASM").is_some()),
        &[
            "proto/stream/v1/stream.proto",
            "proto/stream/v1/session.proto",
        ],
        "acyclic-stream-v1.bin",
    )?;
    let output = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    acyclic_proto_build::write_authorization(
        &std::fs::read(output.join("acyclic-stream-v1.bin"))?,
        "acyclic.stream.v1.StreamService",
        "StreamRpc",
        &output,
    )
}
