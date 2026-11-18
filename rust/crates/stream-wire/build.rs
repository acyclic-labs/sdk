//! Generates Stream gRPC clients and native servers.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let compiler = acyclic_proto_build::Compiler::new()?;
    let mut prost = compiler.config();
    prost.bytes([".acyclic.stream.v1"]);

    let native = std::env::var("CARGO_CFG_TARGET_ARCH")? != "wasm32";
    compiler.compile(
        prost,
        acyclic_proto_build::services(native)
            .build_transport(native)
            .build_client(true),
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
