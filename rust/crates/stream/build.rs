//! Generates the public hierarchical Stream gRPC client and server.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let compiler = acyclic_proto_build::Compiler::new()?;
    println!("cargo:rustc-check-cfg=cfg(loom)");
    // One definition per target predicate, so every gate names its users.
    let native = std::env::var("CARGO_CFG_TARGET_ARCH").as_deref() != Ok("wasm32");
    let feature = |name: &str| std::env::var_os(format!("CARGO_FEATURE_{name}")).is_some();
    // Session transports: native, or the browser build.
    let host_runtime = native || feature("WASM");
    // Replica hosts: the session hosts plus the wasm journal provider.
    let replica_host = host_runtime || feature("WASM_JOURNAL");
    // Journal codec users: the replica hosts plus the codec fuzz support.
    let journal_codec = replica_host || feature("TEST_SUPPORT");
    for (name, on) in [
        ("host_runtime", host_runtime),
        ("replica_host", replica_host),
        ("journal_codec", journal_codec),
    ] {
        println!("cargo:rustc-check-cfg=cfg({name})");
        if on {
            println!("cargo:rustc-cfg={name}");
        }
    }
    let mut prost = compiler.config();
    prost.bytes([".acyclic.stream.v1"]);

    compiler.compile(
        prost,
        acyclic_proto_build::services(true)
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
