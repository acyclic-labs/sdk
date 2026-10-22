//! Generate the packaged Filesystem protocol independently of its engine.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let native_transport = std::env::var("CARGO_CFG_TARGET_ARCH")? != "wasm32";
    acyclic_proto_build::compile(
        acyclic_proto_build::config()?,
        acyclic_proto_build::services(native_transport),
        &["proto/filesystem/v2/filesystem.proto"],
        "acyclic-filesystem-v2.bin",
    )?;
    Ok(())
}
