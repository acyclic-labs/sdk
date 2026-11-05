//! Generate the packaged Filesystem protocol independently of its engine.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let native_transport = std::env::var("CARGO_CFG_TARGET_ARCH")? != "wasm32";
    acyclic_proto_build::compile(
        acyclic_proto_build::config()?,
        acyclic_proto_build::services(native_transport),
        &["proto/filesystem/v2/filesystem.proto"],
        "acyclic-filesystem-v2.bin",
    )?;
    let output = std::path::PathBuf::from(std::env::var_os("OUT_DIR").ok_or("OUT_DIR is unset")?);
    acyclic_proto_build::write_authorization(
        &std::fs::read(output.join("acyclic-filesystem-v2.bin"))?,
        "acyclic.filesystem.v2.FilesystemService",
        "FsMethod",
        &output,
    )
}
