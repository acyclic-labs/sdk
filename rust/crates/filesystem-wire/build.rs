//! Generate the packaged Filesystem protocol independently of its engine.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let compiler = acyclic_proto_build::Compiler::new()?;
    compiler.compile(
        compiler.config(),
        acyclic_proto_build::target_services()?,
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
