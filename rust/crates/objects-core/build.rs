//! Generate the public objects contract from the crate-owned protocol.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let compiler = acyclic_proto_build::Compiler::new()?;
    // Bodies share the caller's buffer end to end instead of copying it.
    let mut config = compiler.config();
    config.bytes([
        ".acyclic.objects.v1.PutBlobRequest.body",
        ".acyclic.objects.v1.GetBlobResponse.body",
    ]);
    compiler.compile(
        config,
        acyclic_proto_build::target_services()?,
        &["proto/objects/v1/objects.proto"],
        "acyclic-objects-v1.bin",
    )?;
    let output = std::path::PathBuf::from(std::env::var_os("OUT_DIR").ok_or("OUT_DIR is unset")?);
    acyclic_proto_build::write_authorization(
        &std::fs::read(output.join("acyclic-objects-v1.bin"))?,
        "acyclic.objects.v1.BlobsService",
        "ObjectsMethod",
        &output,
    )
}
