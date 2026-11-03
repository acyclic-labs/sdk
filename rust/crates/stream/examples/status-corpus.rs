//! Capture the original SDK status writer's HTTP trailer fields for fuzzing.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use sha2::{Digest as _, Sha256};
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("fuzz/corpus/status");
    std::fs::create_dir_all(&root)?;
    let mut provenance = String::from("file\tsha256\tproducer\n");
    for (name, bytes) in acyclic_stream::codec_fuzz::status::seeds()? {
        provenance.push_str(&format!(
            "{name}\t{:x}\twire_codec::error_status -> tonic::Status::add_header -> grpc-message\n",
            Sha256::digest(&bytes)
        ));
        std::fs::write(root.join(name), &bytes)?;
    }
    std::fs::write(
        root.parent()
            .ok_or("missing corpus parent")?
            .join("status-SOURCES.tsv"),
        provenance,
    )?;
    Ok(())
}
