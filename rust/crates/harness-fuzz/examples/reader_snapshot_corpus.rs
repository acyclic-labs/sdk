use acyclic_harness_fuzz::reader_snapshot;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus/reader_snapshot");
    std::fs::create_dir_all(&root)?;
    let mut sources = String::from("file\tblake3\tproducer\n");
    for (name, bytes) in reader_snapshot::seeds() {
        reader_snapshot::check(&bytes)?;
        sources.push_str(&format!(
            "{name}\t{}\tRecordCache::snapshot via public cache admission\n",
            blake3::hash(&bytes)
        ));
        std::fs::write(root.join(name), bytes)?;
    }
    std::fs::write(root.with_file_name("reader_snapshot-SOURCES.tsv"), sources)?;
    Ok(())
}
