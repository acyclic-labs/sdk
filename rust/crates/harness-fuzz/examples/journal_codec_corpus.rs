//! Capture execution journal codec bytes for the `journal_codec` target.
use acyclic_harness_core::filesystem::execution_codec_fuzz;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = acyclic_test_support::crate_path!("corpus/journal_codec");
    std::fs::create_dir_all(&root)?;
    let mut sources = String::from("file\tblake3\tproducer\n");
    let mut count = 0;
    for (name, bytes) in execution_codec_fuzz::seeds()? {
        execution_codec_fuzz::check(&bytes);
        sources.push_str(&format!(
            "{name}\t{}\tExecution journal codec writer via public codec_fuzz::seeds\n",
            blake3::hash(&bytes)
        ));
        std::fs::write(root.join(name), bytes)?;
        count += 1;
    }
    std::fs::write(root.with_file_name("journal_codec-SOURCES.tsv"), sources)?;
    println!("wrote {count} seeds to {}", root.display());
    Ok(())
}
