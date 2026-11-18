//! Emits the deterministic duplicate-free pairwise workload corpus.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "{}",
        acyclic_conformance_filesystem::workload_corpus_json()?
    );
    Ok(())
}
