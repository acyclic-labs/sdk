//! Emits the deterministic duplicate-free pairwise workload corpus.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "{}",
        acyclic_conformance::filesystem::workload_corpus_json()?
    );
    Ok(())
}
