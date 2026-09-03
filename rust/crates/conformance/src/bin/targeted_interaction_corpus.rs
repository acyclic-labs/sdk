//! Emits every deterministic mandatory targeted three-way interaction case.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "{}",
        acyclic_conformance::filesystem::targeted_interaction_corpus_json()?
    );
    Ok(())
}
