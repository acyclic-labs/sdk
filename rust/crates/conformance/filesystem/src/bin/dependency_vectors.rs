//! Emits canonical cross-language dependency-evidence vectors.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "{}",
        acyclic_conformance_filesystem::dependency_vectors_json()?
    );
    Ok(())
}
