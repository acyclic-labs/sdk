//! Emits the stable machine-readable workload and access-plan manifest.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", acyclic_conformance::filesystem::manifest_json()?);
    Ok(())
}
