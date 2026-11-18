//! Emits the stable machine-readable workload and access-plan manifest.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", acyclic_conformance_filesystem::manifest_json()?);
    Ok(())
}
