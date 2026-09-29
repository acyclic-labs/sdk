//! Emits the Rust-owned Workers HTTP route table for TypeScript generation.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", serde_json::to_string(acyclic_workers::HTTP_ROUTES)?);
    Ok(())
}
