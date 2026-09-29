//! Emits the Rust-owned Actors HTTP route table for TypeScript generation.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", serde_json::to_string(acyclic_actors::HTTP_ROUTES)?);
    Ok(())
}
