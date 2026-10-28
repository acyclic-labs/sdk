//! Emits the Rust-owned filesystem contract payloads in one process.
fn main() -> Result<(), serde_json::Error> {
    println!("{}", serde_json::to_string(&acyclic_fs::tooling_contracts::exports()?)?);
    Ok(())
}
