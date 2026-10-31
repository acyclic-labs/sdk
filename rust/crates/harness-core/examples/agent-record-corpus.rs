//! Emits the current agent-record fuzz corpus through production encoders.
#[path = "../tests/agent_model/corpus.rs"]
mod corpus;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", serde_json::to_string(&corpus::seeds()?)?);
    Ok(())
}
