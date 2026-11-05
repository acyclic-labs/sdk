//! Emits the current agent-record fuzz corpus through production encoders.
#[path = "../tests/agent_model/corpus.rs"]
mod corpus;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    let seeds = match arguments.as_slice() {
        [] => corpus::seeds()?,
        #[cfg(feature = "test-support")]
        [mode] if mode == "--codecs" => {
            futures::executor::block_on(acyclic_harness_core::agent::codec_fuzz::seeds())?
                .into_iter()
                .collect()
        }
        _ => return Err("step=select agent corpus; recovery=cargo run --locked -p acyclic-harness-core --features test-support --example agent-record-corpus -- --codecs".into()),
    };
    println!("{}", serde_json::to_string(&seeds)?);
    Ok(())
}
