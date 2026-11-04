//! Capture history descriptor syntax through the shipping typed JSON writer.
use acyclic_harness_core::agent::{HistoryCheckpoint, codec_fuzz};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let seeds = codec_fuzz::history_seeds()?;
    for (_, bytes) in &seeds {
        // Positive admission defeats a decoder that simply refuses everything.
        HistoryCheckpoint::decode(bytes)?;
        codec_fuzz::check(bytes);
        for end in 0..bytes.len() {
            codec_fuzz::check(&bytes[..end]);
        }
        for index in 0..bytes.len() {
            let mut changed = bytes.clone();
            changed[index] ^= 0x80;
            codec_fuzz::check(&changed);
        }
    }
    println!("{}", serde_json::to_string(&seeds)?);
    Ok(())
}
