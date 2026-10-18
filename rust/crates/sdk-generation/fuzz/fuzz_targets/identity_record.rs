#![no_main]

#[path = "../../src/cache_record.rs"]
mod cache_record;
pub use acyclic_fuzz_support as fuzz_support;
use sha2::{Digest, Sha256};

fuzz_support::fuzz_target!(|bytes: &[u8], fuzz_input| {
    let digest = format!("sha256:{:x}", Sha256::digest(bytes));
    let encoded = cache_record::encode(&digest, &digest);
    let _ = fuzz_support::round_trip_option(&digest, cache_record::decode(&encoded, &digest));
    if let Some((key, value)) = fuzz_support::FuzzInput::optional_view(fuzz_input, |fuzz_input| {
        let bytes = fuzz_input.bytes();
        let split = bytes.iter().position(|byte| *byte == b'\n')?;
        let key = fuzz_input.slice(0..split)?.text()?;
        cache_record::decode(bytes.get(split + 1..)?, key).map(|value| (key, value))
    }) {
        let replay = cache_record::encode(key, &value);
        let _ = fuzz_support::round_trip_option(&value, cache_record::decode(&replay, key));
    }
});
