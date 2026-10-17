#![no_main]
#[path = "../../src/cache_record.rs"]
mod cache_record;
use sha2::{Digest, Sha256};

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    let digest = format!("sha256:{:x}", Sha256::digest(bytes));
    let encoded = cache_record::encode(&digest, &digest);
    assert_eq!(cache_record::decode(&encoded, &digest), Some(digest));
    if let Some(split) = bytes.iter().position(|byte| *byte == b'\n') {
        if let Ok(key) = std::str::from_utf8(&bytes[..split]) {
            if let Some(value) = cache_record::decode(&bytes[split + 1..], key) {
                let replay = cache_record::encode(key, &value);
                assert_eq!(cache_record::decode(&replay, key), Some(value));
            }
        }
    }
});
