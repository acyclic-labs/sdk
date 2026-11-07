//! Check persisted identity authentication against the original JSON.
#![no_main]
use sdk_generation::cache_record;
use sha2::{Digest, Sha256};
#[path = "../identity_record.rs"]
mod identity_record;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    let digest = format!("sha256:{:x}", Sha256::digest(bytes));
    let encoded = cache_record::encode(&digest, &digest);
    assert_eq!(identity_record::check(&encoded, &digest), Some(digest));
    let mut fields = bytes.splitn(2, |byte| *byte == b'\n');
    if let (Some(key), Some(record)) = (fields.next(), fields.next()) {
        if let Ok(key) = std::str::from_utf8(key) {
            identity_record::check(record, key);
        }
    }
});
