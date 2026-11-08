use super::cache_record;
use sha2::{Digest, Sha256};

pub fn check(bytes: &[u8], key: &str) -> Option<String> {
    let value = cache_record::decode(bytes, key)?;
    for hash in [key, value.as_str()] {
        assert_eq!(hash.len(), 71, "accepted a malformed SHA-256 identity");
        assert!(hash.starts_with("sha256:"));
        assert!(hash[7..]
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f')));
    }
    // Compare the admitted bytes, before the writer can repair authentication.
    // JSON ordering and whitespace are not part of the persisted contract.
    let original: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    let mut checksum = Sha256::new();
    checksum.update(key.as_bytes());
    checksum.update([0]);
    checksum.update(value.as_bytes());
    let canonical = serde_json::json!({
        "key": key,
        "digest": value,
        "checksum": format!("sha256:{:x}", checksum.finalize()),
    });
    assert_eq!(
        original, canonical,
        "accepted an unauthenticated cache record"
    );
    Some(value)
}
