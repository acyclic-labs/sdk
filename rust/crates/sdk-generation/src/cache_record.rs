use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    key: String,
    digest: String,
    checksum: String,
}

fn checksum(key: &str, digest: &str) -> String {
    format!(
        "sha256:{:x}",
        Sha256::digest(format!("{key}\0{digest}").as_bytes())
    )
}

fn valid_hash(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

pub fn encode(key: &str, digest: &str) -> Vec<u8> {
    serde_json::to_vec(&Record {
        key: key.into(),
        digest: digest.into(),
        checksum: checksum(key, digest),
    })
    .unwrap()
}

pub fn decode(bytes: &[u8], key: &str) -> Option<String> {
    if bytes.len() > 512 {
        return None;
    }
    let record: Record = serde_json::from_slice(bytes).ok()?;
    (record.key == key
        && valid_hash(key)
        && valid_hash(&record.digest)
        && record.checksum == checksum(key, &record.digest))
    .then_some(record.digest)
}

/// Exercises cache-record integrity and canonical round trips with synthetic bytes.
#[cfg(feature = "test-support")]
pub fn fuzz_check(bytes: &[u8]) {
    let digest = format!("sha256:{:x}", Sha256::digest(bytes));
    let encoded = encode(&digest, &digest);
    assert_eq!(decode(&encoded, &digest), Some(digest));
    if let Some(split) = bytes.iter().position(|byte| *byte == b'\n') {
        if let Ok(key) = std::str::from_utf8(&bytes[..split]) {
            if let Some(value) = decode(&bytes[split + 1..], key) {
                let replay = encode(key, &value);
                assert_eq!(decode(&replay, key), Some(value));
            }
        }
    }
}
