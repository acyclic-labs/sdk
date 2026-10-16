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
