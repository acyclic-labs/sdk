pub(super) fn encode(sequence: u64, key: &[u8; 32]) -> [u8; 40] {
    let mut bytes = [0; 40];
    bytes[..8].copy_from_slice(&sequence.to_le_bytes());
    bytes[8..].copy_from_slice(key);
    bytes
}

pub(super) fn decode(bytes: &[u8]) -> Option<(u64, &[u8; 32])> {
    if bytes.len() != 40 {
        return None;
    }
    Some((
        u64::from_le_bytes(bytes[..8].try_into().ok()?),
        bytes[8..].try_into().ok()?,
    ))
}
