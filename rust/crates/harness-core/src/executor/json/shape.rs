/// Byte length and conservative allocation nodes in encoded JSON or a complete
/// sequence of encoded values. Quoted punctuation and escaped quotes are payload.
/// This is allocation planning, not JSON validation or read authority.
#[must_use]
pub fn encoded_json_shape(bytes: &[u8]) -> (u64, usize) {
    #[cfg(any(test, feature = "test-support"))]
    acyclic_differential::record(("JSON shape bytes", bytes.len() as u64));
    let mut nodes = 1;
    let mut quoted = false;
    let mut escape = false;
    for &byte in bytes {
        if escape {
            escape = false;
        } else if quoted && byte == b'\\' {
            escape = true;
        } else if byte == b'"' {
            quoted = !quoted;
        } else if !quoted && matches!(byte, b'{' | b'[' | b',' | b':') {
            nodes += 1;
        }
    }
    (bytes.len() as u64, nodes)
}
