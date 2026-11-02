/// Formats an identity with lowercase hexadecimal bytes and the supplied framing.
pub fn format_digest(prefixes: &[&str], digest: &[u8], suffix: &str) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(
        prefixes.iter().map(|part| part.len()).sum::<usize>() + digest.len() * 2 + suffix.len(),
    );
    for prefix in prefixes {
        result.push_str(prefix);
    }
    for &byte in digest {
        result.push(DIGITS[(byte >> 4) as usize] as char);
        result.push(DIGITS[(byte & 0xf) as usize] as char);
    }
    result.push_str(suffix);
    result
}

/// Returns the unprefixed lowercase hexadecimal SHA-256 identity of exact bytes.
pub fn sha256_hex(bytes: &[u8]) -> String {
    format_digest(
        &[],
        ring::digest::digest(&ring::digest::SHA256, bytes).as_ref(),
        "",
    )
}
