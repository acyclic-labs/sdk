//! Admission for fixed, versioned binary format headers.

/// Borrow a payload only after its domain and version have been admitted.
///
/// Fixed headers require constant work, no allocation and no payload copy.
/// Errors are constructed only on refusal, before any payload interpretation.
/// Callers provide their boundary's typed errors rather than translating text.
///
/// Another domain, or input that ends inside the expected header, is
/// `invalid`: truncation is malformed input, not a version choice. A complete
/// header naming another version is `unsupported`.
pub fn header<'a, E>(
    encoded: &'a [u8],
    domain: &[u8],
    version: &[u8],
    invalid: impl FnOnce() -> E,
    unsupported: impl FnOnce() -> E,
) -> Result<&'a [u8], E> {
    match encoded.strip_prefix(domain) {
        Some(rest) => match rest.strip_prefix(version) {
            Some(payload) => Ok(payload),
            None if rest.len() < version.len() => Err(invalid()),
            None => Err(unsupported()),
        },
        None => Err(invalid()),
    }
}
