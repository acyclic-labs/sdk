//! Admission for fixed, versioned binary format headers.

/// Borrow a payload only after its domain and version have been admitted.
///
/// Fixed headers require constant work, no allocation and no payload copy.
/// Errors are constructed only on refusal, before any payload interpretation.
/// Callers provide their boundary's typed errors rather than translating text.
pub fn header<'a, E>(
    encoded: &'a [u8],
    domain: &[u8],
    version: &[u8],
    invalid: impl FnOnce() -> E,
    unsupported: impl FnOnce() -> E,
) -> Result<&'a [u8], E> {
    encoded
        .strip_prefix(domain)
        .ok_or_else(invalid)?
        .strip_prefix(version)
        .ok_or_else(unsupported)
}
