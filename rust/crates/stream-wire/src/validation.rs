//! Canonical input grammar shared by the Stream domain and HTTP codec.

use crate::wire::StreamLimit;

/// Whether this is a canonical nonempty account-relative Stream path.
#[must_use]
pub fn is_path(path: &str) -> bool {
    if path.is_empty()
        || path.len() > StreamLimit::MaxPathBytes as usize
        || !path.is_ascii()
        || path.starts_with('/')
        || path.ends_with('/')
    {
        return false;
    }
    for segment in path.split('/') {
        if segment.is_empty()
            || segment == "."
            || segment == ".."
            || segment
                .bytes()
                .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
            || segment.as_bytes().contains(&b'\\')
        {
            return false;
        }
    }
    true
}

/// Whether a caller retry identity has its permitted wire width.
#[must_use]
pub const fn is_idempotency_key(value: &[u8]) -> bool {
    !value.is_empty() && value.len() <= StreamLimit::MaxIdempotencyKeyBytes as usize
}
