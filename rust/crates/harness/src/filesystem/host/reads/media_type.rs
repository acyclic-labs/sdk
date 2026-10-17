//! Media for an unstaged workspace file, from its already bounded, pinned bytes.
pub(super) fn classify(bytes: &[u8]) -> &'static str {
    if !bytes.contains(&0) && std::str::from_utf8(bytes).is_ok() {
        "text/plain"
    } else {
        "application/octet-stream"
    }
}
