//! Media for an unstaged workspace file, from its already bounded, pinned bytes.
/// Classifies captured file bytes using the same image and document boundary as reads.
pub fn captured(bytes: &[u8]) -> &'static str {
    crate::model::image_media_type(bytes).unwrap_or_else(|| classify(bytes))
}

pub(crate) fn classify(bytes: &[u8]) -> &'static str {
    if bytes.starts_with(b"%PDF-") {
        "application/pdf"
    } else if !bytes.contains(&0) && std::str::from_utf8(bytes).is_ok() {
        "text/plain"
    } else {
        "application/octet-stream"
    }
}
