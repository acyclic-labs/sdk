//! UTF-8 round-trip oracle for classification before the immutable file is pinned.
#[path = "../../harness/src/filesystem/host/reads/media_type.rs"]
mod media_type;
use acyclic_harness::conversation::FileDescriptor;

pub fn classify(bytes: &[u8]) -> &'static str {
    media_type::classify(bytes)
}

pub fn matches(bytes: &[u8], actual: &str) -> bool {
    // An independently owned text round-trip defines text, including empty files.
    // NUL is binary even when the remaining bytes are valid Unicode.
    let text = String::from_utf8(bytes.to_vec())
        .ok()
        .filter(|text| !text.chars().any(|c| c == '\0'));
    let expected = if text.is_some() {
        "text/plain"
    } else {
        "application/octet-stream"
    };
    if actual != expected {
        return false;
    }
    let descriptor = FileDescriptor::from_bytes(bytes, actual, &mut None).unwrap();
    let encoded = serde_json::to_vec(&descriptor).unwrap();
    let restored: FileDescriptor = serde_json::from_slice(&encoded).unwrap();
    restored == descriptor
        && restored.verify(bytes, &mut None).is_ok()
        && text.is_none_or(|text| text.into_bytes() == bytes)
}

pub fn exercise(bytes: &[u8]) {
    assert!(
        matches(bytes, classify(bytes)),
        "workspace media must preserve verified UTF-8 and pinned descriptors"
    );
}
