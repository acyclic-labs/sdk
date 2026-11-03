//! Independent complete-line grammar for the streaming conflict recognizer.
pub(crate) fn contains(bytes: &[u8]) -> bool {
    bytes.split(|&byte| byte == b'\n').any(|line| {
        let end = line
            .iter()
            .rposition(|&byte| byte != b'\r')
            .map_or(0, |last| last + 1);
        &line[..end] == b"======="
            || [b"<<<<<<<", b"|||||||", b">>>>>>>"].iter().any(|marker| {
                line.starts_with(*marker) && matches!(line.get(marker.len()), Some(b' ' | b'\t'))
            })
    })
}
