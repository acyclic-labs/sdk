use super::merge_markers as markers;

fn contains(text: &str) -> bool {
    let mut scanner = markers::Scanner::new();
    scanner.push(text.as_bytes()) || scanner.finish()
}

/// Checks streaming merge markers across arbitrary chunk boundaries.
pub fn check(bytes: &[u8]) {
    let text = String::from_utf8_lossy(bytes);
    let mut scanner = markers::Scanner::new();
    let chunk = bytes.first().map_or(1, |byte| usize::from(*byte % 31) + 1);
    for part in bytes.chunks(chunk) {
        scanner.push(part);
    }
    assert_eq!(scanner.finish(), contains(&text));
    assert_eq!(contains(&text), contains(&text.replace("\r\n", "\n")));
}
