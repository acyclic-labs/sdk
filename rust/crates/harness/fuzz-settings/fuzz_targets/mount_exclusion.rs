//! Compare production mount exclusion admission with framed byte matching.
#![no_main]
#[path = "../../src/local/workspace/volumes/exclude/pattern.rs"]
mod pattern;
use libfuzzer_sys::fuzz_target;

/// Text inputs keep their spelling; others become arbitrary native units:
/// bytes on Unix, UTF-16 units on Windows (unpaired surrogates included).
fn native_path(input: &[u8]) -> std::ffi::OsString {
    if let Ok(text) = std::str::from_utf8(input) {
        return text.into();
    }
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt as _;
        std::ffi::OsString::from_vec(input.to_vec())
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStringExt as _;
        let units: Vec<_> = input
            .chunks(2)
            .map(|pair| u16::from_le_bytes([pair[0], *pair.get(1).unwrap_or(&0)]))
            .collect();
        std::ffi::OsString::from_wide(&units)
    }
}
fuzz_target!(|input: &[u8]| {
    // Exercise the same opaque-byte line matcher used for existing excludes.
    // The independent oracle searches framed bytes instead of splitting lines.
    let mut framed = Vec::with_capacity(input.len() + 2);
    framed.push(b'\n');
    framed.extend_from_slice(input);
    framed.push(b'\n');
    let rule = "/.volumes/scratch";
    let expected = [
        b"\n/.volumes/scratch\n".as_slice(),
        b"\n/.volumes/scratch\r\n".as_slice(),
    ]
    .iter()
    .any(|needle| framed.windows(needle.len()).any(|bytes| bytes == *needle));
    assert_eq!(pattern::contains(input, rule), expected);
    let path = native_path(input);
    let path = std::path::Path::new(&path);
    if let Ok(encoded) = pattern::pattern(path) {
        assert!(path.to_str().is_some());
        assert!(encoded.starts_with('/'));
        assert!(!encoded.contains(['\r', '\n', '\0']));
        assert!(!encoded.ends_with('/'));
    }
});
