#![no_main]
#[path = "../../src/local/workspace/volumes/exclude/pattern.rs"]
mod pattern;
use libfuzzer_sys::fuzz_target;
use std::os::unix::ffi::OsStrExt;
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
    let path = std::path::Path::new(std::ffi::OsStr::from_bytes(input));
    if let Ok(encoded) = pattern::pattern(path) {
        assert!(path.to_str().is_some());
        assert!(encoded.starts_with('/'));
        assert!(!encoded.contains(['\r', '\n', '\0']));
        assert!(!encoded.ends_with('/'));
    }
});
