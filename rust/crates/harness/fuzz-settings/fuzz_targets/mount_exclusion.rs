#![no_main]
#[path = "../../src/local/workspace/volumes/exclude/pattern.rs"]
mod pattern;
use libfuzzer_sys::fuzz_target;
use std::os::unix::ffi::OsStrExt;
fuzz_target!(|input: &[u8]| {
    let path = std::path::Path::new(std::ffi::OsStr::from_bytes(input));
    if let Ok(encoded) = pattern::pattern(path) {
        assert!(path.to_str().is_some());
        assert!(encoded.starts_with('/'));
        assert!(!encoded.contains(['\r', '\n', '\0']));
        assert!(!encoded.ends_with('/'));
    }
});
