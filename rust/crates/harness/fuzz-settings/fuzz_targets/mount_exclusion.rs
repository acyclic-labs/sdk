#![no_main]
#[path = "../../src/local/owner/exclusion/rules.rs"]
mod rules;
use libfuzzer_sys::fuzz_target;
use std::os::unix::ffi::OsStrExt;
fuzz_target!(|input: &[u8]| {
    // Exercise the same opaque-byte line matcher used for existing excludes.
    // The oracle retains whole lines, unlike the constant-space production scan.
    let rule = b"/.volumes/scratch";
    let lines = input.split(|byte| *byte == b'\n').collect::<Vec<_>>();
    let last_rule = lines
        .iter()
        .rposition(|line| line.strip_suffix(b"\r").unwrap_or(line) == rule);
    let last_negation = lines.iter().rposition(|line| line.first() == Some(&b'!'));
    let expected = last_rule.is_some() && last_rule > last_negation;
    let length = u64::try_from(input.len()).unwrap();
    assert_eq!(
        rules::contains(input, length, rule).unwrap(),
        (expected, input.last().is_none_or(|byte| *byte == b'\n')),
    );
    let path = std::path::Path::new(std::ffi::OsStr::from_bytes(input));
    if let Ok(encoded) = rules::pattern(path) {
        assert_eq!(encoded.is_empty(), path.components().next().is_none());
        if !encoded.is_empty() {
            assert_eq!(encoded.first(), Some(&b'/'));
            assert_ne!(encoded.last(), Some(&b'/'));
        }
        assert!(!encoded.iter().any(|byte| matches!(byte, b'\r' | b'\n' | 0)));
    }
});
