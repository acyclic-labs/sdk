//! Exercise the production recognizer, including partial conflict blocks.
#![no_main]
#[path = "../../src/git_compat/merge_markers.rs"]
mod markers;
#[path = "../git_markers_oracle.rs"]
mod oracle;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    let expected = oracle::contains(bytes);
    let selected = bytes.first().map_or(1, |byte| usize::from(*byte) + 1);
    for chunk in [1, selected, bytes.len().max(1)] {
        let mut scanner = markers::Scanner::new();
        for part in bytes.chunks(chunk) {
            let found = scanner.push(part);
            assert!(!found || expected, "marker grammar: premature recognition");
        }
        assert_eq!(
            scanner.finish(),
            expected,
            "marker grammar: final recognition"
        );
    }
});
