//! Exercise the production recognizer, including partial conflict blocks.
#![no_main]
#[path = "../../src/git_compat/merge_markers.rs"]
mod markers;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    let text = String::from_utf8_lossy(bytes);
    assert_eq!(
        markers::contains(&text),
        markers::contains(&text.replace("\r\n", "\n"))
    );
});
