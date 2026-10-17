#![no_main]

mod content_matcher_input;
#[path = "../../harness/tests/e2e/matchers.rs"]
mod matchers;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    content_matcher_input::check(bytes);
    // Observation arrays retain ordered subset semantics outside content.
    if let Ok(values) = serde_json::from_slice::<Vec<serde_json::Value>>(bytes) {
        let _ = matchers::ordered(&values, &values, "fuzz observations");
    }
});
