//! Check arbitrary admitted anchors through the shared persisted-value oracle.
#![no_main]
use acyclic_harness_tui_core::TranscriptAnchor;
use acyclic_harness_tui_core::transcript_anchor::oracle;
use libfuzzer_sys::fuzz_target;


fuzz_target!(|data: &[u8]| {
    if let Ok(anchor) = serde_json::from_slice::<TranscriptAnchor>(data) {
        assert_eq!(oracle::check(&anchor), Ok(()));
    }
});
