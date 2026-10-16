#![cfg(not(target_arch = "wasm32"))]

#[path = "support/journal_input.rs"]
mod journal;
#[path = "support/wire_input.rs"]
mod wire;
use proptest::prelude::*;

#[test]
fn decoder_seed_corpus() {
    for data in [
        include_bytes!("../../harness-fuzz/corpus/wire_frames/acknowledge").as_slice(),
        include_bytes!("../../harness-fuzz/corpus/wire_frames/admission"),
        include_bytes!("../../harness-fuzz/corpus/journal_decode/truncated-protobuf"),
        include_bytes!("../../harness-fuzz/corpus/wire_frames/invalid-json"),
        include_bytes!("../../harness-fuzz/corpus/journal_decode/oversized-journal-header"),
        include_bytes!("../../harness-fuzz/corpus/wire_frames/utf8"),
        include_bytes!("../../harness-fuzz/corpus/journal_decode/empty-request"),
    ] {
        wire::check(data);
        journal::check(data);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn wire_and_journal_decode_arbitrary_bytes(data in prop::collection::vec(any::<u8>(), 0..4096)) {
        wire::check(&data);
        journal::check(&data);
    }
}

#[test]
#[should_panic(expected = "client protobuf roundtrip changed the generation")]
fn negative_control_detects_dropped_generation() {
    wire::check_with_dropped_generation(b"generation", true);
}

#[test]
#[should_panic(expected = "journal lost a complete committed record")]
fn negative_control_detects_lost_committed_record() {
    journal::check_with_faults(b"record", true, false);
}

#[test]
#[should_panic(expected = "journal retained an incomplete trailing record")]
fn negative_control_detects_unrepaired_tail() {
    journal::check_with_faults(b"record", false, true);
}
