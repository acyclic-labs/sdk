#![cfg(not(target_arch = "wasm32"))]

#[path = "support/decoder_input.rs"]
mod input;
use proptest::prelude::*;

#[test]
fn decoder_seed_corpus() {
    for data in [
        include_bytes!("../fuzz/corpus/decoders/acknowledge").as_slice(),
        include_bytes!("../fuzz/corpus/decoders/admission"),
        include_bytes!("../fuzz/corpus/decoders/truncated-protobuf"),
        include_bytes!("../fuzz/corpus/decoders/invalid-json"),
        include_bytes!("../fuzz/corpus/decoders/oversized-journal-header"),
        include_bytes!("../fuzz/corpus/decoders/utf8"),
    ] {
        input::wire(data);
        input::journal(data);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn wire_and_journal_decode_arbitrary_bytes(data in prop::collection::vec(any::<u8>(), 0..4096)) {
        input::wire(&data);
        input::journal(&data);
    }
}
