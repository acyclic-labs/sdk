#[path = "support/streaming_input.rs"]
mod input;
use proptest::prelude::*;

#[test]
fn streaming_seed_corpus() {
    let corpus = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("fuzz/corpus/streaming");
    for entry in std::fs::read_dir(corpus).unwrap() {
        input::check(&std::fs::read(entry.unwrap().path()).unwrap());
    }
}

#[test]
#[should_panic(expected = "changed Markdown rows were not replaced")]
fn negative_control_detects_append_only_rows() {
    input::check_without_row_replacement(b"|a", true);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn streaming_and_normalized_replay_preserve_every_prefix(
        data in prop_oneof![
            prop::collection::vec(any::<u8>(), 0..512),
            prop::collection::vec(any::<char>(), 0..96)
                .prop_map(|chars| chars.into_iter().collect::<String>().into_bytes()),
        ],
    ) {
        input::check(&data);
    }
}
