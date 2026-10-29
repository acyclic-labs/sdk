//! Reproducible streaming-prefix and corpus invariants.
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
fn control_before_unicode_punctuation_keeps_literal_delimiters() {
    let bytes = include_bytes!("../fuzz/corpus/streaming/control-delimiter-context");
    input::check(bytes);
    let mut variant = *bytes;
    let control = bytes.iter().position(|&byte| byte == 0x0e).unwrap();
    for before in 0..=127 {
        *variant.get_mut(control).unwrap() = before;
        input::check(&variant);
    }
    for count in 1..=8 {
        let mut escaped = bytes.get(..control).unwrap().to_vec();
        escaped.extend(std::iter::repeat_n(b'\\', count));
        escaped.extend_from_slice(bytes.get(control + 1..).unwrap());
        input::check(&escaped);
    }
}

#[test]
#[should_panic(expected = "changed Markdown rows were not replaced")]
fn negative_control_detects_append_only_rows() {
    input::check_without_row_replacement(b"|a", true);
}

proptest! {
    #![proptest_config(ProptestConfig { rng_seed: proptest::test_runner::RngSeed::Fixed(0x6d61_726b_0000_0006), ..ProptestConfig::with_cases(64) })]
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
