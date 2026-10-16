#[path = "path_input.rs"]
mod input;
use proptest::prelude::*;

#[test]
fn path_seed_corpus() {
    for bytes in [
        include_bytes!("../fuzz/corpus/paths/unicode").as_slice(),
        include_bytes!("../fuzz/corpus/paths/parent"),
        include_bytes!("../fuzz/corpus/paths/repeated-separator"),
        include_bytes!("../fuzz/corpus/paths/embedded-nul"),
        include_bytes!("../fuzz/corpus/paths/posix-bytes"),
        include_bytes!("../fuzz/corpus/paths/utf16-surrogate"),
        include_bytes!("../fuzz/corpus/paths/fold-expansions"),
    ] {
        input::check(bytes);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn canonical_paths_and_fold_keys_preserve_namespace_boundaries(
        data in prop_oneof![
            prop::collection::vec(any::<u8>(), 0..2048),
            prop::collection::vec(any::<char>(), 0..48)
                .prop_map(|characters| characters.into_iter().collect::<String>().into_bytes()),
        ]
    ) {
        input::check(&data);
    }
}
