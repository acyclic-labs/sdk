use acyclic_fs::workspace::check_workspace_name;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn canonical_names_round_trip_and_denied_effects_never_execute(
        text in prop::collection::vec(any::<char>(), 0..300).prop_map(|c| c.into_iter().collect::<String>()),
    ) { check_workspace_name(text.as_bytes()); }

    #[test]
    fn byte_inputs_keep_exact_errors_and_actual_work_limits(bytes in prop::collection::vec(any::<u8>(), 0..1024)) {
        check_workspace_name(&bytes);
    }
}

#[test]
fn seed_corpus_covers_unicode_ordering_truncation_and_name_boundaries() {
    for seed in [
        include_bytes!("../../fuzz/corpus/workspace_name/ascii").as_slice(),
        include_bytes!("../../fuzz/corpus/workspace_name/decomposed"),
        include_bytes!("../../fuzz/corpus/workspace_name/hangul"),
        include_bytes!("../../fuzz/corpus/workspace_name/combining"),
        include_bytes!("../../fuzz/corpus/workspace_name/invalid"),
        include_bytes!("../../fuzz/corpus/workspace_name/long"),
        include_bytes!("../../fuzz/corpus/workspace_name/maximum"),
        include_bytes!("../../fuzz/corpus/workspace_name/truncated"),
        b"",
        b".",
        b"..",
        b"bad\0name",
        b"bad\xff",
    ] {
        check_workspace_name(seed);
    }
}
