//! Arbitrary request bytes and shrinking admission/settlement properties.
#![cfg(not(target_arch = "wasm32"))]
#[path = "../search_request/support.rs"]
mod support;
use acyclic_harness::search::SearchRequest;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn request_round_trip_and_terminal_rejection(
        query in prop_oneof![".{0,256}", Just("東".repeat(4096)), Just("東".repeat(4097)), Just("\u{2003}".into())],
        max_results in prop_oneof![0u32..24, any::<u32>()],
    ) {
        let request = SearchRequest { query, max_results };
        let bytes = serde_json::to_vec(&request).unwrap();
        let expected = support::expected(&bytes);
        prop_assert_eq!(expected.is_some(), request.query.chars().count() <= 4096 && request.query.chars().any(|c| !c.is_whitespace()) && (1..=20).contains(&max_results));
        if let Some(expected) = expected { prop_assert_eq!(&expected, &request); }
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(support::check(&bytes));
    }

    #[test]
    fn arbitrary_request_bytes_settle(bytes in proptest::collection::vec(any::<u8>(), 0..2048)) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(support::check(&bytes));
    }
}

#[test]
fn request_seed_corpus_settles() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let corpus = acyclic_test_support::source_path("rust/crates/harness")
        .join("../harness-fuzz/corpus/search_request");
    for entry in std::fs::read_dir(corpus).unwrap() {
        runtime.block_on(support::check(
            &std::fs::read(entry.unwrap().path()).unwrap(),
        ));
    }
    // Direct admission also covers the fixed request cap, beyond the task record cap.
    for length in [262_143, 262_144, 262_145] {
        let mut bytes = br#"{"query":"boundary"}"#.to_vec();
        bytes.resize(length, b' ');
        runtime.block_on(support::check(&bytes));
    }
}
