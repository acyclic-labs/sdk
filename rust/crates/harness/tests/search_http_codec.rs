//! Ordinary/native HTTP codec properties and shared fuzz corpus settlement checks.
#![cfg(not(target_arch = "wasm32"))]
#[path = "search_http_codec/support.rs"]
mod support;
use proptest::prelude::*;

fn json_leaf() -> impl Strategy<Value = serde_json::Value> {
    prop_oneof![
        Just(serde_json::Value::Null),
        any::<bool>().prop_map(serde_json::Value::Bool),
        any::<i64>().prop_map(|n| serde_json::json!(n)),
        ".{0,128}".prop_map(serde_json::Value::String),
    ]
    .prop_recursive(4, 64, 8, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..8).prop_map(serde_json::Value::Array),
            proptest::collection::btree_map(".{0,16}", inner, 0..8)
                .prop_map(|map| serde_json::Value::Object(map.into_iter().collect())),
        ]
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Both decoding paths agree with the contract; funded native calls settle.
    #[test]
    fn complete_search_responses_always_settle_and_replay(
        depth in prop_oneof![2 => 0usize..16, 1 => 0usize..4097, 1 => Just(255), 1 => Just(256), 1 => Just(257), 1 => Just(258), 1 => Just(4096)],
        size in prop_oneof![8 => 0usize..8192, 1 => Just(1_048_576), 1 => Just(1_048_577)],
        shape in any::<u8>(),
        leaf in json_leaf(),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        for exa in [true, false] {
            runtime.block_on(support::check(support::response(depth, size, shape, &leaf), exa));
        }
    }
}

#[test]
fn exa_codec_seed_corpus_settles() {
    corpus(true);
}

#[test]
fn tinyfish_codec_seed_corpus_settles() {
    corpus(false);
}

#[tokio::test]
async fn incomplete_and_over_depth_responses_publish_rejected() {
    for exa in [true, false] {
        for bytes in [
            br#"{"results":[]"#.to_vec(),
            support::response(4096, 0, 0, &serde_json::Value::Null),
        ] {
            support::check(bytes, exa).await;
        }
    }
}

fn corpus(exa: bool) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for corpus in ["search_http_codec", "search_http_envelope"] {
        let corpus = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../harness-fuzz/corpus")
            .join(corpus);
        for entry in std::fs::read_dir(corpus).unwrap() {
            let bytes = std::fs::read(entry.unwrap().path()).unwrap();
            runtime.block_on(support::check(support::fuzz_response(&bytes), exa));
        }
    }
    runtime.block_on(support::check(
        support::response(0, 1_047_552, 1, &serde_json::Value::String(String::new())),
        exa,
    ));
    for length in [1_048_575, 1_048_576, 1_048_577] {
        let mut bytes = support::response(0, 0, 0, &serde_json::Value::Null);
        bytes.resize(length, b' ');
        runtime.block_on(support::check(bytes, exa));
    }
}
