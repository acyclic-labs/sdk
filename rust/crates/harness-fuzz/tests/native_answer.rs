#![cfg(feature = "native-answer")]

#[path = "../native_answer_oracle.rs"]
mod oracle;

#[test]
fn recorded_and_derived_native_events_satisfy_the_same_fuzz_oracle() {
    oracle::check_prefix_work();
    let corpus = acyclic_test_support::crate_path!("corpus/native_answer");
    let mut inputs: Vec<_> = std::fs::read_dir(corpus)
        .expect("native event corpus")
        .map(|entry| entry.expect("corpus entry").path())
        .collect();
    inputs.sort();
    assert!(!inputs.is_empty(), "native event corpus missing");
    for input in inputs {
        eprintln!("native event input: {}", input.display());
        assert!(
            oracle::check(&std::fs::read(input).expect("native event bytes")) > 0,
            "recorded/derived event input was never admitted"
        );
    }
}
