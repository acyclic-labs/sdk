//! Replay the retained production claim corpus through the shared fuzz oracle.
#[path = "claim_codec/oracle.rs"]
mod native_claim_record;

#[test]
fn retained_claim_corpus_satisfies_the_shared_oracle() {
    for bytes in [
        include_bytes!("claim_codec/corpus/empty-record").as_slice(),
        include_bytes!("claim_codec/corpus/native-bytes").as_slice(),
        include_bytes!("claim_codec/corpus/at-bound").as_slice(),
        include_bytes!("claim_codec/corpus/past-bound").as_slice(),
    ] {
        native_claim_record::check(bytes);
        native_claim_record::check_prefixes(bytes);
    }
}
