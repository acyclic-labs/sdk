//! Replay the retained production claim corpus through the shared fuzz oracle.
#[path = "../../../fuzz/native_claim_record.rs"]
mod native_claim_record;

#[test]
fn retained_claim_corpus_satisfies_the_shared_oracle() {
    for bytes in [
        include_bytes!("../../../fuzz/corpus/native_claim_record/empty-record").as_slice(),
        include_bytes!("../../../fuzz/corpus/native_claim_record/native-bytes").as_slice(),
        include_bytes!("../../../fuzz/corpus/native_claim_record/at-bound").as_slice(),
        include_bytes!("../../../fuzz/corpus/native_claim_record/past-bound").as_slice(),
    ] {
        native_claim_record::check(bytes);
    }
}
