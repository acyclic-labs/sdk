//! Ordinary replay and fuzzing share the original public owner-envelope path.
#[path = "owner_codec/oracle.rs"]
mod native_owner_record;

#[test]
fn retained_owner_corpus_satisfies_the_shared_oracle() {
    let corpus = [
        include_bytes!("owner_codec/corpus/minimal").as_slice(),
        include_bytes!("owner_codec/corpus/binary").as_slice(),
        include_bytes!("owner_codec/corpus/text").as_slice(),
    ];
    let produced = native_owner_record::seeds().unwrap();
    assert_eq!(produced.len(), corpus.len());
    for ((name, encoded), bytes) in produced.into_iter().zip(corpus) {
        // Retained Unix writer bytes are portable parser inputs. Windows DPAPI
        // ciphertext is randomized and remains bound to its original owner.
        #[cfg(unix)]
        assert_eq!(encoded, bytes, "original owner corpus changed: {name}");
        #[cfg(windows)]
        let _ = name;
        native_owner_record::check_retained(bytes);
        #[cfg(windows)]
        native_owner_record::check_retained(&encoded);
    }
}
