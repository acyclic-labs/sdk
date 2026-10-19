use super::*;

const BUDGET: WorkBudget = crate::performance::BOUNDARY_BUDGET;

#[test]
fn authority_paths_keep_the_full_stream_bound_on_either_side_of_small_scratch()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let authority = AuthorityId::from_bytes([7; 16]);
    for generation in [None, Some(GenerationId::new(Digest::from_bytes([9; 32])))] {
        let fixed = b"fs/authorities/".len() + 32 + if generation.is_some() { 64 } else { 0 };
        for length in [fixed, 127, 128, 129, acyclic_stream::MAX_PATH_BYTES] {
            let suffix = "x".repeat(length - fixed);
            let mut work = AuthorityWork::new(BUDGET, &cancellation);
            let path = work.path(authority, &suffix, generation)?;
            assert_eq!(path.as_str().len(), length);
            assert_eq!(work.spent().bytes_encoded, length as u64);
            assert_eq!(
                work.spent().bytes_copied,
                (b"fs/authorities/".len() + suffix.len() + length) as u64
            );
            assert_eq!(work.spent().allocation_operations, 1);
        }
        let suffix = "x".repeat(acyclic_stream::MAX_PATH_BYTES + 1 - fixed);
        let mut work = AuthorityWork::new(BUDGET, &cancellation);
        assert!(matches!(
            work.path(authority, &suffix, generation),
            Err(AuthorityStoreError::RejectedStatic("fork path is too long"))
        ));
        assert_eq!(work.spent(), WorkCounters::UNCHARGED);
    }
    Ok(())
}

proptest::proptest! {
    // The small/cold split preserves canonical bytes, errors and every charge
    // of the former full-sized scratch path, under arbitrary admission limits.
    #[test]
    fn small_scratch_matches_full_scratch_bytes_and_receipts(
        identity in proptest::prelude::any::<[u8; 16]>(),
        digest in proptest::option::of(proptest::prelude::any::<[u8; 32]>()),
        suffix in "[a-z]{0,256}",
        limit in 0u8..7,
    ) {
        let authority = AuthorityId::from_bytes(identity);
        let generation = digest.map(|bytes| GenerationId::new(Digest::from_bytes(bytes)));
        let expected = format!("fs/authorities/{}{}{}", hex::encode(identity), suffix,
            digest.map_or_else(String::new, hex::encode));
        let length = expected.len();
        let writes = codec_work(length as u64, (b"fs/authorities/".len() + suffix.len()) as u64, 0);
        let mut budget = BUDGET;
        match limit {
            1 => budget.bytes_encoded = length as u64 - 1,
            2 => budget.bytes_copied = (b"fs/authorities/".len() + suffix.len()) as u64 - 1,
            3 => budget.allocation_operations = 0,
            4 => budget.peak_allocation_bytes = 0,
            5 => budget.bytes_copied = (b"fs/authorities/".len() + suffix.len() + length) as u64 - 1,
            6 => {
                budget.peak_allocation_bytes = length as u64;
                budget.bytes_encoded = length as u64;
                budget.bytes_copied = (b"fs/authorities/".len() + suffix.len() + length) as u64;
            }
            _ => {}
        }
        let cancellation = CancellationToken::new();
        let mut old = AuthorityWork::new(budget, &cancellation);
        let old_result = old.admit(writes).and_then(|()| old.path_buffer::<{ acyclic_stream::MAX_PATH_BYTES }>(
            b"fs/authorities/", authority, &suffix, digest.as_ref().map(|bytes| bytes.as_slice()), length, writes));
        let mut work = AuthorityWork::new(budget, &cancellation);
        let actual = work.path(authority, &suffix, generation);
        match (actual, old_result) {
            (Ok(actual), Ok(old)) => {
                proptest::prop_assert_eq!(actual.as_str(), old.as_str());
                proptest::prop_assert_eq!(actual.as_str(), expected.as_str());
            }
            (Err(actual), Err(old)) => proptest::prop_assert_eq!(actual.to_string(), old.to_string()),
            (actual, old) => proptest::prop_assert!(false, "actual={actual:?}, old={old:?}"),
        }
        proptest::prop_assert_eq!(work.spent(), old.spent());
    }
}
