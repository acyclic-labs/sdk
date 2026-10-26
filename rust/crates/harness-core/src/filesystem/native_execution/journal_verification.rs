use acyclic_fs::MaterializationJournal;

/// Checks the production journal admission, canonical bytes and completion bound.
/// The host supplies its allocator's peak-byte measurement around the decoder.
///
/// # Panics
/// Panics when decoding exceeds its reservation or a wire invariant is broken.
#[must_use]
pub fn verify_native_preparation_journal(
    input: &[u8],
    measure: impl FnOnce(&mut dyn FnMut()) -> u64,
) -> bool {
    let bound = crate::executor::json::layout_reservation::<MaterializationJournal>(input);
    let mut decoded = None;
    let measured = measure(&mut || {
        decoded = Some(serde_json::from_slice::<MaterializationJournal>(input));
    });
    let decoded = decoded.unwrap();
    // Layout admission refuses every document serde would reject for its
    // shape; whatever it admits decodes within the reservation.
    match bound {
        Some(bound) => assert!(measured <= bound, "{measured} > {bound}"),
        None => assert!(decoded.is_err(), "admission refused a decodable journal"),
    }
    let accepted = decoded.is_ok();
    if let Ok(mut journal) = decoded {
        let compact = serde_json::to_vec(&journal).unwrap();
        let reread: MaterializationJournal = serde_json::from_slice(&compact).unwrap();
        assert_eq!(journal, reread);
        let mut value = serde_json::to_value(&journal).unwrap();
        value.sort_all_objects();
        let legacy = serde_json::to_vec(&value).unwrap();
        assert_eq!(
            serde_json::to_vec(&super::journal_encoding::Canonical(&journal)).unwrap(),
            legacy
        );
        let reread: MaterializationJournal = serde_json::from_slice(&legacy).unwrap();
        assert_eq!(journal, reread);
        let capacity = super::journal_encoding::completion_capacity(&journal, legacy.len());
        if let Some(revision) = journal.revision.checked_add(1) {
            journal.revision = revision;
            journal.work = acyclic_fs::WorkCounters::UNBOUNDED;
            let completed =
                serde_json::to_vec(&super::journal_encoding::Canonical(&journal)).unwrap();
            assert_eq!(capacity, Some(completed.len()));
        } else {
            assert!(capacity.is_none());
        }
    }
    accepted
}

#[cfg(test)]
pub(crate) fn verify_decode(input: &[u8]) -> bool {
    verify_native_preparation_journal(input, |decode| {
        acyclic_test_support::measure(decode).bytes_max as u64
    })
}
