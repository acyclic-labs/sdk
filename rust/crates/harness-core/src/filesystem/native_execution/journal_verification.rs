use acyclic_fs::MaterializationJournal;

pub(crate) fn verify_decode_with(
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
        Some(bound) => assert!(measured <= bound, "{measured:?} > {bound}"),
        None => assert!(decoded.is_err(), "admission refused a decodable journal"),
    }
    let accepted = decoded.is_ok();
    if let Ok(journal) = decoded {
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
    }
    accepted
}

#[cfg(test)]
pub(crate) fn verify_decode(input: &[u8]) -> bool {
    verify_decode_with(input, |run| acyclic_test_support::measure(run).bytes_max)
}
