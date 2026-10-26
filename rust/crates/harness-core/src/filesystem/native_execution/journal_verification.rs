use acyclic_fs::MaterializationJournal;

/// Checks the production journal admission, canonical bytes and completion bound.
/// The host supplies its allocator's peak-byte measurement around the decoder.
///
/// # Errors
/// Returns encoding/round-trip errors, or an error if the measurement host does
/// not invoke the decoder.
///
/// # Panics
/// Panics when decoding exceeds its reservation or a wire invariant is broken.
pub fn verify_native_preparation_journal(
    input: &[u8],
    measure: impl FnOnce(&mut dyn FnMut()) -> u64,
) -> serde_json::Result<bool> {
    let bound = crate::executor::json::layout_reservation::<MaterializationJournal>(input);
    let mut decoded = None;
    let measured = measure(&mut || {
        decoded = Some(serde_json::from_slice::<MaterializationJournal>(input));
    });
    let decoded = decoded.ok_or_else(|| {
        <serde_json::Error as serde::de::Error>::custom("measurement host did not invoke decoder")
    })?;
    // Layout admission refuses every document serde would reject for its
    // shape; whatever it admits decodes within the reservation.
    match bound {
        Some(bound) => assert!(measured <= bound, "{measured} > {bound}"),
        None => assert!(decoded.is_err(), "admission refused a decodable journal"),
    }
    let accepted = decoded.is_ok();
    if let Ok(mut journal) = decoded {
        let compact = serde_json::to_vec(&journal)?;
        let reread: MaterializationJournal = serde_json::from_slice(&compact)?;
        assert_eq!(journal, reread);
        let mut value = serde_json::to_value(&journal)?;
        value.sort_all_objects();
        let legacy = serde_json::to_vec(&value)?;
        assert_eq!(
            serde_json::to_vec(&super::journal_encoding::Canonical(&journal))?,
            legacy
        );
        let reread: MaterializationJournal = serde_json::from_slice(&legacy)?;
        assert_eq!(journal, reread);
        let capacity = super::journal_encoding::completion_capacity(&journal, legacy.len());
        if let Some(revision) = journal.revision.checked_add(1) {
            journal.revision = revision;
            journal.work = acyclic_fs::WorkCounters::UNBOUNDED;
            let completed = serde_json::to_vec(&super::journal_encoding::Canonical(&journal))?;
            assert_eq!(capacity, Some(completed.len()));
        } else {
            assert!(capacity.is_none());
        }
    }
    Ok(accepted)
}

#[cfg(test)]
pub(crate) fn verify_decode(input: &[u8]) -> bool {
    verify_native_preparation_journal(input, |decode| {
        acyclic_test_support::measure(decode).bytes_max as u64
    })
    .expect("shared journal verification must succeed")
}

#[cfg(test)]
#[test]
fn missing_measurement_callback_is_a_verification_error() {
    let result = verify_native_preparation_journal(b"{}", |_| 0);
    assert!(result.is_err());
}
