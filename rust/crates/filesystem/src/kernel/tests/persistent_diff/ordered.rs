use super::*;
use proptest::prelude::*;

fn insert_pages<const RESET_CURSOR: bool>(keys: &[u8]) -> proptest::test_runner::TestCaseResult {
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let mut values = Changes::<FileTableFormat>::allocate(0, &mut accounting).unwrap();
    let mut cursor = None;
    for before in [true, false] {
        if RESET_CURSOR {
            cursor = None;
        }
        for &key in keys {
            let mut record = file_record(key).unwrap();
            record.link_count = if before { 1 } else { 2 };
            let guard = accounting.claim_owned(0, 0).unwrap();
            insert_value::<FileTableFormat>(
                &mut values,
                Owned::new(record, guard, 0),
                before,
                keys.len() * 2,
                DecodeLimits::default(),
                &mut accounting,
                &mut cursor,
            )
            .unwrap();
        }
        if before {
            prop_assert_eq!(accounting.spent().items_examined, 0);
        }
    }
    prop_assert_eq!(values.value.len(), keys.len());
    for (&key, change) in keys.iter().zip(&values.value) {
        prop_assert_eq!(change.key, FileId::from_bytes([key; 16]));
        prop_assert_eq!(
            change.before.as_ref().map(|value| value.link_count),
            Some(1)
        );
        prop_assert_eq!(change.after.as_ref().map(|value| value.link_count), Some(2));
    }
    // For 64 keys the first lookup takes seven binary probes, then every
    // adjacent key needs exactly one comparison. Appending above needed none.
    if keys.len() == 64 {
        prop_assert_eq!(accounting.spent().items_examined, 70);
    }
    values.retire(&mut accounting).unwrap();
    Ok(())
}

proptest! {
    #[test]
    fn ordered_pages_preserve_every_before_and_after_value(
        keys in prop::collection::btree_set(1_u8..=255, 0..65),
    ) {
        insert_pages::<true>(&keys.into_iter().collect::<Vec<_>>())?;
    }
}

#[test]
fn ordered_page_model_detects_a_missing_cursor_reset() {
    insert_pages::<true>(&[1, 2, 3]).unwrap();
    assert!(insert_pages::<false>(&[1, 2, 3]).is_err());
}

#[test]
fn appends_and_adjacent_matches_reach_their_probe_lower_bounds() {
    insert_pages::<true>(&(1..=64).collect::<Vec<_>>()).unwrap();
}
