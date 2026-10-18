use super::*;
use crate::{CancellationToken, WorkBudget};
use proptest::prelude::*;

fn colliding_ids(count: usize) -> Vec<ObjectId> {
    let hashes = hash_state();
    // Independent slot geometry: smallest power of two below 7/8 occupancy.
    let capacity = (2usize..)
        .find(|size| size.is_power_of_two() && count < *size && count <= size - size / 8)
        .unwrap();
    (0u64..)
        .map(|key| ObjectId {
            kind: crate::ObjectKind::BlobChunk,
            digest: object_digest(crate::ObjectKind::BlobChunk, &key.to_le_bytes()),
        })
        .filter(|id| hashes.hash_one(id) as usize % capacity == 0)
        .take(count)
        .collect()
}

fn budget(items_examined: u64) -> WorkBudget {
    WorkBudget {
        items_examined,
        ..WorkBudget::UNBOUNDED
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn collisions_charge_every_probe_before_access_and_reject_missing_meter(
        count in 2usize..33, extra in 0u64..33, cut in 0u64..529,
    ) {
        let ids = colliding_ids(count);
        let values: Vec<_> = (0..count).collect();
        let required = (count * (count + 1) / 2) as u64;
        let cancellation = CancellationToken::new();
        for limit in [0, cut % required, required - 1, required, required + extra] {
            let mut work = KernelAccounting::standalone(budget(limit), &cancellation);
            let mut index = Index::new(count, &mut work)
                .map_err(|error| TestCaseError::fail(error.to_string()))?;
            let mut inserted = 0;
            let mut error = None;
            for (id, value) in ids.iter().zip(&values) {
                match index.insert(*id, value, &mut work) {
                    Ok(previous) => { prop_assert_eq!(previous, None); inserted += 1; }
                    Err(failure) => { error = Some(failure); break; }
                }
            }
            prop_assert_eq!(inserted == count, limit >= required);
            prop_assert_eq!(work.spent().items_examined, required.min(limit));
            prop_assert_eq!(index.slots.iter().flatten().count(), inserted);
            for (position, slot) in index.slots.iter().take(inserted).enumerate() {
                prop_assert_eq!(*slot, Some((ids[position], &values[position])));
            }
            if limit < required {
                match error {
                    Some(crate::FsError::Work(WorkError::BudgetExceeded { counter, observed, maximum })) => {
                        prop_assert_eq!(counter, "items_examined");
                        prop_assert_eq!(observed, limit + 1);
                        prop_assert_eq!(maximum, limit);
                    }
                    other => return Err(TestCaseError::fail(format!("unexpected denial: {other:?}"))),
                }
                // Denial must not install the next entry. Retrying it with a
                // fresh ledger still traverses its complete collision chain.
                let mut retry = KernelAccounting::standalone(budget((inserted + 1) as u64), &cancellation);
                prop_assert_eq!(index.insert(ids[inserted], &values[inserted], &mut retry)
                    .map_err(|error| TestCaseError::fail(error.to_string()))?, None);
                prop_assert_eq!(retry.spent().items_examined, (inserted + 1) as u64);
                inserted += 1;
            } else {
                prop_assert!(error.is_none());
            }
            // Duplicate resolution traverses the same occupied chain, returns
            // the original value, and never replaces it with the new value.
            let replacement = usize::MAX;
            for position in 0..inserted {
                let mut duplicate = KernelAccounting::standalone(budget((position + 1) as u64), &cancellation);
                prop_assert_eq!(index.insert(ids[position], &replacement, &mut duplicate)
                    .map_err(|error| TestCaseError::fail(error.to_string()))?, Some(&values[position]));
                prop_assert_eq!(duplicate.spent().items_examined, (position + 1) as u64);
            }
            index.release(&mut work).map_err(|error| TestCaseError::fail(error.to_string()))?;
        }
        // Restore the old one-charge-per-insert accounting in the real loop.
        // The same collision input bypasses a budget below its actual probes.
        let mut work = KernelAccounting::standalone(budget(count as u64), &cancellation);
        let mut broken = Index::new(count, &mut work)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        for (id, value) in ids.iter().zip(&values) {
            prop_assert_eq!(broken.insert_metered::<false>(*id, value, &mut work)
                .map_err(|error| TestCaseError::fail(error.to_string()))?, None);
        }
        prop_assert_ne!(work.spent().items_examined, required);
        prop_assert_eq!(work.spent().items_examined, count as u64);
        prop_assert_eq!(broken.slots.iter().flatten().count(), count);
        broken.release(&mut work).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}

#[test]
fn cancellation_denies_probe_before_index_mutation() -> Result<(), crate::FsError> {
    let cancellation = CancellationToken::new();
    let mut work = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let mut index = Index::new(1, &mut work)?;
    let id = colliding_ids(1)[0];
    let value = 42;
    cancellation.cancel();
    assert!(index.insert(id, &value, &mut work).is_err());
    assert_eq!(work.spent().items_examined, 0);
    assert!(index.slots.iter().all(Option::is_none));
    Ok(())
}
