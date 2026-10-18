use super::*;
use crate::{CancellationToken, WorkBudget};
use acyclic_test_support::measure;
use proptest::prelude::*;
use std::collections::HashMap;

fn required(count: usize) -> u64 {
    // Independent geometry oracle: first power of two with room for all inputs
    // at the table's 7/8 load, including an empty terminating slot.
    let slots = (1usize..)
        .map(|shift| 1usize << shift)
        .find(|slots| count < *slots && count <= slots - slots / 8);
    slots.map_or(0, |slots| {
        (slots * size_of::<Option<(ObjectId, &u8)>>()) as u64
    })
}

// The former production constructor: its estimates are intentionally retained
// only as a separately initialized allocator/admission negative control.
fn legacy(count: usize, work: &mut KernelAccounting<'_>) -> Result<(), crate::FsError> {
    let estimate = |capacity| (capacity * (2 * (size_of::<(ObjectId, &u8)>() + 1)) + 32) as u64;
    let bytes = estimate(count);
    let mut allocation = work.claim_owned(bytes, 1)?;
    let mut table: HashMap<ObjectId, &u8> = HashMap::new();
    table
        .try_reserve(count)
        .map_err(ObjectStoreError::MemoryTableAllocationFailed)?;
    let actual = estimate(table.capacity());
    work.reconcile_owned(&mut allocation, bytes, actual)?;
    drop(table);
    release(work, allocation, actual)
}

fn attempt(count: usize, remaining: u64, broken: bool) -> (bool, u64, u64, u64) {
    let cancellation = CancellationToken::new();
    let mut work = KernelAccounting::standalone(
        WorkBudget {
            peak_allocation_bytes: remaining,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    let mut accepted = false;
    let physical = measure(|| {
        accepted = if broken {
            legacy(count, &mut work).is_ok()
        } else {
            Index::<u8>::new(count, &mut work)
                .and_then(|index| index.release(&mut work))
                .is_ok()
        };
    });
    (
        accepted,
        physical.bytes_total,
        work.spent().peak_allocation_bytes,
        physical.count_total,
    )
}

fn exact(sample: (bool, u64, u64, u64), bytes: u64, remaining: u64) -> bool {
    let (accepted, physical, charged, allocations) = sample;
    let expected = remaining >= bytes;
    accepted == expected
        && physical == if expected { bytes } else { 0 }
        && charged == physical
        && allocations == u64::from(expected)
}

#[cfg(not(target_arch = "wasm32"))]
fn native_attempt(count: usize, remaining: u64) -> Result<(bool, u64, u64, u64), TestCaseError> {
    let cancellation = CancellationToken::new();
    let baseline = crate::NativeMaterializationWork::accounting_state_bytes();
    let original = crate::NativeMaterializationWork::new(
        WorkBudget {
            peak_allocation_bytes: baseline + remaining,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(error, _)| TestCaseError::fail(error.to_string()))?;
    let mut work = KernelAccounting::native(&original, &cancellation);
    let mut accepted = false;
    let physical = measure(|| {
        accepted = Index::<u8>::new(count, &mut work)
            .and_then(|index| index.release(&mut work))
            .is_ok();
    });
    prop_assert_eq!(original.live_allocation_bytes(), baseline);
    Ok((
        accepted,
        physical.bytes_total,
        original.receipt().peak_allocation_bytes - baseline,
        physical.count_total,
    ))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn exact_backing_admission_holds_below_at_and_above_every_quota(
        count in 1usize..129, extra in 0u64..129,
    ) {
        let bytes = required(count);
        for remaining in [bytes - 1, bytes, bytes + extra] {
            let actual = attempt(count, remaining, false);
            prop_assert!(exact(actual, bytes, remaining), "count={count} remaining={remaining} actual={actual:?}");
            #[cfg(not(target_arch = "wasm32"))] {
                let native = native_attempt(count, remaining)?;
                prop_assert!(exact(native, bytes, remaining), "count={count} remaining={remaining} native={native:?}");
            }
        }
        let broken = attempt(count, bytes, true);
        prop_assert!(!exact(broken, bytes, bytes), "opaque-table estimate passed the exact backing predicate");
    }

    #[test]
    fn fixed_index_preserves_the_first_value_without_insert_allocations(
        entries in prop::collection::vec((0u8..33, any::<u8>()), 1..129),
    ) {
        let cancellation = CancellationToken::new();
        let mut work = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let mut index = Index::new(entries.len(), &mut work).map_err(|error| TestCaseError::fail(error.to_string()))?;
        let identity = |key: u8| ObjectId { kind: crate::ObjectKind::BlobChunk, digest: object_digest(crate::ObjectKind::BlobChunk, &[key]) };
        let mut baseline = HashMap::new();
        let expected: Vec<_> = entries.iter().map(|(key, value)| match baseline.entry(identity(*key)) {
            std::collections::hash_map::Entry::Occupied(previous) => Some(**previous.get()),
            std::collections::hash_map::Entry::Vacant(empty) => { empty.insert(value); None }
        }).collect();
        let mut actual = Vec::with_capacity(entries.len());
        let mut failure = None;
        let physical = measure(|| {
            for (key, value) in &entries {
                match index.insert(identity(*key), value) {
                    Ok(previous) => actual.push(previous.copied()),
                    Err(error) => { failure = Some(error); break; }
                }
            }
        });
        prop_assert!(failure.is_none(), "{failure:?}");
        prop_assert_eq!(actual, expected);
        prop_assert_eq!(physical.count_total, 0);
        prop_assert_eq!(physical.bytes_total, 0);
        index.release(&mut work).map_err(|error| TestCaseError::fail(error.to_string()))?;
        let (ledger, _, _, _) = work.standalone_parts().ok_or_else(|| TestCaseError::fail("missing standalone ledger"))?;
        prop_assert_eq!(ledger.live_bytes(), 0);
    }
}

#[test]
fn fifty_seven_entries_reject_before_allocating_and_accept_affordable_storage() {
    let bytes = required(57);
    for remaining in [bytes - 1, bytes, 6_000, 7_000] {
        let actual = attempt(57, remaining, false);
        let broken = attempt(57, remaining, true);
        assert!(exact(actual, bytes, remaining), "{actual:?}");
        assert!(!exact(broken, bytes, remaining), "{broken:?}");
        eprintln!(
            "index count=57 remaining={remaining} exact_bytes={bytes} candidate={actual:?} legacy={broken:?}"
        );
    }
}
