use super::*;
use acyclic_test_support::{AllocationInfo, measure};
use proptest::prelude::*;

#[path = "reserve_baseline.rs"]
mod baseline;

struct Fixture {
    original: NativeMaterializationWork,
    scope: CaptureWorkScope,
    values: Vec<u8>,
}

fn fixture(bytes: &[u8], capacity: usize, denied: u8, cancelled: bool) -> Fixture {
    let cancellation = CancellationToken::new();
    let budget = WorkBudget {
        allocation_operations: 2 + u64::from(capacity != 0) + u64::from(denied != 1),
        bytes_copied: if denied == 2 { 0 } else { u64::MAX },
        ..WorkBudget::UNBOUNDED
    };
    let original = NativeMaterializationWork::new(budget, &cancellation).unwrap();
    let scope = CaptureWorkScope::new(&original).unwrap();
    let mut values = capture_vec(capacity, &scope.native).unwrap();
    assert_eq!(values.capacity(), capacity);
    values.extend_from_slice(bytes);
    if cancelled {
        cancellation.cancel();
    }
    Fixture {
        original,
        scope,
        values,
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Output {
    bytes: Vec<u8>,
    capacity: usize,
    error: Option<String>,
    work: WorkCounters,
    live: u64,
}

fn run(mut fixture: Fixture, additional: usize, old: bool) -> (Output, AllocationInfo) {
    let mut result = None;
    let heap = measure(|| {
        result = Some(if old {
            baseline::reserve(&mut fixture.values, additional, &fixture.scope.native)
        } else {
            capture_reserve(&mut fixture.values, additional, &fixture.scope.native)
        });
    });
    let output = Output {
        bytes: fixture.values.clone(),
        capacity: fixture.values.capacity(),
        // These operations can return only typed work/cancellation errors;
        // Debug retains their wrappers, counter names, limits and actual values.
        error: result.unwrap().err().map(|error| format!("{error:?}")),
        work: fixture.original.receipt(),
        live: fixture.original.live_allocation_bytes(),
    };
    drop(fixture.values);
    drop(fixture.scope);
    assert_eq!(
        fixture.original.live_allocation_bytes(),
        NativeMaterializationWork::accounting_state_bytes()
    );
    (output, heap)
}

fn expected(
    length: usize,
    capacity: usize,
    additional: usize,
    denied: u8,
    cancelled: bool,
) -> (WorkCounters, u64, u64, usize) {
    let header = NativeMaterializationWork::accounting_state_bytes()
        + std::mem::size_of::<crate::demand::CaptureTransientCustody>() as u64;
    let growing = !cancelled && length + additional > capacity;
    let allocated = growing && denied != 1;
    let copied = allocated && (denied != 2 || length == 0);
    let grown = (length + additional).max((capacity * 2).max(4));
    let work = WorkCounters {
        allocation_operations: 2 + u64::from(capacity != 0) + u64::from(allocated),
        peak_allocation_bytes: header + capacity as u64 + if allocated { grown as u64 } else { 0 },
        bytes_copied: if copied { length as u64 } else { 0 },
        ..WorkCounters::UNCHARGED
    };
    let error = cancelled || growing && !copied;
    let heap = u64::from(allocated) + u64::from(error);
    let live = header
        + if copied {
            grown as u64
        } else {
            capacity as u64 + if allocated { grown as u64 } else { 0 }
        };
    (work, live, heap, if copied { grown } else { capacity })
}

fn exact_resources(output: &Output, heap: AllocationInfo, work: WorkCounters, count: u64) -> bool {
    output.work == work && heap.count_total == count
}

#[test]
fn reserve_matches_frozen_baseline_and_has_no_success_error_boxes()
-> Result<(), Box<dyn std::error::Error>> {
    let _subscriber =
        tracing::subscriber::set_default(tracing::subscriber::NoSubscriber::default());
    let check =
        |(bytes, spare, additional, denied, cancelled): (Vec<u8>, usize, usize, u8, bool)| {
            let capacity = bytes.len() + spare;
            let (work, live, count, final_capacity) =
                expected(bytes.len(), capacity, additional, denied, cancelled);
            // Each side owns a new buffer, cancellation source, custody and ledger.
            let (new, heap) = run(
                fixture(&bytes, capacity, denied, cancelled),
                additional,
                false,
            );
            let (old, old_heap) = run(
                fixture(&bytes, capacity, denied, cancelled),
                additional,
                true,
            );
            prop_assert_eq!(&old, &new);
            prop_assert_eq!(&new.bytes, &bytes);
            prop_assert_eq!(new.capacity, final_capacity);
            prop_assert_eq!(new.live, live);
            prop_assert_eq!(new.work, work);
            prop_assert_eq!(
                new.error.is_some(),
                cancelled
                    || bytes.len() + additional > capacity
                        && (denied == 1 || denied == 2 && !bytes.is_empty())
            );
            prop_assert!(exact_resources(&new, heap, work, count));
            let redundant = if cancelled {
                0
            } else if bytes.len() + additional > capacity {
                3
            } else {
                1
            };
            prop_assert_eq!(old_heap.count_total, count + redundant);
            if redundant != 0 {
                // Restoring the old eager boxes must fail the same exact oracle.
                prop_assert!(!exact_resources(&old, old_heap, work, count));
            }
            Ok(())
        };
    for case in [
        (vec![0, 255, 10], 0, 1, 0, false),
        (vec![0, 255, 10], 4, 1, 0, false),
        (vec![0, 255, 10], 0, 1, 1, false),
        (vec![0, 255, 10], 0, 1, 2, false),
        (vec![0, 255, 10], 0, 1, 0, true),
    ] {
        check(case).unwrap();
    }
    proptest::test_runner::TestRunner::new(ProptestConfig {
        cases: 64,
        source_file: Some(file!()),
        ..ProptestConfig::default()
    })
    .run(
        &(
            proptest::collection::vec(any::<u8>(), 0..32),
            0usize..32,
            0usize..32,
            0u8..3,
            any::<bool>(),
        ),
        check,
    )?;
    Ok(())
}

#[test]
fn reserve_overflow_preserves_the_exact_error_and_unchanged_buffer() {
    let old = run(fixture(&[0xff], 1, 0, false), usize::MAX, true);
    let new = run(fixture(&[0xff], 1, 0, false), usize::MAX, false);
    assert_eq!(old.0, new.0);
    assert_eq!(new.0.bytes, [0xff]);
    assert_eq!(new.0.capacity, 1);
    assert_eq!(new.0.error.as_deref(), Some("Kernel(Work(Overflow))"));
    let (work, live, _, _) = expected(1, 1, 0, 0, false);
    assert_eq!(new.0.work, work);
    assert_eq!(new.0.live, live);
    let (new, heap) = run(fixture(&[0xff], 1, 0, false), usize::MAX, false);
    assert!(exact_resources(&new, heap, work, 1));
}
