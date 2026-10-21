//! The dedicated model command instruments the shared production scan and mutex.
#![cfg(acyclic_harness_loom)]

mod primitives {
    include!("../src/sync/arc.rs");
    include!("../src/sync/mutex.rs");
}
mod sync {
    pub(crate) use crate::primitives::Mutex as SyncMutex;
}
#[cfg(debug_assertions)]
#[path = "../src/memory_store/retention_scan_trace.rs"]
mod accounting_trace;
#[path = "../src/memory_store/retention_scan.rs"]
mod owned_scan;

use acyclic_fs::kernel::{KernelAccounting, allocation::AllocationLedger};
use acyclic_fs::{CancellationToken, WorkBudget, WorkCounters};
use primitives::{Arc, Mutex};
use std::collections::BTreeMap;

fn paths() -> BTreeMap<String, ()> {
    [("aa".into(), ()), ("bb".into(), ())].into()
}

#[test]
fn competing_scans_keep_every_charge_and_consistent_state() {
    for (allowance, first_conflict) in [(3, false), (4, false), (1, true), (2, true)] {
        loom::model(move || {
            let items_per_scan = if first_conflict { 1 } else { 2 };
            let cancellation = CancellationToken::default();
            let budget = WorkBudget {
                items_examined: allowance,
                source_bytes_read: 16,
                peak_allocation_bytes: 5,
                ..Default::default()
            };
            let mut allocations = AllocationLedger::default();
            let mut spent = WorkCounters::default();
            KernelAccounting::borrowed_standalone(
                &mut allocations,
                &mut spent,
                budget,
                &cancellation,
            )
            .claim_owned(5, 0)
            .unwrap();
            let work = Arc::new(Mutex::new((spent, Ok(allocations))));
            let mut threads = Vec::new();
            for _ in 0..2 {
                let work = Arc::clone(&work);
                let cancellation = cancellation.clone();
                threads.push(loom::thread::spawn(move || {
                    let result = owned_scan::conflict(
                        &paths(),
                        "zz",
                        budget,
                        &work,
                        &cancellation,
                        |_, _| {
                            // No nested lock is permitted in the owned scan scope.
                            assert!(work.try_lock().is_err());
                            first_conflict
                        },
                    );
                    result.is_ok()
                }));
            }
            // The parent is the observer: the same read may occur anywhere
            // between the two writers, without an idle fourth model thread.
            {
                let state = work.lock().unwrap();
                assert_eq!(state.0.source_bytes_read, state.0.items_examined * 4);
                assert!(
                    state.0.items_examined == 0
                        || state.0.items_examined == items_per_scan
                        || state.0.items_examined == allowance,
                    "partial scan observed between ledger acquisitions"
                );
                assert_eq!(
                    state.0.peak_allocation_bytes,
                    state.1.as_ref().unwrap().live_bytes()
                );
            }
            let successes = threads
                .into_iter()
                .map(|thread| usize::from(thread.join().unwrap()))
                .sum::<usize>();
            let state = work.lock().unwrap();
            assert_eq!(state.0.items_examined, allowance);
            assert_eq!(state.0.source_bytes_read, allowance * 4);
            assert_eq!(state.0.peak_allocation_bytes, 5);
            assert_eq!(successes, (allowance / items_per_scan) as usize);
        });
    }
}

#[test]
#[should_panic(expected = "lost charge")]
fn split_update_negative_control_loses_a_charge() {
    loom::model(|| {
        let work = Arc::new(Mutex::new(WorkCounters::default()));
        let mut threads = Vec::new();
        for _ in 0..2 {
            let work = Arc::clone(&work);
            threads.push(loom::thread::spawn(move || {
                let old = *work.lock().unwrap();
                loom::thread::yield_now();
                let next = old
                    .checked_add(WorkCounters {
                        items_examined: 1,
                        source_bytes_read: 4,
                        ..Default::default()
                    })
                    .unwrap();
                *work.lock().unwrap() = next;
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(work.lock().unwrap().items_examined, 2, "lost charge");
    });
}

#[test]
#[should_panic(expected = "lock-order violation")]
fn reentering_the_owned_ledger_violates_lock_order() {
    loom::model(|| {
        let work = Mutex::new((WorkCounters::default(), Ok(AllocationLedger::default())));
        let cancellation = CancellationToken::default();
        let _ = owned_scan::conflict(
            &paths(),
            "zz",
            WorkBudget::UNBOUNDED,
            &work,
            &cancellation,
            |_, _| {
                assert!(
                    work.try_lock().is_ok(),
                    "lock-order violation: reentered owned ledger"
                );
                false
            },
        );
    });
}

#[test]
#[should_panic(expected = "partial scan")]
fn splitting_the_real_scan_reveals_partial_work() {
    loom::model(|| {
        let work = Arc::new(Mutex::new((
            WorkCounters::default(),
            Ok(AllocationLedger::default()),
        )));
        let writer = Arc::clone(&work);
        let thread = loom::thread::spawn(move || {
            let cancellation = CancellationToken::default();
            for name in ["aa", "bb"] {
                owned_scan::conflict(
                    &BTreeMap::from([(name.to_owned(), ())]),
                    "zz",
                    WorkBudget::UNBOUNDED,
                    &writer,
                    &cancellation,
                    |_, _| false,
                )
                .unwrap();
            }
        });
        let observed = work.lock().unwrap().0;
        thread.join().unwrap();
        assert_ne!(observed.items_examined, 1, "partial scan observed");
    });
}

#[test]
fn cancelled_scans_leave_the_ledger_unchanged_and_unlocked() {
    loom::model(|| {
        let work = Arc::new(Mutex::new((
            WorkCounters::default(),
            Ok(AllocationLedger::default()),
        )));
        let cancellation = CancellationToken::default();
        cancellation.cancel();
        let mut threads = Vec::new();
        for _ in 0..2 {
            let work = Arc::clone(&work);
            let cancellation = cancellation.clone();
            threads.push(loom::thread::spawn(move || {
                assert!(
                    owned_scan::conflict(
                        &paths(),
                        "zz",
                        WorkBudget::UNBOUNDED,
                        &work,
                        &cancellation,
                        |_, _| panic!("cancelled scan compared a key"),
                    )
                    .is_err()
                );
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(work.try_lock().unwrap().0, WorkCounters::default());
    });
}

#[test]
fn cancellation_during_a_scan_preserves_its_admitted_prefix() {
    loom::model(|| {
        let work = Arc::new(Mutex::new((
            WorkCounters::default(),
            Ok(AllocationLedger::default()),
        )));
        let cancellation = CancellationToken::default();
        let writer = Arc::clone(&work);
        let cancel = cancellation.clone();
        let thread = loom::thread::spawn(move || {
            owned_scan::conflict(
                &paths(),
                "zz",
                WorkBudget::UNBOUNDED,
                &writer,
                &cancel,
                |_, _| false,
            )
        });
        let cancel = loom::thread::spawn(move || cancellation.cancel());
        let result = thread.join().unwrap();
        cancel.join().unwrap();
        let state = work.try_lock().unwrap();
        assert_eq!(state.0.source_bytes_read, state.0.items_examined * 4);
        assert_eq!(state.0.items_examined == 2, result.is_ok());
        assert!(state.0.items_examined <= 2);
    });
}
