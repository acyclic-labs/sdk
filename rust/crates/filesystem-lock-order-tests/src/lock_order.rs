//! Production ranked locks against their independent rank and wake invariants.
#![cfg(not(loom))]

#[path = "../../filesystem/src/lock_order/checked.rs"]
mod lock_order;
#[path = "../../filesystem/src/sync.rs"]
mod sync;

use lock_order::*;
use proptest::prelude::*;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::LockResult;
#[cfg(not(target_arch = "wasm32"))]
use sync::Arc;

fn healthy<T>(result: LockResult<T>) -> T {
    result.unwrap_or_else(|error| std::panic::resume_unwind(Box::new(error.to_string())))
}

fn dynamic_lock(rank: u8, mutex: &crate::sync::Mutex<()>) -> MutexGuard<'_, ()> {
    healthy(match rank {
        0 => lock::<_, 0>(mutex),
        1 => lock::<_, 1>(mutex),
        2 => lock::<_, 2>(mutex),
        3 => lock::<_, 3>(mutex),
        _ => lock::<_, 4>(mutex),
    })
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,
        rng_seed: proptest::test_runner::RngSeed::Fixed(0x7261_6e6b_732d_7631),
        ..ProptestConfig::default()
    })]
    #[test]
    fn every_acquire_and_out_of_order_drop_matches_held_ranks(
        operations in prop::collection::vec((any::<bool>(), 0u8..5), 0..48)
    ) {
        let locks: [_; 5] = std::array::from_fn(|_| crate::sync::Mutex::new(()));
        let mut held = Vec::new();
        for (acquire, rank) in operations {
            if !acquire {
                if !held.is_empty() {
                    drop(held.remove(usize::from(rank) % held.len()));
                }
                continue;
            }
            let allowed = held.iter().all(|(previous, _)| rank > *previous);
            let mutex = locks.get(usize::from(rank)).ok_or_else(||
                proptest::test_runner::TestCaseError::fail("generated rank has no test lock"))?;
            let result = catch_unwind(AssertUnwindSafe(|| dynamic_lock(rank, mutex)));
            prop_assert_eq!(result.is_ok(), allowed);
            if let Ok(guard) = result { held.push((rank, guard)); }
        }
        drop(held);
        // Includes rank zero: a sentinel or stale entry must not reject it.
        drop(dynamic_lock(0, &locks[0]));
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn poison_recovery_keeps_rank_until_recovered_guard_drops() {
    let mutex = Arc::new(Mutex::<_, 1>::new(()));
    let other = Arc::clone(&mutex);
    assert!(
        sync::native::thread::spawn(move || {
            let _guard = healthy(other.lock());
            std::panic::resume_unwind(Box::new("poison the guarded value"));
        })
        .join()
        .is_err()
    );
    let Err(poisoned) = mutex.lock() else {
        unreachable!("the child poisoned the mutex");
    };
    let recovered = poisoned.into_inner();
    let lower = Mutex::<_, 0>::new(());
    assert!(catch_unwind(|| lower.lock()).is_err());
    drop(recovered);
    drop(healthy(lower.lock()));
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn condition_wait_restores_rank_and_rejects_descending_reacquisition() {
    let lower = Mutex::<_, 0>::new(());
    let higher = Arc::new(Mutex::<_, 1>::new(false));
    let condition = Arc::new(Condvar::new());
    let mut guard = healthy(higher.lock());
    let notifier = sync::native::thread::spawn({
        let higher = Arc::clone(&higher);
        let condition = Arc::clone(&condition);
        move || {
            let mut state = healthy(higher.lock());
            *state = true;
            condition.notify_all();
        }
    });
    while !*guard {
        guard = healthy(condition.wait(guard));
    }
    assert!(catch_unwind(|| lower.lock()).is_err());
    drop(guard);
    assert!(notifier.join().is_ok(), "the notifying worker completes");

    let lower_guard = healthy(lower.lock());
    let higher_guard = healthy(higher.lock());
    assert!(catch_unwind(AssertUnwindSafe(|| { condition.wait(lower_guard) })).is_err());
    drop(higher_guard);
    // The rejected wait consumed its guard, so no rank may remain registered.
    drop(
        lower
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn independent_threads_can_hold_equal_ranks() {
    let (first_ready, wait_first) = sync::native::retirement_channel();
    let (second_ready, wait_second) = sync::native::retirement_channel();
    sync::native::thread::scope(|scope| {
        for (ready, wait) in [(first_ready, wait_second), (second_ready, wait_first)] {
            scope.spawn(move || {
                let mutex = Mutex::<_, 1>::new(());
                let _guard = healthy(mutex.lock());
                assert!(
                    ready.send(()).is_ok(),
                    "the peer retains its ready receiver"
                );
                assert!(wait.recv().is_ok(), "the peer announces its held rank");
            });
        }
    });
}
