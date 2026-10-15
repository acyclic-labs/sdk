use super::*;
use proptest::prelude::*;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::LockResult;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::{Arc, Barrier};
#[cfg(not(target_arch = "wasm32"))]
use std::time::Duration;

fn healthy<T>(result: LockResult<T>) -> T {
    result.unwrap_or_else(|error| std::panic::resume_unwind(Box::new(error.to_string())))
}

fn dynamic_lock(rank: u8, mutex: &std::sync::Mutex<()>) -> MutexGuard<'_, ()> {
    healthy(match rank {
        0 => lock::<_, 0>(mutex),
        1 => lock::<_, 1>(mutex),
        2 => lock::<_, 2>(mutex),
        3 => lock::<_, 3>(mutex),
        _ => lock::<_, 4>(mutex),
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn every_acquire_and_out_of_order_drop_matches_held_ranks(
        operations in prop::collection::vec((any::<bool>(), 0u8..5), 0..48)
    ) {
        let locks: [_; 5] = std::array::from_fn(|_| std::sync::Mutex::new(()));
        let mut held = Vec::new();
        for (acquire, rank) in operations {
            if !acquire {
                if !held.is_empty() {
                    drop(held.remove(usize::from(rank) % held.len()));
                }
                continue;
            }
            let allowed = held.iter().all(|(previous, _)| rank > *previous);
            let result = catch_unwind(AssertUnwindSafe(|| dynamic_lock(rank, &locks[usize::from(rank)])));
            prop_assert_eq!(result.is_ok(), allowed);
            if let Ok(guard) = result { held.push((rank, guard)); }
        }
        drop(held);
        // Includes rank zero: a sentinel or stale entry must not reject it.
        drop(dynamic_lock(0, &locks[0]));
    }
}

#[test]
fn violation_names_both_acquisition_sites() {
    let higher = Mutex::<_, VIEW>::new(());
    let lower = Mutex::<_, OPERATION>::new(());
    let held_line = line!() + 1;
    let _held = healthy(higher.lock());
    let requested_line = line!() + 1;
    let result = catch_unwind(|| lower.lock());
    let Err(error) = result else {
        unreachable!("descending acquisition must panic");
    };
    let Some(message) = error.downcast_ref::<String>() else {
        unreachable!("rank panic has a message");
    };
    assert!(message.contains(&format!("{}:{held_line}:", file!())));
    assert!(message.contains(&format!("{}:{requested_line}:", file!())));
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn demand_leaf_reentry_panics_before_locking() {
    let meter = crate::demand::NativeDemandReadMeter::new(crate::demand::DemandWork::new(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    ));
    let guard = meter.lock();
    assert!(catch_unwind(AssertUnwindSafe(|| meter.lock())).is_err());
    drop(guard);
    drop(meter.lock());
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn poison_recovery_keeps_rank_until_recovered_guard_drops() {
    let mutex = Arc::new(Mutex::<_, VIEW>::new(()));
    let other = Arc::clone(&mutex);
    assert!(
        std::thread::spawn(move || {
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
    let lower = Mutex::<_, OPERATION>::new(());
    assert!(catch_unwind(|| lower.lock()).is_err());
    drop(recovered);
    drop(healthy(lower.lock()));
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn condition_wait_restores_rank_and_rejects_descending_reacquisition() {
    let lower = Mutex::<_, OPERATION>::new(());
    let higher = Mutex::<_, VIEW>::new(());
    let condition = Condvar::new();
    let guard = healthy(higher.lock());
    let (guard, _) = healthy(condition.wait_timeout(guard, Duration::ZERO));
    assert!(catch_unwind(|| lower.lock()).is_err());
    drop(guard);

    let lower_guard = healthy(lower.lock());
    let higher_guard = healthy(higher.lock());
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            condition.wait_timeout(lower_guard, Duration::ZERO)
        }))
        .is_err()
    );
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
fn public_while_predicates_and_returned_guards_keep_the_rank() {
    for timed in [false, true] {
        let higher = Mutex::<_, VIEW>::new(());
        let lower = Mutex::<_, OPERATION>::new(());
        let condition = Condvar::new();
        let guard = healthy(higher.lock());
        let predicate = |_: &mut ()| {
            assert!(catch_unwind(|| lower.lock()).is_err());
            false
        };
        let guard = if timed {
            let (guard, timeout) =
                healthy(condition.wait_timeout_while(guard, Duration::ZERO, predicate));
            assert!(!timeout.timed_out());
            guard
        } else {
            healthy(condition.wait_while(guard, predicate))
        };
        assert!(catch_unwind(|| lower.lock()).is_err());
        drop(guard);
        drop(healthy(lower.lock()));
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn independent_threads_can_hold_equal_ranks() {
    let barrier = Arc::new(Barrier::new(2));
    std::thread::scope(|scope| {
        for _ in 0..2 {
            let barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                let mutex = Mutex::<_, VIEW>::new(());
                let _guard = healthy(mutex.lock());
                barrier.wait();
            });
        }
    });
}
