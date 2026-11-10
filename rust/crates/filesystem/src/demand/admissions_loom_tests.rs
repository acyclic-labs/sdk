use super::accounting::NativeAllocationAccounting;
use crate::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

// Loom's Arc::strong_count is stronger than std's relaxed load. Model the
// reference count explicitly while using the actual production accounting.

#[test]
fn final_owner_acquires_all_retirement_before_discarding_accounting() {
    loom::model(|| {
        const PAYLOAD: u64 = 16;
        const HEADER: u64 = 32;
        // std Arc only carries the model storage; the first atomic models the
        // actual production Arc count, including the original ledger's owner.
        let shared = Arc::new((AtomicUsize::new(2), NativeAllocationAccounting::default()));
        let publishing = shared.clone();
        let publisher = loom::thread::spawn(move || {
            publishing.1.retain_non_native(7);
            publishing.1.retire_native(PAYLOAD);
            publishing.1.retire_non_native(7);
            publishing.1.retire_native(HEADER);
            publishing.0.fetch_sub(1, Ordering::Release);
        });
        let mut retired = shared.1.take_retired();
        let drained = shared
            .1
            .drain_for_final_owner(shared.0.load(Ordering::Relaxed), 0)
            .unwrap();
        if let Some(final_retired) = drained {
            retired += final_retired;
            assert_eq!(
                retired,
                PAYLOAD + HEADER + NativeAllocationAccounting::ALLOCATION_BYTES,
                "final-owner retirement was lost"
            );
            assert_eq!(shared.1.non_native.load(Ordering::Relaxed), 0);
        }
        // Join only after the terminal drain: joining before it would hide the
        // missing acquire by synchronizing with the publisher on every path.
        assert!(publisher.join().is_ok());
        if drained.is_none() {
            // A nonfinal observation leaves the aggregate for the next admitted
            // operation, which must also drain every charge exactly once.
            retired += shared
                .1
                .drain_for_final_owner(shared.0.load(Ordering::Relaxed), 0)
                .unwrap()
                .unwrap();
        }
        assert_eq!(
            retired,
            PAYLOAD + HEADER + NativeAllocationAccounting::ALLOCATION_BYTES
        );
        assert_eq!(shared.1.take_retired(), 0);
    });
}

#[cfg(feature = "native-mount")]
#[test]
fn final_consumer_settles_original_read_credit_before_discarding_accounting() {
    use super::super::DemandWork;
    use crate::{CancellationToken, performance::BOUNDARY_BUDGET};

    for completed in [false, true] {
        loom::model(move || {
            let mut original = DemandWork::new(BOUNDARY_BUDGET, &CancellationToken::new());
            let mut read = original.reserve_view_read().unwrap();
            if completed {
                read.completed();
            }
            // Exactly the original ledger and its actual completion own the
            // aggregate. The model count uses std Arc's relaxed observation.
            assert_eq!(
                Arc::strong_count(original.roots.allocation_accounting.as_ref().unwrap()),
                2
            );
            let owners = Arc::new(AtomicUsize::new(2));
            let publishing = owners.clone();
            let publisher = loom::thread::spawn(move || {
                // The production destructor refunds or records the reservation
                // before releasing its original accounting reference.
                drop(read);
                publishing.fetch_sub(1, Ordering::Release);
            });
            let assert_retired = |original: &DemandWork| {
                assert_eq!(
                    original.roots.pending.items_examined, 0,
                    "final consumer stranded its original read reservation"
                );
                assert_eq!(
                    original.budget.items_examined,
                    BOUNDARY_BUDGET.items_examined
                );
                assert_eq!(original.work.items_examined, u64::from(completed));
                assert_eq!(original.allocations.live_bytes(), 0);
                assert!(original.roots.allocation_accounting.is_none());
            };
            let observed = owners.load(Ordering::Relaxed);
            // Only the Arc count is refined. This is the same reconciliation
            // function, original finite ledger and completion used by callers.
            original
                .reconcile_native_controls_for_owners(observed)
                .unwrap();
            if observed == 1 {
                assert_retired(&original);
            }
            // Joining earlier would supply the acquire missing in the negative
            // control and hide a lost completion before terminal reconciliation.
            assert!(publisher.join().is_ok());
            if observed != 1 {
                original
                    .reconcile_native_controls_for_owners(owners.load(Ordering::Relaxed))
                    .unwrap();
            }
            assert_retired(&original);
        });
    }
}
