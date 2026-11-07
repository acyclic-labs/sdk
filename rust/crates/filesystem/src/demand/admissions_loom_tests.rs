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
