use loom::sync::atomic::{AtomicU64, AtomicUsize, Ordering, fence};
use std::sync::Arc;

// Compile production publication, drains and the final-owner fence with Loom.
// Loom's
// Arc::strong_count synchronizes more strongly than std's relaxed load, so model
// std Arc's release decrement and relaxed count explicitly below.
#[path = "admissions_accounting.rs"]
mod accounting;
use accounting::NativeProofAccounting;

fn reclamation_model(acquire_final_owner: fn(usize) -> bool) {
    loom::model(move || {
        const PAYLOAD: u64 = 16;
        const HEADER: u64 = 32;
        // std Arc only carries the model storage; the first atomic models the
        // actual production Arc count, including the original ledger's owner.
        let shared = Arc::new((AtomicUsize::new(2), NativeProofAccounting::default()));
        let publishing = shared.clone();
        let publisher = loom::thread::spawn(move || {
            publishing.1.retain_non_native(7);
            publishing.1.retire_native(PAYLOAD);
            publishing.1.retire_non_native(7);
            publishing.1.retire_native(HEADER);
            publishing.0.fetch_sub(1, Ordering::Release);
        });
        let mut retired = shared.1.take_retired();
        let discarded = acquire_final_owner(shared.0.load(Ordering::Relaxed));
        if discarded {
            retired += shared.1.take_retired();
            assert_eq!(retired, PAYLOAD + HEADER, "final-owner retirement was lost");
            assert_eq!(shared.1.non_native.load(Ordering::Relaxed), 0);
        }
        // Join only after the terminal drain: joining before it would hide the
        // missing acquire by synchronizing with the publisher on every path.
        assert!(publisher.join().is_ok());
        if !discarded {
            // A nonfinal observation leaves the aggregate for the next admitted
            // operation, which must also drain every charge exactly once.
            retired += shared.1.take_retired();
            assert!(acquire_final_owner(shared.0.load(Ordering::Relaxed)));
        }
        assert_eq!(retired, PAYLOAD + HEADER);
        assert_eq!(shared.1.take_retired(), 0);
    });
}

#[test]
fn final_owner_acquires_all_retirement_before_discarding_accounting() {
    reclamation_model(accounting::acquire_final_owner);
}

#[test]
#[should_panic(expected = "final-owner retirement was lost")]
fn missing_final_owner_acquire_reproduces_lost_retirement() {
    reclamation_model(|owners| owners == 1);
}
