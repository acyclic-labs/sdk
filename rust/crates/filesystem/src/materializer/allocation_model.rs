use super::*;
use crate::{CancellationToken, WorkBudget, WorkCounters};
use std::cell::Cell;

loom::thread_local! {
    static FORCE_LEDGER: Cell<bool> = Cell::new(false);
}
pub(super) fn force_ledger_release() -> bool {
    FORCE_LEDGER.with(Cell::get)
}

fn model<const BROKEN: bool>(resources: bool, cancelled: bool) {
    loom::model(move || {
        let cancel = CancellationToken::new();
        let root = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancel).unwrap();
        let parent = if resources {
            root.native_volume_work(WorkBudget::UNBOUNDED).unwrap()
        } else {
            root.clone()
        };
        let work = if resources {
            parent.native_volume_work(WorkBudget::UNBOUNDED).unwrap()
        } else {
            parent.clone()
        };
        let baseline = root.live_allocation_bytes();
        let parent_credit = parent
            .resource
            .as_ref()
            .map(|r| r.remaining().unwrap().peak_allocation_bytes);
        let child_credit = work
            .resource
            .as_ref()
            .map(|r| r.remaining().unwrap().peak_allocation_bytes);
        let mut retained = work.claim(4, 1, WorkCounters::UNCHARGED).unwrap();
        let mut retiring = work.claim(4, 1, WorkCounters::UNCHARGED).unwrap();
        let mut empty = work.claim(0, 0, WorkCounters::UNCHARGED).unwrap();
        let receipt = root.receipt();
        // Context owners remain live through join. Every state lock exercised
        // below is the production crate::sync leaf or resource mutex.
        let worker = loom::thread::spawn(move || {
            if cancelled {
                cancel.cancel();
            }
            retiring.release(4).unwrap();
        });
        FORCE_LEDGER.with(|flag| flag.set(BROKEN));
        let nested = work.state(|_| {
            // Catch the checker's deliberately broken reentrant acquisition
            // before unwinding across/poisoning the original leaf guard.
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                empty.release(0).unwrap();
                retained.release(0).unwrap();
            }))
        });
        FORCE_LEDGER.with(|flag| flag.set(false));
        worker.join().unwrap();
        assert!(
            nested.is_ok(),
            "empty retirement must not reacquire the original leaf"
        );
        assert_eq!(root.live_allocation_bytes(), baseline + 4);
        assert_eq!(root.receipt(), receipt);
        drop(empty);
        drop(retained);
        assert_eq!(root.live_allocation_bytes(), baseline);
        assert_eq!(root.receipt(), receipt);
        assert_eq!(
            parent
                .resource
                .as_ref()
                .map(|r| r.remaining().unwrap().peak_allocation_bytes),
            parent_credit
        );
        assert_eq!(
            work.resource
                .as_ref()
                .map(|r| r.remaining().unwrap().peak_allocation_bytes),
            child_credit
        );
    });
}

#[test]
fn empty_retirement_is_an_identity_while_nonempty_retirement_races_cancellation() {
    for resource in [false, true] {
        for cancelled in [false, true] {
            model::<false>(resource, cancelled);
        }
    }
}
#[test]
#[should_panic(expected = "empty retirement must not reacquire the original leaf")]
fn reacquiring_the_original_leaf_is_detected() {
    model::<true>(true, false);
}
