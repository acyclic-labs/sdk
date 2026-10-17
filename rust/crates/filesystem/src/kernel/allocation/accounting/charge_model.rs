use super::*;
use std::cell::Cell;

loom::thread_local! {
    static SPLIT_CHARGE: Cell<bool> = Cell::new(false);
}

pub(super) fn split_charge() -> bool {
    SPLIT_CHARGE.with(Cell::get)
}

fn model<const BROKEN: bool, const CHECK_LIMIT: bool, const SCALAR: bool>(
    limit: u64,
    depth: u8,
    recheck: bool,
    cancel: bool,
) {
    loom::model(move || {
        let root_cancel = crate::CancellationToken::new();
        let caller_cancel = crate::CancellationToken::new();
        let budget = WorkBudget {
            bytes_copied: limit,
            items_examined: u64::from(!recheck) * limit,
            ..WorkBudget::UNBOUNDED
        };
        let root = crate::NativeMaterializationWork::new(
            if depth == 0 {
                budget
            } else {
                WorkBudget::UNBOUNDED
            },
            &root_cancel,
        )
        .unwrap();
        let mut work = root.clone();
        for _ in 0..depth {
            work = work.native_volume_work(budget).unwrap();
        }
        if recheck {
            work = work.recheck_admitted();
        }
        let worker_work = work.clone();
        let worker_cancel = caller_cancel.clone();
        let charge = |work: &crate::NativeMaterializationWork,
                      cancellation: &crate::CancellationToken| {
            SPLIT_CHARGE.with(|flag| flag.set(BROKEN));
            let mut accounting = KernelAccounting::native(work, cancellation);
            let _ = if SCALAR {
                accounting.charge_items(1)
            } else {
                accounting.charge(WorkCounters {
                    bytes_copied: 1,
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })
            };
        };
        // Arc only transports storage kept alive through join. The actual
        // leaf, resource mutexes and cancellation atomics use crate::sync.
        let worker = loom::thread::spawn(move || charge(&worker_work, &worker_cancel));
        if cancel {
            caller_cancel.cancel();
        }
        charge(&work, &caller_cancel);
        worker.join().unwrap();
        let receipt = root.receipt();
        if CHECK_LIMIT {
            assert!(
                if SCALAR {
                    receipt.items_examined
                } else {
                    receipt.bytes_copied
                } <= limit,
                "an atomic kernel charge must respect its allowance"
            );
        }
        if SCALAR {
            assert_eq!(receipt.bytes_copied, 0);
        } else {
            assert_eq!(
                receipt.items_examined,
                u64::from(!recheck) * receipt.bytes_copied
            );
        }
    });
}

#[test]
fn competing_kernel_charges_keep_the_real_leaf_through_admission_and_commit() {
    for limit in [0, 1, 2] {
        model::<false, true, false>(limit, 0, false, false);
    }
    model::<false, true, false>(1, 1, false, false);
    model::<false, true, false>(1, 2, true, false);
    model::<false, true, false>(1, 1, false, true);
}

fn rejects_split_charge<const SCALAR: bool>() {
    let error = std::panic::catch_unwind(|| model::<true, true, SCALAR>(1, 0, false, false))
        .expect_err("split admission and commit must violate the allowance");
    let message = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("an atomic kernel charge must respect its allowance"),
        "{message}"
    );
    model::<true, false, SCALAR>(1, 0, false, false);
}

#[test]
fn scalar_kernel_charges_use_the_same_atomic_leaf_path() {
    for limit in [0, 1, 2] {
        model::<false, true, true>(limit, 0, false, false);
    }
    model::<false, true, true>(1, 0, false, true);
}

#[test]
fn splitting_the_real_charge_is_detected_and_needs_the_limit_assertion() {
    rejects_split_charge::<false>();
    rejects_split_charge::<true>();
}
