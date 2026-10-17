use super::*;
use crate::kernel::allocation::effects::{OMIT_REFUND, OMIT_RESERVATION};
use crate::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn copy_once<const BROKEN: bool>(
    work: &crate::NativeMaterializationWork,
    cancellation: &crate::CancellationToken,
    allocation: KernelOwnedAllocation,
    actual: &AtomicUsize,
) {
    OMIT_RESERVATION.with(|flag| flag.set(BROKEN));
    let mut accounting = KernelAccounting::native(work, cancellation);
    let mut observed = OriginalReadAccounting {
        accounting: &mut accounting,
        allocation,
        owned_bytes: 0,
        pending: WorkCounters::UNCHARGED,
    };
    if observed
        .observe(super::super::super::codec::ReadAllocationStep::CheckCopy(8))
        .and_then(|()| {
            observed.observe(super::super::super::codec::ReadAllocationStep::BeforeCopy(
                8,
            ))
        })
        .is_ok()
    {
        let mut output = [0; 8];
        output.copy_from_slice(std::hint::black_box(b"contents"));
        assert_eq!(&std::hint::black_box(output), b"contents");
        actual.fetch_add(8, Ordering::Relaxed);
        let _ = observed.observe(super::super::super::codec::ReadAllocationStep::Copied(8));
    }
    let (allocation, bytes) = observed.into_parts();
    assert_eq!(bytes, 0);
    drop(allocation);
}

fn copy_model_impl<const BROKEN: bool, const CHECK_LIMIT: bool>(
    maximum: u64,
    cancel: bool,
    resource: bool,
) {
    loom::model(move || {
        let cancellation = crate::CancellationToken::new();
        let limit = WorkBudget {
            bytes_copied: maximum,
            ..WorkBudget::UNBOUNDED
        };
        let original = crate::NativeMaterializationWork::new(
            if resource {
                WorkBudget::UNBOUNDED
            } else {
                limit
            },
            &cancellation,
        )
        .unwrap();
        let work = if resource {
            original.native_volume_work(limit).unwrap()
        } else {
            original.clone()
        };
        let worker_allocation = work.claim(0, 0, WorkCounters::UNCHARGED).unwrap();
        let local_allocation = work.claim(0, 0, WorkCounters::UNCHARGED).unwrap();
        let baseline = original.live_allocation_bytes();
        // Standard Arc transports storage kept alive through join. Original
        // leaf/resource locks, cancellation and the witness use crate::sync.
        let actual = Arc::new(AtomicUsize::new(0));
        let worker_work = work.clone();
        let worker_cancel = cancellation.clone();
        let worker_actual = actual.clone();
        let worker = loom::thread::spawn(move || {
            copy_once::<BROKEN>(
                &worker_work,
                &worker_cancel,
                KernelOwnedAllocation::NativeOriginal(worker_allocation),
                &worker_actual,
            )
        });
        if cancel {
            cancellation.cancel();
            drop(local_allocation);
        } else {
            copy_once::<BROKEN>(
                &work,
                &cancellation,
                KernelOwnedAllocation::NativeOriginal(local_allocation),
                &actual,
            );
        }
        worker.join().unwrap();
        let completed = actual.load(Ordering::Relaxed) as u64;
        if CHECK_LIMIT {
            assert!(completed <= maximum, "a rejected copy must not execute");
        }
        assert_eq!(
            original.receipt().bytes_copied,
            completed,
            "every completed copy stays charged"
        );
        assert_eq!(original.live_allocation_bytes(), baseline);
    });
}
fn copy_model<const BROKEN: bool>(maximum: u64, cancel: bool) {
    copy_model_impl::<BROKEN, true>(maximum, cancel, true);
}

#[test]
fn competing_copies_reserve_shared_allowance_before_the_effect() {
    copy_model::<false>(0, false);
    copy_model::<false>(8, false);
    copy_model::<false>(16, false);
    copy_model::<false>(8, true);
    copy_model_impl::<false, true>(8, false, false);
}

#[test]
fn missing_reservation_and_deleting_the_limit_assertion_are_detected() {
    let rejected = std::panic::catch_unwind(|| copy_model::<true>(8, false));
    let panic = rejected.expect_err("the real limit assertion must reject the broken reservation");
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("a rejected copy must not execute"),
        "{message}"
    );
    let assertion_deleted =
        std::panic::catch_unwind(|| copy_model_impl::<true, false>(8, false, true));
    assert!(
        assertion_deleted.is_ok(),
        "without the limit assertion the same broken model must pass"
    );
}

fn observer<'a, 'operation>(
    accounting: &'a mut KernelAccounting<'operation>,
    allocation: crate::NativeAllocation,
) -> OriginalReadAccounting<'a, 'operation> {
    OriginalReadAccounting {
        accounting,
        allocation: KernelOwnedAllocation::NativeOriginal(allocation),
        owned_bytes: 0,
        pending: WorkCounters::UNCHARGED,
    }
}

// Read the real root's withheld credit, independently of observer.pending.
// Root reservations subtract from budget; completed work is in the receipt.
fn assert_credit(
    work: &crate::NativeMaterializationWork,
    reserved: u64,
    charged: u64,
    refunded: u64,
    outstanding: u64,
) {
    let (pending, completed, remaining) = work.state(|state| {
        (
            state.original_budget().bytes_copied - state.budget.bytes_copied,
            state.work.bytes_copied,
            state.remaining().map(|remaining| remaining.bytes_copied),
        )
    });
    // Assert outside the leaf so the deliberate failure can unwind without
    // poisoning a ledger that observer/owner destructors must still enter.
    assert_eq!(
        pending + completed + refunded,
        reserved,
        "outstanding + charged + refunded must equal reserved"
    );
    assert_eq!(pending, outstanding, "no abandoned reservation may survive");
    assert_eq!(completed, charged);
    assert_eq!(remaining.unwrap(), 8 - charged - outstanding);
}

// action: abandon, cancel before the effect, panic during the callback, or
// partially complete. A child refunds while a sibling attempts admission;
// their ancestor and root allowance must both conserve the original credit.
fn refund_model<const OMIT: bool>(action: u8) {
    loom::model(move || {
        let cancellation = CancellationToken::new();
        let budget = WorkBudget {
            bytes_copied: 8,
            ..WorkBudget::UNBOUNDED
        };
        let root = crate::NativeMaterializationWork::new(budget, &cancellation).unwrap();
        let parent = root.native_volume_work(budget).unwrap();
        let child = parent.native_volume_work(budget).unwrap();
        let outer_allocation = parent.claim(0, 0, WorkCounters::UNCHARGED).unwrap();
        let inner_allocation = child.claim(0, 0, WorkCounters::UNCHARGED).unwrap();
        let worker_allocation = parent.claim(0, 0, WorkCounters::UNCHARGED).unwrap();
        let mut outer_accounting = KernelAccounting::native(&parent, &cancellation);
        let mut outer = observer(&mut outer_accounting, outer_allocation);
        outer
            .admit(WorkCounters {
                bytes_copied: 4,
                ..WorkCounters::UNCHARGED
            })
            .unwrap();
        let mut inner_accounting = KernelAccounting::native(&child, &cancellation);
        let mut inner = observer(&mut inner_accounting, inner_allocation);
        inner
            .admit(WorkCounters {
                bytes_copied: 4,
                ..WorkCounters::UNCHARGED
            })
            .unwrap();
        assert_credit(&parent, 8, 0, 0, 8);
        let worker_work = parent.clone();
        let worker_cancel = cancellation.clone();
        let worker = loom::thread::spawn(move || {
            let mut accounting = KernelAccounting::native(&worker_work, &worker_cancel);
            let mut observed = observer(&mut accounting, worker_allocation);
            if observed
                .admit(WorkCounters {
                    bytes_copied: 4,
                    ..WorkCounters::UNCHARGED
                })
                .is_err()
            {
                return 0;
            }
            let mut output = [0; 4];
            output.copy_from_slice(std::hint::black_box(b"copy"));
            assert_eq!(&std::hint::black_box(output), b"copy");
            // Completion remains charged even when cancellation races the effect.
            let _ = observed.record_completed(WorkCounters {
                bytes_copied: 4,
                ..WorkCounters::UNCHARGED
            });
            4
        });
        let completed = if action == 3 {
            let mut output = [0; 2];
            output.copy_from_slice(std::hint::black_box(b"co"));
            assert_eq!(&std::hint::black_box(output), b"co");
            let _ = inner.record_completed(WorkCounters {
                bytes_copied: 2,
                ..WorkCounters::UNCHARGED
            });
            2
        } else {
            0
        };
        if action == 1 {
            cancellation.cancel();
        }
        OMIT_REFUND.with(|flag| flag.set(OMIT));
        if action == 2 {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                let _until_unwind = inner;
                panic!("callback panic after reservation");
            }));
            assert!(result.is_err());
        } else {
            drop(inner);
        }
        OMIT_REFUND.with(|flag| flag.set(false));
        let worker_completed = worker.join().unwrap();
        let reserved = 8 + worker_completed;
        let charged = completed + worker_completed;
        let refunded = 4 - completed;
        assert_credit(&parent, reserved, charged, refunded, 4);
        // Parent credit is independently returned after the child's unwind.
        drop(outer);
        assert_credit(&parent, reserved, charged, refunded + 4, 0);
        assert_credit(&child, reserved, charged, refunded + 4, 0);
    });
}

#[test]
fn abandoned_cancelled_panicking_and_child_effects_refund_credit() {
    for action in 0..4 {
        refund_model::<false>(action);
    }
}

#[test]
fn a_missing_refund_is_detected_by_reservation_conservation() {
    let rejected = std::panic::catch_unwind(|| refund_model::<true>(0));
    let panic = rejected.expect_err("omitting a refund must fail conservation");
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("outstanding + charged + refunded must equal reserved"),
        "{message}"
    );
}
