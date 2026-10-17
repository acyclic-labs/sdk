use super::*;
use crate::kernel::allocation::effects::{OMIT_REFUND, OMIT_RESERVATION};
use crate::sync::atomic::{AtomicU64, Ordering};
use std::cell::RefCell;

const HASHED: u64 = (b"acyclic-fs-workspace-id-v1\0".len() + 16 + 8 + 1) as u64;
loom::thread_local! {
    static ACTUAL: RefCell<Option<Arc<[AtomicU64; 2]>>> = RefCell::new(None);
}

pub(super) fn completed(copied: u64, hashed: u64) {
    ACTUAL.with(|actual| {
        if let Some(actual) = &*actual.borrow() {
            actual[0].fetch_add(copied, Ordering::Relaxed);
            actual[1].fetch_add(hashed, Ordering::Relaxed);
            loom::thread::yield_now();
        }
    });
}

fn once<const OLD: bool>(
    work: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
    name: &WorkspaceName,
    actual: &Arc<[AtomicU64; 2]>,
) {
    OMIT_RESERVATION.with(|flag| flag.set(OLD));
    ACTUAL.with(|slot| *slot.borrow_mut() = Some(actual.clone()));
    let _ = WorkspaceId::derive([3; 16], name, &work, cancellation);
    ACTUAL.with(|slot| *slot.borrow_mut() = None);
    OMIT_RESERVATION.with(|flag| flag.set(false));
}

fn assert_completed(
    work: &crate::NativeMaterializationWork,
    root: &crate::NativeMaterializationWork,
    actual: &[AtomicU64; 2],
    limit: crate::WorkBudget,
) {
    let copied = actual[0].load(Ordering::Relaxed);
    let hashed = actual[1].load(Ordering::Relaxed);
    assert_eq!(root.receipt().bytes_copied, copied);
    assert_eq!(root.receipt().bytes_hashed, hashed);
    let unreserved = root.state(|state| {
        state.budget.bytes_copied == state.original_budget().bytes_copied
            && state.budget.bytes_hashed == state.original_budget().bytes_hashed
    });
    assert!(unreserved, "identity credit must not remain reserved");
    let remaining = work.state(|state| state.remaining());
    match (
        limit.bytes_copied.checked_sub(copied),
        limit.bytes_hashed.checked_sub(hashed),
    ) {
        (Some(copy), Some(hash)) => {
            let remaining = remaining.unwrap();
            assert_eq!(remaining.bytes_copied, copy);
            assert_eq!(remaining.bytes_hashed, hash);
        }
        _ => assert!(remaining.is_err()),
    }
}

fn model<const OLD: bool, const CHECK_EFFECTS: bool>(
    copies: u64,
    hashes: u64,
    depth: u8,
    recheck: bool,
    cancel: u8,
) {
    loom::model(move || {
        let root_cancel = CancellationToken::new();
        let caller_cancel = CancellationToken::new();
        let limit = crate::WorkBudget {
            bytes_copied: 16 * copies,
            bytes_hashed: HASHED * hashes,
            ..crate::WorkBudget::UNBOUNDED
        };
        let root = crate::NativeMaterializationWork::new(
            if depth == 0 {
                limit
            } else {
                crate::WorkBudget::UNBOUNDED
            },
            &root_cancel,
        )
        .unwrap();
        let mut work = root.clone();
        for _ in 0..depth {
            work = work.native_volume_work(limit).unwrap();
        }
        if recheck {
            work = work.recheck_admitted();
        }
        // Immutable fixture storage lives through join; actual leaf/resource
        // locks, cancellation and the byte witnesses use the crate's shim.
        let name = WorkspaceName(Arc::new(("x".into(), KernelOwnedAllocation::Standalone)));
        let actual = Arc::new([AtomicU64::new(0), AtomicU64::new(0)]);
        let worker_work = work.clone();
        let worker_cancel = caller_cancel.clone();
        let worker_name = name.clone();
        let worker_actual = actual.clone();
        let worker = loom::thread::spawn(move || {
            once::<OLD>(&worker_work, &worker_cancel, &worker_name, &worker_actual)
        });
        match cancel {
            1 => caller_cancel.cancel(),
            2 => root_cancel.cancel(),
            _ => (),
        }
        once::<OLD>(&work, &caller_cancel, &name, &actual);
        worker.join().unwrap();
        if CHECK_EFFECTS {
            assert!(
                actual[0].load(Ordering::Relaxed) <= limit.bytes_copied
                    && actual[1].load(Ordering::Relaxed) <= limit.bytes_hashed,
                "identity effects must be admitted before they execute"
            );
        }
        assert_completed(&work, &root, &actual, limit);
    });
}

#[test]
fn real_workspace_id_hashes_and_copies_share_the_admitted_allowance() {
    for (copies, hashes) in [(0, 2), (1, 2), (2, 0), (2, 1), (1, 1), (2, 2)] {
        model::<false, true>(copies, hashes, 0, false, 0);
    }
    model::<false, true>(1, 1, 2, true, 0);
    model::<false, true>(1, 1, 1, false, 1);
    model::<false, true>(1, 1, 0, false, 2);
}

fn message(panic: &(dyn std::any::Any + Send)) -> &str {
    panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("")
}

#[test]
fn old_check_only_admission_fails_the_real_effect_assertion_and_needs_it() {
    for (copies, hashes) in [(1, 2), (2, 1)] {
        let panic = std::panic::catch_unwind(|| model::<true, true>(copies, hashes, 0, false, 0))
            .expect_err("check-only admission must allow an unadmitted identity effect");
        assert!(
            message(&*panic).contains("identity effects must be admitted before they execute"),
            "{}",
            message(&*panic)
        );
        // Delete only the effect-limit assertion: the same old production path
        // still reports all completed work, so the broken variant must pass.
        model::<true, false>(copies, hashes, 0, false, 0);
    }
}

fn unwind_model<const OMIT: bool>() {
    loom::model(|| {
        let cancellation = CancellationToken::new();
        let limit = crate::WorkBudget {
            bytes_copied: 16,
            bytes_hashed: HASHED,
            ..crate::WorkBudget::UNBOUNDED
        };
        let root = crate::NativeMaterializationWork::new(limit, &cancellation).unwrap();
        let work = root
            .native_volume_work(limit)
            .unwrap()
            .native_volume_work(limit)
            .unwrap();
        let name = WorkspaceName(Arc::new(("x".into(), KernelOwnedAllocation::Standalone)));
        let actual = Arc::new([AtomicU64::new(0), AtomicU64::new(0)]);
        let worker_work = work.clone();
        let worker_cancel = cancellation.clone();
        let worker_actual = actual.clone();
        let worker = loom::thread::spawn(move || {
            once::<false>(&worker_work, &worker_cancel, &name, &worker_actual)
        });
        OMIT_REFUND.with(|flag| flag.set(OMIT));
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut accounting = KernelAccounting::native(&work, &cancellation);
            accounting.perform(
                crate::WorkCounters {
                    bytes_copied: 16,
                    bytes_hashed: HASHED,
                    ..crate::WorkCounters::UNCHARGED
                },
                || panic!("abandoned identity before the effect"),
            )
        }));
        OMIT_REFUND.with(|flag| flag.set(false));
        worker.join().unwrap();
        assert_completed(&work, &root, &actual, limit);
    });
}

#[test]
fn unwinding_before_an_effect_restores_both_ancestor_credits() {
    unwind_model::<false>();
}

#[test]
fn skipping_unwind_refunds_is_detected() {
    let panic = std::panic::catch_unwind(unwind_model::<true>)
        .expect_err("missing refund must strand credit");
    assert!(
        message(&*panic).contains("identity credit must not remain reserved"),
        "{}",
        message(&*panic)
    );
}
