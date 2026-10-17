use super::*;
use crate::sync::atomic::{AtomicU64, Ordering};
use std::cell::Cell;

loom::thread_local! {
    static OMIT_PLAN_CHARGE: Cell<bool> = Cell::new(false);
    static CHECK_ONLY: Cell<bool> = Cell::new(false);
}
// Restores main's check-only admit, copy, record sequence.
pub(super) fn check_only() -> bool {
    CHECK_ONLY.with(Cell::get)
}
pub(super) fn omit_plan_charge() -> bool {
    OMIT_PLAN_CHARGE.with(Cell::get)
}

fn once<const BROKEN: bool>(
    work: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
    phase: u8,
    actual: &AtomicU64,
) {
    OMIT_PLAN_CHARGE.with(|flag| flag.set(BROKEN && phase == 0));
    CHECK_ONLY.with(|flag| flag.set(BROKEN && phase != 0));
    let mut accounting = KernelAccounting::native(work, cancellation);
    let mut probe = |tag, value, bytes| {
        let count = match (phase, tag) {
            (0, 0 | 1) => 1,
            (1, 2) => bytes,
            (2, 4) => value,
            _ => 0,
        };
        if count != 0 {
            actual.fetch_add(count, Ordering::Relaxed);
        }
    };
    match phase {
        0 => {
            let _ = plan::inspect("x", &mut accounting, char::is_ascii, &mut probe);
        }
        1 => {
            let _ = normalize::encode('é', &mut [0; 2], &mut accounting, &mut probe);
        }
        _ => {
            // Storage is admitted separately in the production caller. This
            // model isolates the same real append span with ready capacity.
            let mut bytes = String::with_capacity(2);
            let _ = append("xy", &mut bytes, &mut accounting, &mut probe);
        }
    }
    OMIT_PLAN_CHARGE.with(|flag| flag.set(false));
    CHECK_ONLY.with(|flag| flag.set(false));
}

fn model<const BROKEN: bool, const CHECK_EFFECTS: bool>(
    phase: u8,
    units: u64,
    depth: u8,
    cancel: bool,
) {
    loom::model(move || {
        let root_cancel = CancellationToken::new();
        let caller = CancellationToken::new();
        let maximum = units * if phase == 0 { 1 } else { 2 };
        let mut limit = WorkBudget::UNBOUNDED;
        if phase == 0 {
            limit.items_examined = maximum;
        } else {
            limit.bytes_copied = maximum;
        }
        let root = crate::NativeMaterializationWork::new(
            if depth == 0 {
                limit
            } else {
                WorkBudget::UNBOUNDED
            },
            &root_cancel,
        )
        .unwrap();
        let mut work = root.clone();
        for _ in 0..depth {
            work = work.native_volume_work(limit).unwrap();
        }
        // Standard Arc transports live immutable storage through join. Actual
        // leaf/resource locks, cancellation and witnesses use crate::sync.
        let actual = Arc::new(AtomicU64::new(0));
        let worker_work = work.clone();
        let worker_cancel = caller.clone();
        let worker_actual = actual.clone();
        let worker = loom::thread::spawn(move || {
            once::<BROKEN>(&worker_work, &worker_cancel, phase, &worker_actual)
        });
        if cancel {
            caller.cancel();
        }
        once::<BROKEN>(&work, &caller, phase, &actual);
        worker.join().unwrap();
        let observed = actual.load(Ordering::Relaxed);
        if CHECK_EFFECTS {
            assert!(
                observed <= maximum,
                "name work must be admitted before it executes"
            );
        }
        let completed = if phase == 0 {
            root.receipt().items_examined
        } else {
            root.receipt().bytes_copied
        };
        assert_eq!(completed, observed, "no name work may lose its charge");
        let credit = root.state(|state| (state.budget, state.original_budget()));
        assert_eq!(credit.0.items_examined, credit.1.items_examined);
        assert_eq!(credit.0.bytes_copied, credit.1.bytes_copied);
    });
}

#[test]
fn real_planning_and_copy_spans_respect_shared_root_and_ancestor_allowances() {
    for phase in 0..3 {
        for units in 0..3 {
            model::<false, true>(phase, units, 0, false);
        }
        model::<false, true>(phase, 1, 2, false);
        model::<false, true>(phase, 1, 1, true);
    }
}

fn message(panic: &(dyn std::any::Any + Send)) -> &str {
    panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("")
}

#[test]
fn missing_planning_commit_and_check_only_copies_fail_the_actual_work_assertions() {
    for phase in 0..3 {
        let panic = std::panic::catch_unwind(|| model::<true, true>(phase, 1, 0, false))
            .expect_err("broken admission/commit must be detected");
        let error = message(&*panic);
        assert!(
            error.contains("name work must be admitted before it executes")
                || (phase == 0 && error.contains("no name work may lose its charge")),
            "{error}"
        );
        if phase != 0 {
            // Without the effect-limit assertion, the old check-only path still
            // records all completed copies and must pass the remaining checks.
            model::<true, false>(phase, 1, 0, false);
        }
    }
}
