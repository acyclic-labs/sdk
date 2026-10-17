use super::*;
use crate::sync::atomic::{AtomicU64, Ordering};
use std::cell::RefCell;

// Both production callers copy exactly two bytes: "xy" and "/" + "x".
const COPIED: u64 = 2;
loom::thread_local! {
    static ACTUAL: RefCell<Option<Arc<AtomicU64>>> = RefCell::new(None);
    static CHECK_ONLY: std::cell::Cell<bool> = std::cell::Cell::new(false);
    static UNCHECKED_FILL: std::cell::Cell<bool> = std::cell::Cell::new(false);
    static FILL_CUT: RefCell<Option<(CancellationToken, Option<loom::sync::mpsc::Sender<()>>)>> = RefCell::new(None);
}

pub(super) fn unchecked_fill_charge() -> bool {
    UNCHECKED_FILL.with(std::cell::Cell::get)
}

pub(super) fn before_fill() {
    let cut = FILL_CUT.with(|slot| slot.borrow_mut().take());
    if let Some((caller, ready)) = cut {
        if let Some(ready) = ready {
            ready.send(()).unwrap();
        }
        caller.cancel();
        loom::thread::yield_now();
    }
}

// Restores main's check, copy, record sequence on native ledgers.
pub(super) fn check_only() -> bool {
    CHECK_ONLY.with(std::cell::Cell::get)
}

pub(super) fn completed(copied: u64) {
    ACTUAL.with(|actual| {
        if let Some(actual) = &*actual.borrow() {
            actual.fetch_add(copied, Ordering::Relaxed);
            loom::thread::yield_now();
        }
    });
}

// Runs one real caller. Successful owners are retired after the join, so
// every allocation byte surviving beyond setup is a leak from a failed copy.
fn once<const OLD: bool, const OMIT: bool>(
    work: &crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
    label: Option<&WorkspaceName>,
    actual: &Arc<AtomicU64>,
) -> Option<crate::kernel::Owned<String>> {
    CHECK_ONLY.with(|flag| flag.set(OLD));
    crate::materializer::omit_child_debit(OMIT);
    ACTUAL.with(|slot| *slot.borrow_mut() = Some(actual.clone()));
    let mut accounting = KernelAccounting::native(work, cancellation);
    let copied = match label {
        Some(name) => copy_workspace_label(name, &mut accounting),
        None => restore_absolute_path("x", &mut accounting, cancellation),
    };
    ACTUAL.with(|slot| *slot.borrow_mut() = None);
    CHECK_ONLY.with(|flag| flag.set(false));
    crate::materializer::omit_child_debit(false);
    let owned = copied.ok()?;
    assert_eq!(owned.value, if label.is_some() { "xy" } else { "/x" });
    Some(owned)
}

fn model<const OLD: bool, const CHECK_EFFECTS: bool, const OMIT: bool>(
    copies: u64,
    callers: [bool; 2],
    depth: u8,
    recheck: bool,
    cancel: u8,
) {
    loom::model(move || {
        let root_cancel = CancellationToken::new();
        let caller_cancel = CancellationToken::new();
        let limit = crate::WorkBudget {
            bytes_copied: COPIED * copies,
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
        // Unequal bounds: each outer scope allows one more copy than the
        // next, so an ancestor's credit cannot stand in for a child's.
        let mut work = root.clone();
        let mut scopes = vec![(root.clone(), limit.bytes_copied)];
        for level in 0..depth {
            let bound = limit.bytes_copied + COPIED * u64::from(depth - 1 - level);
            work = work
                .native_volume_work(crate::WorkBudget {
                    bytes_copied: bound,
                    ..crate::WorkBudget::UNBOUNDED
                })
                .unwrap();
            scopes.push((work.clone(), bound));
        }
        if recheck {
            work = work.recheck_admitted();
        }
        let setup = root.live_allocation_bytes();
        let name = WorkspaceName::new(
            "xy",
            &crate::WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .ok()
        .map(|name| name.value);
        let labels = callers.map(|restore| (!restore).then(|| name.clone().unwrap()));
        let actual = Arc::new(AtomicU64::new(0));
        let worker_work = work.clone();
        let worker_cancel = caller_cancel.clone();
        let worker_actual = actual.clone();
        let [worker_label, label] = labels;
        let worker = loom::thread::spawn(move || {
            once::<OLD, OMIT>(
                &worker_work,
                &worker_cancel,
                worker_label.as_ref(),
                &worker_actual,
            )
        });
        match cancel {
            1 => caller_cancel.cancel(),
            2 => root_cancel.cancel(),
            _ => (),
        }
        let owned = once::<OLD, OMIT>(&work, &caller_cancel, label.as_ref(), &actual);
        let owners = [worker.join().unwrap(), owned];
        let retiring = CancellationToken::new();
        let mut accounting = KernelAccounting::native(&work, &retiring);
        for owned in owners.into_iter().flatten() {
            owned.retire(&mut accounting).unwrap();
        }
        let copied = actual.load(Ordering::Relaxed);
        if CHECK_EFFECTS {
            assert!(
                copied <= limit.bytes_copied,
                "workspace text copies must be admitted before they execute"
            );
        }
        assert_eq!(root.receipt().bytes_copied, copied);
        let unreserved =
            root.state(|state| state.budget.bytes_copied == state.original_budget().bytes_copied);
        assert!(unreserved, "copy credit must not remain reserved");
        assert_eq!(
            root.live_allocation_bytes(),
            setup,
            "failed copies retire storage"
        );
        // Each scope's own counters, not the intersected remaining budget:
        // every scope records exactly the copies and holds nothing pending.
        for (local, pending) in work.resource_chain() {
            assert!(
                local.bytes_copied == copied && pending == crate::WorkCounters::UNCHARGED,
                "every scope records its own copy and nothing stays pending"
            );
        }
        // Every bounded scope, including a grandparent of the caller, must
        // hold exactly the unspent credit: nothing reserved or stranded.
        for (scope, bound) in scopes.iter().skip(usize::from(depth != 0)) {
            let remaining = scope.state(|state| state.remaining());
            match bound.checked_sub(copied) {
                Some(copy) => assert_eq!(
                    remaining.unwrap().bytes_copied,
                    copy,
                    "copy credit must not remain reserved"
                ),
                None => assert!(remaining.is_err()),
            }
        }
    });
}

// Label and restore share one helper; one mixed pair covers both callers.
#[test]
fn concurrent_workspace_text_copies_cannot_share_remaining_credit() {
    for copies in 0..3 {
        model::<false, true, false>(copies, [false, true], 0, false, 0);
    }
    model::<false, true, false>(1, [true, true], 2, true, 0);
    model::<false, true, false>(1, [true, false], 2, false, 1);
    model::<false, true, false>(1, [false, true], 0, false, 2);
}

fn message(panic: &(dyn std::any::Any + Send)) -> &str {
    panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("")
}

#[test]
fn old_check_only_copy_admission_fails_the_real_effect_assertion_and_needs_it() {
    let panic =
        std::panic::catch_unwind(|| model::<true, true, false>(1, [false, true], 0, false, 0))
            .expect_err("check-only admission must let both copies spend one allowance");
    assert!(
        message(&*panic).contains("workspace text copies must be admitted before they execute"),
        "{}",
        message(&*panic)
    );
    // Delete only the effect-limit assertion: the old path still records
    // every completed byte and retires storage, so the variant passes.
    model::<true, false, false>(1, [false, true], 0, false, 0);
}

// An intersected remaining budget can let a charged parent mask a child that
// never recorded its copy; the raw per-scope assertion catches it.
#[test]
fn an_omitted_child_debit_fails_the_per_scope_assertion() {
    let panic =
        std::panic::catch_unwind(|| model::<false, true, true>(2, [false, true], 2, false, 0))
            .expect_err("a child scope that skips its debit must be detected");
    assert!(
        message(&*panic).contains("every scope records its own copy"),
        "{}",
        message(&*panic)
    );
}

fn fill_cut<const UNCHECKED: bool>(depth: u8, exhaust: bool) {
    loom::model(move || {
        let caller = CancellationToken::new();
        let root = crate::NativeMaterializationWork::new(
            crate::WorkBudget {
                bytes_copied: COPIED,
                ..crate::WorkBudget::UNBOUNDED
            },
            &CancellationToken::new(),
        )
        .unwrap();
        let mut work = root.clone();
        for level in 0..depth {
            work = work
                .native_volume_work(crate::WorkBudget {
                    bytes_copied: COPIED * u64::from(depth - level),
                    ..crate::WorkBudget::UNBOUNDED
                })
                .unwrap();
        }
        let setup = root.live_allocation_bytes();
        let actual = Arc::new(AtomicU64::new(0));
        let (ready, receiver) = loom::sync::mpsc::channel();
        let competing = work.clone();
        let worker = exhaust.then(|| {
            loom::thread::spawn(move || {
                receiver.recv().unwrap();
                competing.charge(copy_work(COPIED))
            })
        });
        FILL_CUT.with(|slot| *slot.borrow_mut() = Some((caller.clone(), exhaust.then_some(ready))));
        UNCHECKED_FILL.with(|flag| flag.set(UNCHECKED));
        ACTUAL.with(|slot| *slot.borrow_mut() = Some(actual.clone()));
        let mut accounting = KernelAccounting::native(&work, &caller);
        let result = restore_absolute_path("x", &mut accounting, &caller);
        ACTUAL.with(|slot| *slot.borrow_mut() = None);
        UNCHECKED_FILL.with(|flag| flag.set(false));
        let competing = worker.map(|worker| worker.join().unwrap());
        assert!(
            matches!(&result, Err(WorkspaceError::Cancelled(_))),
            "caller cancellation at fill precedes competing credit exhaustion: {:?}",
            result.as_ref().err()
        );
        assert_eq!(
            actual.load(Ordering::Relaxed),
            0,
            "caller cancellation at fill prevents copies"
        );
        assert_eq!(root.live_allocation_bytes(), setup);
        assert_eq!(
            root.receipt().bytes_copied,
            if exhaust { COPIED } else { 0 }
        );
        assert!(competing.is_none_or(|result| result.is_ok()));
        for (local, pending) in work.resource_chain() {
            assert_eq!(local.bytes_copied, if exhaust { COPIED } else { 0 });
            assert_eq!(pending, crate::WorkCounters::UNCHARGED);
        }
    });
}

#[test]
fn cancellation_after_storage_admission_precedes_fill_and_racing_credit_exhaustion() {
    for depth in [0, 2] {
        for exhaust in [false, true] {
            fill_cut::<false>(depth, exhaust);
        }
    }
}

#[test]
fn ignoring_the_caller_at_fill_fails_the_cancellation_cut_assertions() {
    for exhaust in [false, true] {
        let panic = std::panic::catch_unwind(|| fill_cut::<true>(0, exhaust))
            .expect_err("caller-blind charge must fail the exact fill cut");
        assert!(
            message(&*panic).contains("caller cancellation at fill"),
            "{}",
            message(&*panic)
        );
    }
}
