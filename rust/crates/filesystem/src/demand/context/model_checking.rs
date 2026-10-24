use super::*;
use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind};

fn panic_message(error: &(dyn std::any::Any + Send)) -> &str {
    error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .expect("assertion message")
}

loom::thread_local! {
    static BROKEN_RELEASE: Cell<bool> = Cell::new(false);
    static SKIP_WORKER_WAIT: Cell<bool> = Cell::new(false);
    static ALLOW_CLOSED_WORKER: Cell<bool> = Cell::new(false);
    static CLOSE_WITHOUT_LEAF: Cell<bool> = Cell::new(false);
}

pub(super) fn broken_release() -> bool {
    BROKEN_RELEASE.with(Cell::get)
}

pub(super) fn skip_worker_wait() -> bool {
    SKIP_WORKER_WAIT.with(Cell::get)
}

pub(super) fn allow_closed_worker() -> bool {
    ALLOW_CLOSED_WORKER.with(Cell::get)
}

pub(super) fn close_without_leaf() -> bool {
    CLOSE_WITHOUT_LEAF.with(Cell::get)
}

#[test]
fn capture_close_joins_in_flight_transient_accounting_before_draining() {
    crate::initialize_test_native_io();
    let exercise = |broken| {
        loom::model(move || {
            let work = crate::NativeMaterializationWork::new(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .unwrap();
            let capture = CaptureTransientCustody::new_native(&work).unwrap();
            let bound = work.with_capture_custody(&capture).unwrap();
            let task = loom::thread::spawn(move || {
                let _ = bound.with_original_leaf(|state| {
                    state.claim_allocation(1, 1, WorkCounters::UNCHARGED)
                });
            });
            CLOSE_WITHOUT_LEAF.with(|flag| flag.set(broken));
            capture.close().unwrap();
            task.join().unwrap();
            drop(capture);
            assert_eq!(
                work.live_allocation_bytes(),
                crate::NativeMaterializationWork::accounting_state_bytes(),
                "transient capture allocation retirement"
            );
        });
    };
    exercise(false);
    let error = catch_unwind(AssertUnwindSafe(|| exercise(true)))
        .expect_err("closing outside the leaf must lose transient custody");
    let message = panic_message(&*error);
    assert!(
        message.contains("transient capture allocation retirement"),
        "{message}"
    );
}

#[test]
fn capture_close_waits_for_accepted_payloads_but_not_accounting_handles() {
    crate::initialize_test_native_io();
    let exercise = |broken| {
        loom::model(move || {
            let work = crate::NativeMaterializationWork::new(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .unwrap();
            let capture = CaptureTransientCustody::new_native(&work).unwrap();
            let stale = work.with_capture_custody(&capture).unwrap();
            let worker = CaptureTransientCustody::worker(&capture).unwrap();
            let done = Arc::new(AtomicBool::new(false));
            let other_done = done.clone();
            let task = loom::thread::spawn(move || {
                other_done.store(true, Ordering::Release);
                drop(worker);
            });
            SKIP_WORKER_WAIT.with(|flag| flag.set(broken));
            capture.close().unwrap();
            let retired_at_close = done.load(Ordering::Acquire);
            task.join().unwrap();
            assert!(
                retired_at_close,
                "accepted capture payload retired: {retired_at_close}"
            );
            assert!(CaptureTransientCustody::worker(&capture).is_err());
            assert!(stale.with_original_leaf(|state| state.check()).is_err());
            drop(capture);
            assert_eq!(
                work.live_allocation_bytes(),
                crate::NativeMaterializationWork::accounting_state_bytes()
                    + CaptureTransientCustody::ALLOCATION_BYTES
            );
            drop(stale);
            assert_eq!(
                work.live_allocation_bytes(),
                crate::NativeMaterializationWork::accounting_state_bytes()
            );
        });
    };
    exercise(false);
    let error = catch_unwind(AssertUnwindSafe(|| exercise(true)))
        .expect_err("closing without worker retirement must fail");
    let message = panic_message(&*error);
    assert!(
        message.contains("accepted capture payload retired"),
        "{message}"
    );
}

#[test]
fn capture_worker_registration_racing_close_never_returns_an_active_closed_lease() {
    crate::initialize_test_native_io();
    let exercise = |broken| {
        loom::model(move || {
            let work = crate::NativeMaterializationWork::new(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .unwrap();
            let capture = CaptureTransientCustody::new_native(&work).unwrap();
            let other = capture.clone();
            let task = loom::thread::spawn(move || {
                ALLOW_CLOSED_WORKER.with(|flag| flag.set(broken));
                if let Ok(worker) = CaptureTransientCustody::worker(&other) {
                    let open = other.is_open();
                    drop(worker);
                    open
                } else {
                    true
                }
            });
            capture.close().unwrap();
            let registered_open = task.join().unwrap();
            assert!(registered_open, "registered worker remains open");
            assert!(!capture.is_open());
            drop(capture);
            assert_eq!(
                work.live_allocation_bytes(),
                crate::NativeMaterializationWork::accounting_state_bytes()
            );
        });
    };
    exercise(false);
    let error = catch_unwind(AssertUnwindSafe(|| exercise(true)))
        .expect_err("registration after closure must fail the same lifetime bound");
    let message = panic_message(&*error);
    assert!(
        message.contains("registered worker remains open"),
        "{message}"
    );
}

#[test]
fn concurrent_capture_retirement_and_transfer_conserve_every_byte() {
    crate::initialize_test_native_io();
    let exercise = |broken| {
        loom::model(move || {
            let work = crate::NativeMaterializationWork::new(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .unwrap();
            let capture = CaptureTransientCustody::new_native(&work).unwrap();
            capture.claim(5);
            let other = capture.clone();
            let worker = loom::thread::spawn(move || {
                BROKEN_RELEASE.with(|fault| fault.set(broken));
                other.claim(3);
                other.release(3);
            });
            capture.transfer_retained_bytes(2).unwrap();
            worker.join().unwrap();
            assert_eq!(
                capture.take_retained_bytes(),
                3,
                "capture byte conservation"
            );
            assert_eq!(capture.take_retained_bytes(), 0);
            assert!(capture.transfer_retained_bytes(1).is_err());
        });
    };
    exercise(false);
    let error = catch_unwind(AssertUnwindSafe(|| exercise(true)))
        .expect_err("a non-atomic release must lose an update");
    let message = panic_message(&*error);
    assert!(message.contains("capture byte conservation"), "{message}");
}
