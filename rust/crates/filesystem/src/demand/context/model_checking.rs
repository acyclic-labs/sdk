use super::*;
use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind};

loom::thread_local! {
    static BROKEN_RELEASE: Cell<bool> = Cell::new(false);
}

pub(super) fn broken_release() -> bool {
    BROKEN_RELEASE.with(Cell::get)
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
            let other = Arc::clone(&capture);
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
    let message = error.downcast_ref::<String>().expect("assertion message");
    assert!(message.contains("capture byte conservation"), "{message}");
}
