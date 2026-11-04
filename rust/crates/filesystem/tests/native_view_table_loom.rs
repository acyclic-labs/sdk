//! Build the unified `main` test with cfg(loom), then select `native_view_table_loom::`.
//!
//! Model of the production shared-instance view table: callbacks admitted
//! through `enter`, retirement through the owner, and a concurrent insert.
#![cfg(loom)]

use crate::sync;

#[path = "../src/native_mount/shared/view_table.rs"]
mod view_table;
#[path = "../src/native_mount/shared/views.rs"]
mod views;

use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, Ordering};
use view_table::faults::RETIRE_WITHOUT_DRAIN;
use views::NamedViews;

struct Core {
    id: u32,
    dropped: Arc<AtomicBool>,
}

impl Drop for Core {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

#[test]
fn no_callback_runs_on_a_retired_view() {
    check(false);
}

#[test]
#[should_panic(expected = "callback ran after its view retired")]
fn retiring_without_draining_is_detected() {
    check(true);
}

#[test]
fn stop_unwind_drains_the_original_admitted_callbacks() {
    check_unwind(false);
}

#[test]
#[should_panic(expected = "callback ran after its view retired")]
fn stop_unwind_without_draining_is_detected() {
    check_unwind(true);
}

fn check_unwind(broken: bool) {
    loom::model(move || {
        let table = std::sync::Arc::new(NamedViews::new(u64::MAX));
        let dropped = Arc::new(AtomicBool::new(false));
        let mut owner = table
            .insert(1, |_| {
                Ok(Core {
                    id: 1,
                    dropped: Arc::clone(&dropped),
                })
            })
            .unwrap();
        let (serial, lease) = table.enter_name(&1).unwrap();
        let retired = Arc::new(AtomicBool::new(false));
        let callback = loom::thread::spawn({
            let retired = Arc::clone(&retired);
            move || {
                assert_eq!(lease.id, 1);
                assert!(
                    !retired.load(Ordering::SeqCst),
                    "callback ran after its view retired"
                );
                drop(lease);
            }
        });

        RETIRE_WITHOUT_DRAIN.with(|fault| fault.set(broken));
        struct StopUnwind;
        let stopped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            owner.retire_after(|| {
                assert!(table.enter_name(&1).is_none());
                assert!(table.enter(serial).is_none());
                std::panic::resume_unwind(Box::new(StopUnwind))
            })
        }));
        assert!(stopped.is_err_and(|error| error.is::<StopUnwind>()));
        if !broken {
            assert!(
                dropped.load(Ordering::SeqCst),
                "retirement drops the core before resuming unwind"
            );
        }
        retired.store(true, Ordering::SeqCst);
        assert!(table.enter(serial).is_none(), "a retired serial stays gone");
        callback.join().unwrap();
        assert!(dropped.load(Ordering::SeqCst));
        assert!(table.enter_name(&1).is_none());
    });
}

fn check(broken: bool) {
    loom::model(move || {
        let table = std::sync::Arc::new(NamedViews::new(u64::MAX));
        let dropped = Arc::new(AtomicBool::new(false));
        let mut owner = table
            .insert(1, |_| {
                Ok(Core {
                    id: 1,
                    dropped: Arc::clone(&dropped),
                })
            })
            .unwrap();
        let (serial, initial) = table.enter_name(&1).unwrap();
        drop(initial);
        assert!(matches!(
            table.insert(1, |_| unreachable!()),
            Err(crate::NativeMountError::InvalidDestination)
        ));
        let retired = Arc::new(AtomicBool::new(false));

        // A kernel callback for the view, racing its retirement.
        let callback = loom::thread::spawn({
            let (table, retired) = (std::sync::Arc::clone(&table), Arc::clone(&retired));
            move || {
                if let Some(lease) = table.enter(serial) {
                    assert_eq!(lease.id, 1, "a lease reaches only its own view");
                    assert!(
                        !retired.load(Ordering::SeqCst),
                        "callback ran after its view retired"
                    );
                    drop(lease);
                }
            }
        });

        let (name, retiring) = owner.begin_retirement(|| {}).unwrap();
        assert_eq!(name, 1);
        assert!(owner.begin_retirement(|| unreachable!()).is_none());
        assert!(
            table.enter_name(&1).is_none(),
            "transfer closes name admission"
        );
        assert!(table.enter(serial).is_none(), "transfer closes admission");
        let physical = loom::thread::spawn(move || {
            RETIRE_WITHOUT_DRAIN.with(|fault| fault.set(broken));
            drop(retiring);
        });

        // The observer creates the replacement while the physical owner
        // drains callbacks; a separate creator adds no ownership boundary.
        let replacement = table
            .insert(1, |_| {
                Ok(Core {
                    id: 2,
                    dropped: Arc::new(AtomicBool::new(false)),
                })
            })
            .expect("retirement releases the original name");
        let (replacement_serial, lease) = table.enter_name(&1).unwrap();
        assert_ne!(
            replacement_serial, serial,
            "a reused name receives a fresh serial"
        );
        assert_eq!(lease.id, 2, "a lease reaches only its own view");
        drop(lease);

        physical.join().unwrap();
        if !broken {
            assert!(
                dropped.load(Ordering::SeqCst),
                "retirement drops the core before it returns"
            );
        }
        retired.store(true, Ordering::SeqCst);
        assert!(table.enter(serial).is_none(), "a retired serial stays gone");

        callback.join().unwrap();
        assert!(table.enter(serial).is_none(), "a retired serial stays gone");
        assert_eq!(table.enter_name(&1).unwrap().1.id, 2);
        drop(replacement);
        assert!(table.enter_name(&1).is_none());
    });
}
