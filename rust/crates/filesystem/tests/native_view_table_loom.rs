//! Build the unified `main` test with cfg(loom), then select `native_view_table_loom::`.
//!
//! Model of the production shared-instance view table: callbacks admitted
//! through `enter`, retirement through the owner, and a concurrent insert.
#![cfg(loom)]

use crate::sync;

#[path = "../src/native_mount/shared/view_table.rs"]
mod view_table;

use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, Ordering};
use view_table::{ViewTable, faults::RETIRE_WITHOUT_DRAIN};

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

fn check(broken: bool) {
    loom::model(move || {
        let table = Arc::new(ViewTable::new(u64::MAX));
        let dropped = Arc::new(AtomicBool::new(false));
        let owner = ViewTable::insert(
            &table,
            Core {
                id: 1,
                dropped: Arc::clone(&dropped),
            },
        )
        .unwrap();
        let serial = owner.serial().get();
        let retired = Arc::new(AtomicBool::new(false));

        // A kernel callback for the view, racing its retirement.
        let callback = loom::thread::spawn({
            let (table, retired) = (Arc::clone(&table), Arc::clone(&retired));
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

        // A replacement view created while the old one retires.
        let creator = loom::thread::spawn({
            let table = Arc::clone(&table);
            move || {
                let replacement = ViewTable::insert(
                    &table,
                    Core {
                        id: 2,
                        dropped: Arc::new(AtomicBool::new(false)),
                    },
                )
                .unwrap();
                assert_ne!(
                    replacement.serial().get(),
                    serial,
                    "serials are never reused"
                );
                let lease = table.enter(replacement.serial().get()).unwrap();
                assert_eq!(lease.id, 2, "a lease reaches only its own view");
                drop(lease);
                replacement
            }
        });

        RETIRE_WITHOUT_DRAIN.with(|fault| fault.set(broken));
        drop(owner);
        if !broken {
            assert!(
                dropped.load(Ordering::SeqCst),
                "retirement drops the core before it returns"
            );
        }
        retired.store(true, Ordering::SeqCst);
        assert!(table.enter(serial).is_none(), "a retired serial stays gone");

        callback.join().unwrap();
        let replacement = creator.join().unwrap();
        assert!(table.enter(serial).is_none(), "a retired serial stays gone");
        assert_eq!(table.len(), 1);
        drop(replacement);
        assert_eq!(table.len(), 0);
    });
}
