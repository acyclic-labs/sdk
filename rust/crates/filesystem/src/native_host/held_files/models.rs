//! Model the kernel's atomic read, no-replace rename and directory flush
//! around the real publication path, using real files for contents. The one
//! kernel lock never nests; no Rust synchronization is added to production.
//! Enabled only by the dedicated `--cfg acyclic_fs_native_loom` command.
use super::*;
use crate::sync::native::Mutex;
use std::{cell::RefCell, sync::Arc};

pub(crate) struct Kernel {
    // Whether the token's directory entry survives a crash at this cut.
    pub(crate) state: Mutex<bool>,
}

thread_local! {
    static CONTEXT: RefCell<Option<Arc<Kernel>>> = const { RefCell::new(None) };
}

pub(crate) fn current() -> Option<Arc<Kernel>> {
    CONTEXT.with_borrow(Clone::clone)
}

struct Enter;
impl Drop for Enter {
    fn drop(&mut self) {
        CONTEXT.set(None);
        publish_probe::omit_reader_sync(false);
    }
}

fn check(omit_reader_sync: bool) {
    loom::model(move || {
        let directory = tempfile::tempdir().unwrap();
        let kernel = Arc::new(Kernel {
            state: Mutex::new(false),
        });
        let publish = |candidate: u8| {
            let kernel = kernel.clone();
            let root = crate::hold_root(directory.path()).unwrap();
            loom::thread::spawn(move || {
                CONTEXT.set(Some(kernel.clone()));
                let _entered = Enter;
                publish_probe::omit_reader_sync(omit_reader_sync);
                let record = root
                    .read_or_publish(Path::new("token"), || Ok([candidate; 16]))
                    .unwrap();
                let durable = *kernel.state.lock().unwrap();
                (record, durable)
            })
        };
        let first = publish(1);
        let second = publish(2);
        let (first, second) = (first.join().unwrap(), second.join().unwrap());
        assert_eq!(first.0, second.0);
        assert!(first.1 && second.1, "returned an unflushed token");
    });
}

#[test]
fn every_publication_schedule_returns_one_durable_record() {
    check(false);
}

#[test]
#[should_panic(expected = "returned an unflushed token")]
fn negative_control_reading_before_the_publishers_flush_is_not_durable() {
    check(true);
}
