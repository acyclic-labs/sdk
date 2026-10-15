use crate::{
    Error, Result,
    mcp::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

// No locks: the parent publishes cancellation, then awaits physical completion.
pub(super) struct CancelOnDrop(pub(super) Arc<AtomicBool>);

impl CancelOnDrop {
    pub(super) fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.cancel();
    }
}

pub(super) fn check_cancelled(flag: &AtomicBool) -> Result<()> {
    if flag.load(Ordering::Acquire) {
        Err(Error::Process {
            kind: "cancelled",
            message: "MCP registration cancelled".into(),
        })
    } else {
        Ok(())
    }
}

pub(super) fn run<T>(
    flag: Arc<AtomicBool>,
    register: impl FnOnce(&dyn Fn() -> Result<()>) -> Result<T>,
) -> Result<T> {
    let check = || check_cancelled(flag.as_ref());
    check()?;
    let result = register(&check);
    check()?;
    result
}

#[cfg(all(test, loom))]
mod tests {
    use super::*;
    use loom::sync::{atomic::AtomicUsize, mpsc};

    fn cancellation_model(omit_guard: bool) {
        loom::model(move || {
            let flag = Arc::new(AtomicBool::new(false));
            // Negative control removes the production cancellation guard.
            let guard = (!omit_guard).then(|| CancelOnDrop(Arc::clone(&flag)));
            let active = Arc::new(AtomicUsize::new(0));
            struct Connection(Arc<AtomicUsize>);
            impl Drop for Connection {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::AcqRel);
                }
            }
            let connections = Arc::new(AtomicUsize::new(1));
            let connection = Connection(Arc::clone(&connections));
            let (send, receive) = mpsc::channel();
            let worker_flag = Arc::clone(&flag);
            let worker_active = Arc::clone(&active);
            let worker = loom::thread::spawn(move || {
                worker_active.fetch_add(1, Ordering::AcqRel);
                let result = run(worker_flag, move |check| {
                    check()?;
                    Ok(connection)
                });
                worker_active.fetch_sub(1, Ordering::AcqRel);
                assert!(send.send(result).is_ok());
            });
            drop(guard);
            drop(receive.recv().unwrap());
            worker.join().unwrap();
            assert!(
                check_cancelled(flag.as_ref()).is_err(),
                "cancellation must be visible"
            );
            assert_eq!(active.load(Ordering::Acquire), 0);
            assert_eq!(connections.load(Ordering::Acquire), 0);
        });
    }

    #[test]
    fn cancellation_and_completion_always_retire_the_worker_and_connection() {
        cancellation_model(false);
    }

    #[test]
    #[should_panic(expected = "cancellation must be visible")]
    fn negative_control_missing_guard_is_detected() {
        cancellation_model(true);
    }
}
