use crate::{Error, Result};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

// No locks: the parent publishes cancellation, then awaits the worker's physical completion.
// The flag trait lets Loom execute the same guard/check path with instrumented atomics.
pub(super) trait CancelFlag {
    fn cancel(&self);
    fn cancelled(&self) -> bool;
}

impl CancelFlag for AtomicBool {
    fn cancel(&self) {
        self.store(true, Ordering::Release);
    }
    fn cancelled(&self) -> bool {
        self.load(Ordering::Acquire)
    }
}

pub(super) struct CancelOnDrop<F: CancelFlag = AtomicBool>(pub(super) Arc<F>);

impl<F: CancelFlag> Drop for CancelOnDrop<F> {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

pub(super) fn check_cancelled(flag: &impl CancelFlag) -> Result<()> {
    if flag.cancelled() {
        Err(Error::process_io(std::io::ErrorKind::Interrupted.into()))
    } else {
        Ok(())
    }
}

pub(super) fn run<T>(
    flag: Arc<impl CancelFlag>,
    register: impl FnOnce(&dyn Fn() -> Result<()>) -> Result<T>,
) -> Result<T> {
    let check = || check_cancelled(flag.as_ref());
    check()?;
    let result = register(&check);
    check()?;
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use loom::sync::{
        atomic::{AtomicBool as ModelBool, AtomicUsize},
        mpsc,
    };

    impl CancelFlag for ModelBool {
        fn cancel(&self) {
            self.store(true, Ordering::Release);
        }
        fn cancelled(&self) -> bool {
            self.load(Ordering::Acquire)
        }
    }

    #[test]
    fn cancellation_and_completion_always_retire_the_worker_and_connection() {
        loom::model(|| {
            let flag = Arc::new(ModelBool::new(false));
            let guard = CancelOnDrop(Arc::clone(&flag));
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
            assert!(check_cancelled(flag.as_ref()).is_err());
            assert_eq!(active.load(Ordering::Acquire), 0);
            assert_eq!(connections.load(Ordering::Acquire), 0);
        });
    }
}
