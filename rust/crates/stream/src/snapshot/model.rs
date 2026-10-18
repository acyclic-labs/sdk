use std::{cell::RefCell, future::Future, pin::Pin};

type Worker = Pin<Box<dyn Future<Output = ()> + Send>>;

loom::thread_local! {
    // The model owns executor task storage, just as Tokio does. Dropping these
    // futures exercises the production Job's cancellation and ownership return.
    static EXECUTOR: RefCell<Option<(bool, Vec<Worker>)>> = RefCell::new(None);
}

pub(crate) fn capture(discard_on_drop: bool) {
    EXECUTOR.with(|executor| {
        assert!(
            executor
                .borrow_mut()
                .replace((discard_on_drop, Vec::new()))
                .is_none()
        );
    });
}

pub(super) fn discard_on_drop() -> bool {
    EXECUTOR.with(|executor| executor.borrow().as_ref().is_some_and(|entry| entry.0))
}

pub(crate) fn take() -> Worker {
    EXECUTOR.with(|executor| {
        let (_, mut workers) = executor.borrow_mut().take().unwrap();
        assert_eq!(workers.len(), 1);
        workers.pop().unwrap()
    })
}

pub(super) fn spawn(worker: impl Future<Output = ()> + Send + 'static) {
    let worker = EXECUTOR.with(|executor| {
        let mut executor = executor.borrow_mut();
        if let Some((_, workers)) = executor.as_mut() {
            workers.push(Box::pin(worker));
            None
        } else {
            Some(worker)
        }
    });
    if let Some(worker) = worker {
        loom::thread::spawn(move || super::park(worker));
    }
}
