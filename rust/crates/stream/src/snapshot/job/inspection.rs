//! One original snapshot-job cut for deterministic unit-test observations.
use futures::channel::oneshot;
use std::cell::RefCell;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Cut {
    Created,
    Pending,
    Retired,
}

pub(super) struct Observation {
    cut: Cut,
    sender: oneshot::Sender<Probe>,
}

/// Inspects the original job mutex without retaining the job or its state.
pub(crate) struct Probe(Box<dyn Fn() -> bool + Send + Sync>);
impl Probe {
    pub(crate) fn unlocked(&self) -> bool {
        (self.0)()
    }
}

thread_local! {
    static NEXT: RefCell<Option<Observation>> = const { RefCell::new(None) };
}

pub(crate) fn observe<T>(cut: Cut, operation: impl FnOnce() -> T) -> (T, oneshot::Receiver<Probe>) {
    let (sender, receiver) = oneshot::channel();
    NEXT.with(|slot| {
        let mut slot = slot.borrow_mut();
        assert!(slot.is_none(), "a snapshot observation is already armed");
        *slot = Some(Observation { cut, sender });
    });
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            drop(take());
        }
    }
    let reset = Reset;
    let output = operation();
    drop(reset);
    (output, receiver)
}

pub(super) fn take() -> Option<Observation> {
    NEXT.with(|slot| slot.borrow_mut().take())
}

pub(super) fn notify<S: super::Snapshot>(
    observation: &mut Option<Observation>,
    completed: &std::sync::Arc<super::SyncMutex<super::Completion<S>>>,
    cut: Cut,
) {
    if observation
        .as_ref()
        .is_some_and(|current| current.cut == cut)
        && let Some(observation) = observation.take()
    {
        let completed = std::sync::Arc::downgrade(completed);
        let _ = observation.sender.send(Probe(Box::new(move || {
            completed
                .upgrade()
                .is_none_or(|completed| completed.try_lock().is_ok())
        })));
    }
}
