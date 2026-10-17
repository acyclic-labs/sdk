use super::Snapshot;
use crate::query_sync::SyncMutex;
use std::{
    future::{Future, poll_fn},
    sync::Arc,
    task::{Poll, Waker},
};

// Leaf lock: only transfers owned state/results and the waiter registration.
// No provider guard is held here; wake and state destruction happen outside it.
struct Completion<S: Snapshot> {
    value: Option<(S, Option<S::Output>)>,
    waker: Option<Waker>,
}

pub(super) struct Job<S: Snapshot> {
    state: Option<S>,
    completed: Arc<SyncMutex<Completion<S>>>,
    #[cfg(acyclic_loom)]
    discard_on_drop: bool,
}

pub(super) fn new<S: Snapshot>(
    state: S,
) -> (Job<S>, impl Future<Output = (S, Option<S::Output>)> + Send) {
    let completed = Arc::new(SyncMutex::new(Completion {
        value: None,
        waker: None,
    }));
    let job = Job {
        state: Some(state),
        completed: completed.clone(),
        #[cfg(acyclic_loom)]
        discard_on_drop: super::model::discard_on_drop(),
    };
    let receiver = poll_fn(move |cx| {
        let mut completed = completed.lock().unwrap();
        if let Some(value) = completed.value.take() {
            Poll::Ready(value)
        } else {
            completed.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    });
    (job, receiver)
}

impl<S: Snapshot> Job<S> {
    pub(super) async fn run(mut self) {
        let result = self.state.as_mut().unwrap().read().await;
        self.finish(Some(result));
    }

    fn finish(&mut self, result: Option<S::Output>) {
        let Some(state) = self.state.take() else {
            return;
        };
        let waker = {
            let mut completed = self.completed.lock().unwrap();
            completed.value = Some((state, result));
            completed.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<S: Snapshot> Drop for Job<S> {
    fn drop(&mut self) {
        #[cfg(acyclic_loom)]
        if self.discard_on_drop {
            return;
        }
        // The awaited read is dropped before this owner, so cancellation has
        // already released any queued/granted permit and has not moved a cursor.
        self.finish(None);
    }
}
