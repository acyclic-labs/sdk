use super::Snapshot;
use crate::query_sync::SyncMutex;
use std::{
    future::{Future, poll_fn},
    sync::{Arc, PoisonError},
    task::{Poll, Waker},
};

// Leaf lock: only transfers owned state/results and the waiter registration.
// No provider guard is held here; wake and state destruction happen outside it.
struct Completion<S: Snapshot> {
    value: Option<(S, Option<S::Output>)>,
    waker: Option<Waker>,
    // The consumer is gone: its job must not acquire or keep a lock reservation.
    abandoned: bool,
    // Registered only while the read is pending; completion retires it.
    worker: Option<Waker>,
    #[cfg(acyclic_loom)]
    mutant: super::model::Mutant,
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
        abandoned: false,
        worker: None,
        #[cfg(acyclic_loom)]
        mutant: super::model::mutant(),
    }));
    let job = Job {
        state: Some(state),
        completed: completed.clone(),
        #[cfg(acyclic_loom)]
        discard_on_drop: super::model::discard_on_drop(),
    };
    (
        job,
        Receiver {
            completed,
            satisfied: false,
        },
    )
}

/// The consumer's side. Dropping it before completion cancels the job: an
/// unpolled job never acquires, and a queued one releases its reservation on
/// its next poll instead of finishing a read nobody will receive.
struct Receiver<S: Snapshot> {
    completed: Arc<SyncMutex<Completion<S>>>,
    // A received result leaves nothing to cancel.
    satisfied: bool,
}

impl<S: Snapshot> Future for Receiver<S> {
    type Output = (S, Option<S::Output>);
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        let value = {
            let mut completed = self
                .completed
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let value = completed.value.take();
            if value.is_none() {
                completed.waker = Some(cx.waker().clone());
            }
            value
        };
        match value {
            Some(value) => {
                self.satisfied = true;
                Poll::Ready(value)
            }
            None => Poll::Pending,
        }
    }
}

impl<S: Snapshot> Drop for Receiver<S> {
    fn drop(&mut self) {
        if self.satisfied {
            #[cfg(acyclic_loom)]
            if self
                .completed
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .mutant
                != super::model::Mutant::ArmedAfterCompletion
            {
                return;
            }
            #[cfg(not(acyclic_loom))]
            return;
        }
        let (value, worker) = {
            let mut completed = self
                .completed
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            completed.abandoned = true;
            completed.waker = None;
            #[cfg(acyclic_loom)]
            if completed.mutant == super::model::Mutant::NoWake {
                completed.worker = None;
            }
            (completed.value.take(), completed.worker.take())
        };
        // State destruction and the wake happen outside the leaf lock.
        drop(value);
        if let Some(worker) = worker {
            worker.wake();
        }
    }
}

impl<S: Snapshot> Job<S> {
    pub(super) async fn run(mut self) {
        let completed = self.completed.clone();
        // A job owns its state until it finishes, and it runs once.
        let Some(state) = self.state.as_mut() else {
            return;
        };
        let result = {
            let mut read = std::pin::pin!(state.read());
            // Abandonment is checked before every read poll, so a job whose
            // consumer is gone never enters, or stays queued on, a lock.
            poll_fn(|cx| {
                {
                    let mut completed = completed.lock().unwrap_or_else(PoisonError::into_inner);
                    if completed.abandoned {
                        #[cfg(acyclic_loom)]
                        if completed.mutant == super::model::Mutant::LeakReservation {
                            return Poll::Pending;
                        }
                        return Poll::Ready(None);
                    }
                    completed.worker = Some(cx.waker().clone());
                }
                read.as_mut().poll(cx).map(Some)
            })
            .await
        };
        match result {
            Some(result) => self.finish(Some(result)),
            // Nobody receives the cursor; dropping it never advanced it twice.
            None => drop(self.state.take()),
        }
    }

    fn finish(&mut self, result: Option<S::Output>) {
        let Some(state) = self.state.take() else {
            return;
        };
        let (waker, worker) = {
            let mut completed = self
                .completed
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            completed.value = Some((state, result));
            #[cfg(acyclic_loom)]
            if completed.mutant == super::model::Mutant::ArmedAfterCompletion {
                if let Some(waker) = completed.waker.take() {
                    waker.wake();
                }
                return;
            }
            (completed.waker.take(), completed.worker.take())
        };
        drop(worker);
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
