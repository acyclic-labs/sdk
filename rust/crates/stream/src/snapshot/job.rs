use super::Snapshot;
use crate::query_sync::SyncMutex;
use std::{
    future::{Future, poll_fn},
    sync::{Arc, PoisonError},
    task::{Poll, Waker},
};

#[cfg(all(test, not(loom)))]
pub(super) mod inspection;

// Leaf lock: only transfers owned state/results and the waiter registration.
// No provider guard is held here; wake and state destruction happen outside it.
struct Completion<S: Snapshot> {
    value: Option<(S, Result<S::Output, crate::StreamError>)>,
    waker: Option<Waker>,
    // The consumer is gone: its job must not acquire or keep a lock reservation.
    abandoned: bool,
    // Registered only while the read is pending; completion retires it.
    worker: Option<Waker>,
    #[cfg(loom)]
    mutant: super::model::Mutant,
}

pub(super) struct Job<S: Snapshot> {
    state: Option<S>,
    completed: Arc<SyncMutex<Completion<S>>>,
    #[cfg(loom)]
    discard_on_drop: bool,
    #[cfg(all(test, not(loom)))]
    observation: Option<inspection::Observation>,
}

pub(super) fn new<S: Snapshot>(
    state: S,
) -> (
    Job<S>,
    impl Future<Output = (S, Result<S::Output, crate::StreamError>)> + Send,
) {
    let completed = Arc::new(SyncMutex::new(Completion {
        value: None,
        waker: None,
        abandoned: false,
        worker: None,
        #[cfg(loom)]
        mutant: super::model::mutant(),
    }));
    #[cfg(all(test, not(loom)))]
    let observation = {
        let mut observation = inspection::take();
        inspection::notify(&mut observation, &completed, inspection::Cut::Created);
        observation
    };
    let job = Job {
        state: Some(state),
        completed: completed.clone(),
        #[cfg(loom)]
        discard_on_drop: super::model::discard_on_drop(),
        #[cfg(all(test, not(loom)))]
        observation,
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
    type Output = (S, Result<S::Output, crate::StreamError>);
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        let waker = cx.waker().clone();
        let (value, previous) = {
            let mut completed = self
                .completed
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let value = completed.value.take();
            let previous = if value.is_none() {
                completed.waker.replace(waker)
            } else {
                None
            };
            (value, previous)
        };
        drop(previous);
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
            #[cfg(loom)]
            if self
                .completed
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .mutant
                != super::model::Mutant::ArmedAfterCompletion
            {
                return;
            }
            #[cfg(not(loom))]
            return;
        }
        #[cfg(loom)]
        let skip_wake;
        let (value, waker, worker) = {
            let mut completed = self
                .completed
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            completed.abandoned = true;
            #[cfg(loom)]
            {
                skip_wake = completed.mutant == super::model::Mutant::NoWake;
            }
            (
                completed.value.take(),
                completed.waker.take(),
                completed.worker.take(),
            )
        };
        // State destruction and the wake happen outside the leaf lock.
        drop(value);
        drop(waker);
        #[cfg(loom)]
        if skip_wake {
            return;
        }
        if let Some(worker) = worker {
            worker.wake();
        }
    }
}

impl<S: Snapshot> Job<S> {
    pub(super) async fn run(mut self) {
        let completed = self.completed.clone();
        #[cfg(all(test, not(loom)))]
        let observation = &mut self.observation;
        // A job owns its state until it finishes, and it runs once.
        let Some(state) = self.state.as_mut() else {
            return;
        };
        let result = {
            let mut read = std::pin::pin!(state.read());
            // Abandonment is checked before every read poll, so a job whose
            // consumer is gone never enters, or stays queued on, a lock.
            poll_fn(|cx| {
                let waker = cx.waker().clone();
                let previous = {
                    let mut completed = completed.lock().unwrap_or_else(PoisonError::into_inner);
                    if completed.abandoned {
                        #[cfg(loom)]
                        if completed.mutant == super::model::Mutant::LeakReservation {
                            return Poll::Pending;
                        }
                        return Poll::Ready(None);
                    }
                    completed.worker.replace(waker)
                };
                drop(previous);
                let result = read.as_mut().poll(cx).map(Some);
                #[cfg(all(test, not(loom)))]
                if result.is_pending() {
                    inspection::notify(observation, &completed, inspection::Cut::Pending);
                }
                result
            })
            .await
        };
        match result {
            Some(result) => self.finish(result),
            // Nobody receives the cursor; dropping it never advanced it twice.
            None => drop(self.state.take()),
        }
    }

    fn finish(&mut self, result: Result<S::Output, crate::StreamError>) {
        let Some(state) = self.state.take() else {
            return;
        };
        let (waker, worker) = {
            let mut completed = self
                .completed
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            completed.value = Some((state, result));
            let worker = {
                #[cfg(loom)]
                {
                    if completed.mutant == super::model::Mutant::ArmedAfterCompletion {
                        None
                    } else {
                        completed.worker.take()
                    }
                }
                #[cfg(not(loom))]
                completed.worker.take()
            };
            (completed.waker.take(), worker)
        };
        drop(worker);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<S: Snapshot> Drop for Job<S> {
    fn drop(&mut self) {
        #[cfg(loom)]
        if self.discard_on_drop {
            return;
        }
        // The awaited read is dropped before this owner, so cancellation has
        // already released any queued/granted permit and has not moved a cursor.
        self.finish(Err(crate::StreamError::Unavailable));
        #[cfg(all(test, not(loom)))]
        inspection::notify(
            &mut self.observation,
            &self.completed,
            inspection::Cut::Retired,
        );
    }
}
