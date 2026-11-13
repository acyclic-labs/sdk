use crate::StreamError;
use acyclic_collections::{FixedIndex, IndexWork};
use std::{
    future::poll_fn,
    task::{Context, Poll, Waker},
};

#[path = "change/sync.rs"]
pub(crate) mod sync;
use sync::{Arc, AtomicUsize, Mutex, Ordering};

struct State {
    revision: Option<u64>,
    next_id: u64,
    waiters: FixedIndex<Option<Waker>, (), 8>,
}
struct Shared {
    producers: AtomicUsize,
    inner: Mutex<State>,
}
pub(crate) struct Change {
    // Lock order: one lock, never nested. Snapshot polling, waker cloning,
    // destruction and notification all happen outside this lock.
    state: Arc<Shared>,
}
pub(crate) struct Subscription {
    state: Arc<Shared>,
    id: u64,
    seen: u64,
}
impl Change {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(Shared {
                producers: AtomicUsize::new(1),
                inner: Mutex::new(State {
                    revision: Some(0),
                    next_id: 0,
                    waiters: FixedIndex::default(),
                }),
            }),
        }
    }
    pub(crate) fn subscribe(&self) -> Result<Subscription, StreamError> {
        let mut state = self
            .state
            .inner
            .lock()
            .map_err(|_| StreamError::Unavailable)?;
        let id = state.next_id;
        let next_id = id.checked_add(1).ok_or(StreamError::Capacity)?;
        let seen = state.revision.ok_or(StreamError::Capacity)?;
        state.next_id = next_id;
        Ok(Subscription {
            state: Arc::clone(&self.state),
            id,
            seen,
        })
    }
    pub(crate) fn notify(&self) -> Result<(), StreamError> {
        let (revision, waiters) = {
            let mut state = self
                .state
                .inner
                .lock()
                .map_err(|_| StreamError::Unavailable)?;
            state.revision = state.revision.and_then(|revision| revision.checked_add(1));
            (state.revision, std::mem::take(&mut state.waiters))
        };
        dispatch(waiters);
        revision.map(|_| ()).ok_or(StreamError::Capacity)
    }
}
// Subscribers retain the event state, never its producer. In particular, a
// detached journal wait must not keep its vanished notification source alive.
impl Clone for Change {
    fn clone(&self) -> Self {
        self.state.producers.fetch_add(1, Ordering::Relaxed);
        Self {
            state: Arc::clone(&self.state),
        }
    }
}
impl Drop for Change {
    fn drop(&mut self) {
        if self.state.producers.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        let waiters = {
            let mut state = self
                .state
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut state.waiters)
        };
        dispatch(waiters);
    }
}
// Both publication and final producer retirement transfer wake custody outside
// the original lock; callbacks may immediately re-enter the event.
fn dispatch(mut waiters: FixedIndex<Option<Waker>, (), 8>) {
    let extracted = waiters
        .extract_prefix(&[], |_| {})
        .expect("empty prefix fits every key");
    for entry in extracted.into_entries(|_| {}) {
        let (_, wake, ()) = entry.into_parts();
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}

// Registration nodes are unmetered host observation metadata, shared by the
// subscriptions of this event, with no configured admission limit. Radix
// extraction keeps each waker alive until after the lock is released;
// retirement never allocates or runs a waker under that lock.
struct RegistrationWork;
impl IndexWork for RegistrationWork {
    type Error = StreamError;
    type Allocation = ();
    fn check(&mut self) -> Result<(), StreamError> {
        Ok(())
    }
    fn charge_items(&mut self, _: u64) -> Result<(), StreamError> {
        Ok(())
    }
    fn claim(&mut self, _: u64) -> Result<(), StreamError> {
        Ok(())
    }
}
impl Subscription {
    #[cfg(any(all(feature = "wasm", target_arch = "wasm32"), loom))]
    fn mark(&mut self) -> Result<(), StreamError> {
        let previous = {
            let mut state = self
                .state
                .inner
                .lock()
                .map_err(|_| StreamError::Unavailable)?;
            if self.state.producers.load(Ordering::Acquire) == 0 {
                return Err(StreamError::Unavailable);
            }
            self.seen = state.revision.ok_or(StreamError::Capacity)?;
            state.waiters.remove_if(self.id.to_be_bytes(), |_| true)
        };
        drop(previous);
        Ok(())
    }
    fn poll_changed(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), StreamError>> {
        let wake = cx.waker().clone();
        let (previous, result) = {
            let Ok(mut state) = self.state.inner.lock() else {
                return Poll::Ready(Err(StreamError::Unavailable));
            };
            if self.state.producers.load(Ordering::Acquire) == 0 {
                return Poll::Ready(Err(StreamError::Unavailable));
            }
            let Some(revision) = state.revision else {
                return Poll::Ready(Err(StreamError::Capacity));
            };
            if revision != self.seen {
                self.seen = revision;
                (
                    state
                        .waiters
                        .remove_if(self.id.to_be_bytes(), |_| true)
                        .and_then(|entry| entry.into_parts().1),
                    Poll::Ready(Ok(())),
                )
            } else {
                let entry = match state.waiters.get_or_try_insert_with(
                    self.id.to_be_bytes(),
                    &mut RegistrationWork,
                    |_| Ok(None),
                ) {
                    Ok(entry) => entry,
                    Err(error) => return Poll::Ready(Err(error)),
                };
                (entry.replace(wake), Poll::Pending)
            }
        };
        drop(previous);
        result
    }
    pub(crate) async fn changed(&mut self) -> Result<(), StreamError> {
        poll_fn(|cx| self.poll_changed(cx)).await
    }
    #[cfg(any(all(feature = "wasm", target_arch = "wasm32"), loom))]
    pub(crate) async fn next<T>(
        &mut self,
        mut snapshot: impl AsyncFnMut() -> Result<Option<T>, StreamError>,
    ) -> Result<T, StreamError> {
        loop {
            // Mark before the snapshot await: a publication during the read
            // must either be visible in that snapshot or wake its successor.
            self.mark()?;
            if let Some(value) = snapshot().await? {
                return Ok(value);
            }
            self.changed().await?;
        }
    }
}
impl Drop for Subscription {
    fn drop(&mut self) {
        let previous = self
            .state
            .inner
            .lock()
            .ok()
            .and_then(|mut state| state.waiters.remove_if(self.id.to_be_bytes(), |_| true));
        drop(previous);
    }
}

#[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use std::{future::Future, sync::Mutex as StdMutex, task::Wake};

    struct Reenter {
        state: Arc<Shared>,
        victim: StdMutex<Option<Subscription>>,
        wakes: AtomicUsize,
    }
    impl Wake for Reenter {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            assert!(
                self.state.inner.try_lock().is_ok(),
                "event wake retained its registration lock"
            );
            let victim = self.victim.lock().unwrap().take();
            drop(victim);
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn change_wakes_reentrant_cancellation_and_final_producer_retirement() {
        let event = Change::new();
        let mut subscriber = event.subscribe().unwrap();
        let observer = Arc::new(Reenter {
            state: Arc::clone(&event.state),
            victim: StdMutex::new(Some(event.subscribe().unwrap())),
            wakes: AtomicUsize::new(0),
        });
        let wake = Waker::from(Arc::clone(&observer));
        let mut context = Context::from_waker(&wake);
        {
            let mut waiting = std::pin::pin!(subscriber.changed());
            assert!(waiting.as_mut().poll(&mut context).is_pending());
            event.notify().unwrap();
            assert!(matches!(
                waiting.as_mut().poll(&mut context),
                Poll::Ready(Ok(()))
            ));
        }
        assert_eq!(observer.wakes.load(Ordering::SeqCst), 1);
        let mut waiting = std::pin::pin!(subscriber.changed());
        assert!(waiting.as_mut().poll(&mut context).is_pending());
        drop(event);
        assert_eq!(observer.wakes.load(Ordering::SeqCst), 2);
        assert!(matches!(
            waiting.as_mut().poll(&mut context),
            Poll::Ready(Err(StreamError::Unavailable))
        ));
    }
}

#[cfg(test)]
#[cfg(loom)]
#[path = "../publication-models/change.rs"]
mod models;
