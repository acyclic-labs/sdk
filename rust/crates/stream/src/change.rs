use crate::StreamError;
use std::{
    collections::BTreeMap,
    future::poll_fn,
    task::{Context, Poll, Waker},
};

#[path = "change/sync.rs"]
pub(crate) mod sync;
use sync::{Arc, AtomicUsize, Mutex, Ordering};

struct State {
    revision: Option<u64>,
    next_id: u64,
    waiters: BTreeMap<u64, Waker>,
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
                    waiters: BTreeMap::new(),
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
fn dispatch(waiters: BTreeMap<u64, Waker>) {
    for wake in waiters.into_values() {
        wake.wake();
    }
}
impl Subscription {
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
            state.waiters.remove(&self.id)
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
                (state.waiters.remove(&self.id), Poll::Ready(Ok(())))
            } else {
                (state.waiters.insert(self.id, wake), Poll::Pending)
            }
        };
        drop(previous);
        result
    }
    pub(crate) async fn changed(&mut self) -> Result<(), StreamError> {
        poll_fn(|cx| self.poll_changed(cx)).await
    }
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
            .and_then(|mut state| state.waiters.remove(&self.id));
        drop(previous);
    }
}

#[cfg(test)]
#[cfg(loom)]
#[path = "../publication-models/change.rs"]
mod models;
