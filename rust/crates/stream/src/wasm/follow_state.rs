//! One cancellation-aware, demand-driven cursor for browser bindings.
use futures::{FutureExt, StreamExt, stream::LocalBoxStream};

/// Terminal ownership of a pull cursor. Source errors are returned by `next`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CursorStatus {
    /// The source is available or one `next` is pending.
    Open,
    /// The source ended normally.
    Completed,
    /// The consumer closed the cursor or abandoned a pending read.
    Closed,
    /// The source or its ownership lock failed.
    Failed,
}

/// A source failure, overlapping pull, or poisoned ownership lock.
#[derive(Debug, Eq, PartialEq)]
pub enum PullError<E> {
    /// The original source error, delivered once.
    Source(E),
    /// Another `next` owns the source; it has not been cancelled.
    Busy,
    /// The ownership lock failed; the source cannot be resumed.
    Poisoned,
}

struct Slot<T, E> {
    stream: Option<LocalBoxStream<'static, Result<T, E>>>,
    status: CursorStatus,
}

/// One item of credit, with no producer task or prefetched item queue.
/// Dropping the source always happens outside the ownership lock.
pub struct PullCursor<T, E> {
    slot: crate::query_sync::SyncMutex<Slot<T, E>>,
    cancel: crate::query_sync::watch::Sender<bool>,
}

impl<T, E> PullCursor<T, E> {
    /// Takes ownership without polling the source.
    pub fn new(stream: LocalBoxStream<'static, Result<T, E>>) -> Self {
        Self {
            slot: crate::query_sync::SyncMutex::new(Slot {
                stream: Some(stream),
                status: CursorStatus::Open,
            }),
            cancel: crate::query_sync::watch::channel(false).0,
        }
    }

    /// Returns the current ownership state, including terminal failures.
    pub fn status(&self) -> CursorStatus {
        self.slot
            .lock()
            .map_or(CursorStatus::Failed, |slot| slot.status)
    }

    /// Pulls one item. Closing wakes this future; dropping it closes the source.
    /// A simultaneous pull is refused without taking credit from the first.
    pub async fn next(&self) -> Result<Option<T>, PullError<E>> {
        let stream = {
            let mut slot = self.slot.lock().map_err(|_| PullError::Poisoned)?;
            if slot.status != CursorStatus::Open {
                return Ok(None);
            }
            slot.stream.take().ok_or(PullError::Busy)?
        };
        let mut active = ActivePull {
            owner: self,
            stream: Some(stream),
        };
        let mut cancellation = self.cancel.subscribe();
        if *cancellation.borrow_and_update() {
            return Ok(None);
        }
        let result = {
            let stream = active.stream.as_mut().expect("active pull owns its source");
            let next = stream.next().fuse();
            let cancelled = cancellation.changed().fuse();
            futures::pin_mut!(next, cancelled);
            match futures::future::select(next, cancelled).await {
                futures::future::Either::Left((item, _)) => Some(item),
                futures::future::Either::Right((_, _)) => None,
            }
        };
        let mut slot = self.slot.lock().map_err(|_| PullError::Poisoned)?;
        if *self.cancel.borrow() {
            return Ok(None);
        }
        match result {
            Some(Some(Ok(item))) => {
                slot.stream = active.stream.take();
                Ok(Some(item))
            }
            Some(Some(Err(error))) => {
                slot.status = CursorStatus::Failed;
                Err(PullError::Source(error))
            }
            Some(None) => {
                slot.status = CursorStatus::Completed;
                Ok(None)
            }
            None => Ok(None),
        }
    }

    /// Closes once and wakes a pending pull. A terminal source keeps its status.
    pub fn close(&self) {
        self.cancel.send_replace(true);
        let stream = self.slot.lock().ok().and_then(|mut slot| {
            if slot.status == CursorStatus::Open {
                slot.status = CursorStatus::Closed;
            }
            slot.stream.take()
        });
        drop(stream);
    }
}

struct ActivePull<'a, T, E> {
    owner: &'a PullCursor<T, E>,
    stream: Option<LocalBoxStream<'static, Result<T, E>>>,
}

impl<T, E> Drop for ActivePull<'_, T, E> {
    fn drop(&mut self) {
        if self.stream.is_some() {
            self.owner.close();
        }
    }
}

#[cfg(all(test, acyclic_stream_loom))]
type FollowFrames = LocalBoxStream<'static, Result<Vec<u8>, crate::StreamError>>;
#[cfg(all(test, acyclic_stream_loom))]
type FollowState = PullCursor<Vec<u8>, crate::StreamError>;

#[cfg(all(test, acyclic_stream_loom))]
#[path = "../../publication-models/cursor.rs"]
mod models;

#[cfg(all(test, not(acyclic_stream_loom)))]
#[path = "pull_tests.rs"]
mod tests;
