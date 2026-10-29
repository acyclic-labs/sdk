//! Shared pull ownership for native and WebAssembly streaming boundaries.

use futures::stream::LocalBoxStream;

/// One pull at a time, with close waking an outstanding pull and dropping its source.
/// No source is polled before `next`; retained state is independent of stream length.
pub struct PullCursor<T, E> {
    // Lock order: slot -> cancel value. Neither survives await, notification
    // or source destruction; close releases the cancel value before the slot.
    pub(crate) stream: crate::query_sync::SyncMutex<Option<LocalBoxStream<'static, Result<T, E>>>>,
    cancel: crate::query_sync::watch::Sender<bool>,
    unavailable: E,
}
impl<T, E: Clone> PullCursor<T, E> {
    /// Takes ownership of the original source and its typed ownership failure.
    pub fn new(stream: LocalBoxStream<'static, Result<T, E>>, unavailable: E) -> Self {
        Self {
            stream: crate::query_sync::SyncMutex::new(Some(stream)),
            cancel: crate::query_sync::watch::channel(false).0,
            unavailable,
        }
    }
    /// Pulls one value; close interrupts the pending source poll.
    pub async fn next(&self) -> Result<Option<T>, E> {
        let Some(mut stream) = self.stream.lock().ok().and_then(|mut slot| slot.take()) else {
            return Ok(None);
        };
        let mut cancellation = self.cancel.subscribe();
        if *cancellation.borrow_and_update() {
            return Ok(None);
        }
        let next = futures::FutureExt::fuse(futures::StreamExt::next(&mut stream));
        let cancelled = futures::FutureExt::fuse(cancellation.changed());
        futures::pin_mut!(next, cancelled);
        match futures::future::select(next, cancelled).await {
            futures::future::Either::Left((Some(Ok(bytes)), _)) => {
                let mut slot = self.stream.lock().map_err(|_| self.unavailable.clone())?;
                if *self.cancel.borrow() {
                    return Ok(None);
                }
                slot.replace(stream);
                Ok(Some(bytes))
            }
            futures::future::Either::Left((Some(Err(error)), _)) => Err(error),
            futures::future::Either::Left((None, _)) | futures::future::Either::Right((_, _)) => {
                Ok(None)
            }
        }
    }
    /// Stops the original source, including an outstanding `next`.
    pub fn close(&self) {
        self.cancel.send_replace(true);
        let stream = self.stream.lock().ok().and_then(|mut slot| slot.take());
        drop(stream);
    }
}

#[cfg(all(test, not(loom)))]
#[path = "pull_tests.rs"]
mod tests;
