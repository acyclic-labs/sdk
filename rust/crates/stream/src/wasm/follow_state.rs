type FollowFrames = futures::stream::LocalBoxStream<'static, Result<Vec<u8>, crate::StreamError>>;
struct FollowState {
    // Lock order: slot -> cancel value. Neither survives await, notification
    // or source destruction; close releases the cancel value before the slot.
    stream: crate::query_sync::SyncMutex<Option<FollowFrames>>,
    cancel: crate::query_sync::watch::Sender<bool>,
}
impl FollowState {
    fn new(stream: FollowFrames) -> Self {
        Self {
            stream: crate::query_sync::SyncMutex::new(Some(stream)),
            cancel: crate::query_sync::watch::channel(false).0,
        }
    }
    async fn next(&self) -> Result<Option<Vec<u8>>, crate::StreamError> {
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
                let mut slot = self
                    .stream
                    .lock()
                    .map_err(|_| crate::StreamError::Unavailable)?;
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
    fn close(&self) {
        self.cancel.send_replace(true);
        let stream = self.stream.lock().ok().and_then(|mut slot| slot.take());
        drop(stream);
    }
}

#[cfg(acyclic_stream_loom)]
#[path = "../../publication-models/cursor.rs"]
mod models;
