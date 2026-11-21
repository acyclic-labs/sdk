use super::*;

impl LocalStream {
    /// Holds the actual publication writer until an explicit test handshake.
    #[doc(hidden)]
    pub async fn test_publication_barrier(
        &self,
        entered: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::oneshot::Receiver<()>,
    ) {
        let stream = self.worker();
        // The explicit release signal controls the test, even while the
        // future observing this barrier is parked.
        let _ = spawn(async move {
            let _publication = stream.inner.publication.write().await;
            let _ = entered.send(());
            let _ = release.await;
        })
        .await;
    }
}
