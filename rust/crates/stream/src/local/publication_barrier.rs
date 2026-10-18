use super::*;

impl LocalStream {
    /// Holds the actual publication writer until an explicit test handshake.
    #[doc(hidden)]
    pub async fn test_publication_barrier(
        &self,
        entered: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::oneshot::Receiver<()>,
    ) {
        let _publication = self.inner.publication.write().await;
        let _ = entered.send(());
        let _ = release.await;
    }

    /// Detects an outstanding read permit without waiting or a timeout.
    #[doc(hidden)]
    pub fn test_publication_available(&self) -> bool {
        self.inner.publication.try_write().is_ok()
    }
}
