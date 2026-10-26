//! An agent turn's stop signal, followed during cancellable execution waits.
use super::*;

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn follow_cancellation(&self) -> Result<()> {
        let Some(stop) = &self.stop else {
            return std::future::pending().await;
        };
        stop.follow(&*self.stream)
            .await
            .map_err(|error| Error::Storage(error.to_string()))
    }
}
