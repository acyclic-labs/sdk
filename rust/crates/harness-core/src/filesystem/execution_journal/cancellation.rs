//! Runtime-owned delivery cancellation used by execution waits.
use super::*;

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: crate::store::JournalProvider + ?Sized,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn follow_cancellation(&self) -> Result<()> {
        let Some(stop) = &self.stop else {
            return std::future::pending().await;
        };
        stop.cancelled().await;
        Ok(())
    }
}
