//! One host demand boundary for original journal requests.
use super::*;

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: crate::store::JournalProvider + ?Sized,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Diagnostics cannot replace an original result or its cause. Unretained
    /// operation failures leave their published start unfinished; no synthetic
    /// cause or successful terminal is written. Request custody stays in the
    /// caller's original future, including native work and cancellation.
    pub(super) async fn observe_request<T, E>(
        &self,
        request: acyclic_stream::trace::Request,
        work: impl std::future::Future<Output = std::result::Result<T, E>>,
        bytes: impl FnOnce(&T) -> Option<u64>,
    ) -> std::result::Result<T, E> {
        match &self.traces {
            Some((traces, owner)) => {
                traces
                    .observe(owner.as_str(), request, work, bytes)
                    .await
                    .result
            }
            None => work.await,
        }
    }
}
