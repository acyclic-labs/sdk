use super::*;

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> Transaction<A, O> {
    /// Clones one immutable logical range without reading content bytes.
    pub async fn clone_range<W: crate::facade::FsWork>(
        &mut self,
        request: crate::FileCloneRequest<&str>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let crate::FileCloneRequest {
            source,
            source_offset,
            destination,
            destination_offset,
            length,
        } = request;
        self.mutate_pair(
            source,
            destination,
            |source, destination| {
                AuthoredMutation::CloneRange(crate::FileCloneRequest {
                    source,
                    source_offset,
                    destination,
                    destination_offset,
                    length,
                })
            },
            work,
            cancellation,
        )
        .await
    }
}
