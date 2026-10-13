use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Transaction<A, O> {
    /// Clones one immutable logical range without reading content bytes.
    pub async fn clone_range<W: crate::facade::FsWork>(
        &mut self,
        source: &str,
        source_offset: u64,
        destination: &str,
        destination_offset: u64,
        length: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
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
