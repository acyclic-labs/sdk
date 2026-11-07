use super::*;

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> Workspace<A, O> {
    /// Reads one complete regular file within the caller's bound.
    pub fn read<W: crate::facade::FsWork>(
        &self,
        path: &str,
        maximum_bytes: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = W::Output<acyclic_stream::OwnedBytes, WorkspaceError>> {
        read_selected(
            self,
            GenerationSelector::Head,
            path,
            maximum_bytes,
            work,
            cancellation,
        )
    }

    /// Reads one exact file range.
    pub fn read_range<W: crate::facade::FsWork>(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = W::Output<crate::kernel::OwnedSourceChunk, WorkspaceError>> {
        read_range(
            self,
            GenerationSelector::Head,
            path,
            crate::ByteRange { offset, length },
            None,
            work,
            cancellation,
        )
    }

    /// Reads complete no-follow metadata for one path.
    pub fn stat<W: crate::facade::FsWork>(
        &self,
        path: &str,
        work: W,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = W::Output<WorkspaceStat, WorkspaceError>> {
        stat(self, GenerationSelector::Head, path, work, cancellation)
    }

    /// Returns one authenticated bounded directory page and its result allocation.
    pub fn list_directory<W: crate::facade::FsWork>(
        &self,
        path: &str,
        after: Option<&LogicalName>,
        maximum_entries: u32,
        work: W,
        cancellation: &CancellationToken,
    ) -> impl Future<
        Output = W::Output<
            (crate::kernel::DirectoryPage, crate::AuthorityAllocation),
            WorkspaceError,
        >,
    > {
        list(
            self,
            GenerationSelector::Head,
            path,
            after,
            maximum_entries,
            work,
            cancellation,
        )
    }

    /// Reads one opaque symbolic-link target.
    pub fn read_symbolic_link<W: crate::facade::FsWork>(
        &self,
        path: &str,
        work: W,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = W::Output<crate::kernel::OwnedSourceChunk, WorkspaceError>> {
        read_link(self, GenerationSelector::Head, path, work, cancellation)
    }

    /// Plans clipped sparse spans without reading file bodies.
    /// Retains the output on the supplied original work; keep its allocation
    /// guard until the returned spans have been destroyed.
    pub fn plan_extents<W: crate::facade::FsWork>(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        maximum_spans: u32,
        work: W,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = W::Output<(WorkspaceExtentPlan, crate::AuthorityAllocation), WorkspaceError>>
    {
        plan(
            self,
            GenerationSelector::Head,
            path,
            offset,
            length,
            maximum_spans,
            work,
            cancellation,
        )
    }
}
