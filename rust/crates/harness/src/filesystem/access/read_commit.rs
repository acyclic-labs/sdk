use super::*;

/// The selected data, policy and authenticated identity cannot be replaced or
/// extracted. A read consumes this observation and fences its returned bytes.
pub(in crate::filesystem) struct ReadObservation<'a, A, O> {
    workspace: Workspace<A, O>,
    generation: acyclic_fs::Generation<A, O>,
    view: CompiledAccess<'a>,
    reference: &'a FileRef,
    io: &'a PreparationIo,
}

impl<'a, A: AsyncAuthorityStore, O: AsyncObjectStore> ReadObservation<'a, A, O> {
    pub(super) fn new(
        workspace: Workspace<A, O>,
        generation: acyclic_fs::Generation<A, O>,
        view: CompiledAccess<'a>,
        reference: &'a FileRef,
        io: &'a PreparationIo,
    ) -> Self {
        Self {
            workspace,
            generation,
            view,
            reference,
            io,
        }
    }

    pub(in crate::filesystem) async fn read_file(
        self,
        maximum_bytes: u64,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let io = self.io;
        let path = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(format_args!("/{}", self.reference.path()), work)
        })?;
        let _path_hold = io.guard_owned(path.capacity() as u64)?;
        let bytes = read_generation(&self.generation, &path, maximum_bytes, Some(io)).await?;
        self.reference
            .descriptor()
            .verify(&bytes, &mut Some(&mut io.history_io()))?;
        require_current(&self.workspace, &self.view.0, self.io).await?;
        Ok(bytes)
    }

    pub(in crate::filesystem) async fn read_range(
        self,
        range: acyclic_fs::ByteRange,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let bytes = FilesystemHost::read_selected_range(
            &self.workspace,
            &self.generation,
            self.reference,
            range,
            self.io,
        )
        .await?;
        require_current(&self.workspace, &self.view.0, self.io).await?;
        Ok(bytes)
    }
}

/// Linearize disclosure after storage completes, against the current policy.
/// Unrelated commits are accepted by generation-insensitive views. A policy
/// change or expired capability refuses the result; a new request recompiles.
pub(in crate::filesystem) async fn require_current<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    view: &PathTrie,
    io: &PreparationIo,
) -> Result<()> {
    crate::conversation::BoundedContentFuture::new(Some(io), || async move {
        let observed = io.sdk(
            workspace
                .current_access_policy_revision(io, io.cancellation())
                .await,
        )?;
        let Some((generation, revision)) = observed else {
            return Err(stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Unauthorized,
                "filesystem policy changed during disclosure fence",
            ));
        };
        if !view.matches_policy_revision(generation, revision)
            || view.valid_until().is_some_and(|end| access_now() >= end)
        {
            return Err(stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Unauthorized,
                "filesystem policy changed before disclosure",
            ));
        }
        Ok(())
    })?
    .await
}
