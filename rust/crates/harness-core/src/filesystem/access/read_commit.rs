use super::*;

/// The selected data, policy and authenticated identity cannot be replaced or
/// extracted. A read consumes this observation and fences its returned bytes.
pub(in crate::filesystem) struct ReadObservation<'a, A, O> {
    workspace: Workspace<A, O>,
    generation: acyclic_fs::Generation<A, O>,
    view: CompiledAccess<'a>,
    reference: &'a FileRef,
    io: &'a PreparationIo,
    data_access: Option<(Workspace<A, O>, CompiledAccess<'a>)>,
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
            data_access: None,
        }
    }

    pub(super) fn data_workspace_id(&self) -> acyclic_fs::WorkspaceId {
        self.generation.workspace_id()
    }

    pub(super) async fn authorize_data(
        &mut self,
        scope: &Scope,
        ceiling: Option<&crate::Capabilities>,
    ) -> Result<()> {
        let workspace = self.generation.workspace();
        let label = crate::core::filesystem_scope_label(
            scope.capabilities(),
            &mut Some(&mut self.io.history_io()),
        )?;
        let compiled = access_view(
            &workspace,
            scope,
            Some(self.generation.id()),
            capability_payload(label, VolumeOperation::Read, self.io)?,
            ceiling,
            self.io,
        )
        .await?;
        require(&compiled.0, Action::Read, self.reference.path(), self.io)?;
        self.data_access = Some((workspace, compiled));
        Ok(())
    }

    async fn confirm(&self) -> Result<()> {
        require_current(&self.workspace, &self.view.0, self.io).await?;
        if let Some((workspace, view)) = &self.data_access {
            require_current(workspace, &view.0, self.io).await?;
        }
        Ok(())
    }

    pub(in crate::filesystem) async fn read_file(
        self,
        maximum_bytes: u64,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let io = self.io;
        #[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
        acyclic_differential::record(("read", maximum_bytes));
        let path = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(format_args!("/{}", self.reference.path()), work)
        })?;
        let _path_hold = io.guard_owned(path.capacity() as u64)?;
        let bytes = read_generation(&self.generation, &path, maximum_bytes, Some(io)).await?;
        self.reference
            .descriptor()
            .verify(&bytes, &mut Some(&mut io.history_io()))?;
        self.confirm().await?;
        fork_preparer_io::retain(&self.generation, Some(io)).await?;
        self.confirm().await?;
        Ok(bytes)
    }

    pub(in crate::filesystem) async fn read_range(
        self,
        range: acyclic_fs::ByteRange,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let bytes = FilesystemHost::read_selected_range(
            &self.generation.workspace(),
            &self.generation,
            self.reference,
            range,
            self.io,
        )
        .await?;
        self.confirm().await?;
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
        // A concurrent data publication can overtake the policy observation.
        // Retry only that accounted observation, retaining the original bytes
        // and decision. Each attempt still checks cancellation and its budget.
        let (generation, revision) = loop {
            if let Some(current) = io.sdk(
                workspace
                    .current_access_policy_revision(io, io.cancellation())
                    .await,
            )? {
                break current;
            }
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
