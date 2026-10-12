use super::*;

/// Terminal result of deleting one named workspace head.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkspaceDelete {
    /// The terminal deletion fact became durable.
    Deleted,
    /// The workspace was already terminally deleted.
    AlreadyDeleted,
    /// Another authority operation won.
    Conflict,
    /// The retry identity was previously bound to different input.
    IdempotencyConflict,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
    /// Retains the current generation under a permanent label.
    pub async fn checkpoint(&self, label: impl AsRef<str>) -> Result<(WorkspaceName, Generation<A, O>), WorkspaceError> {
        let cancellation = CancellationToken::new();
        let label = WorkspaceName::new(label, WorkBudget::UNBOUNDED, &cancellation).map_err(|failure| failure.error)?.value;
        let generation = self.head(WorkBudget::UNBOUNDED, &cancellation).await.map_err(|failure| failure.error)?.value;
        let text = label.as_str().to_owned();
        let bytes = text.capacity() as u64;
        self.volume.fs.retain_workspace_generation(&self.volume, generation.id, crate::kernel::RetentionKind::Checkpoint,
            crate::kernel::Owned::new(text, KernelOwnedAllocation::Standalone, bytes), None, WorkBudget::UNBOUNDED, &cancellation).await.map_err(|failure| failure.error)?;
        Ok((label, generation))
    }

    /// Durably pins the current immutable generation.
    pub async fn pin(&self, identity: impl AsRef<str>) -> Result<(WorkspaceName, Generation<A, O>), WorkspaceError> {
        let cancellation = CancellationToken::new();
        self.head(WorkBudget::UNBOUNDED, &cancellation).await.map_err(|failure| failure.error)?.value
            .pin(identity, WorkBudget::UNBOUNDED, &cancellation).await.map_err(|failure| failure.error).map(|receipt| receipt.value)
    }

    /// Permanently deletes this named head using the exact retry identity.
    pub async fn delete(&self, key: IdempotencyKey) -> Result<WorkspaceDelete, WorkspaceError> {
        self.volume.fs.delete_workspace_volume(&self.volume, key.operation_id()).await
    }

    /// Returns this workspace's foreign source when supported.
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[must_use]
    pub fn source(&self) -> Option<&crate::Source<A, O>> { self.source.as_ref() }

    /// Captures all remaining foreign source state.
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    pub async fn seal(&self) -> Result<Generation<A, O>, crate::SourceError> {
        match &self.source {
            Some(source) => source.seal().await,
            None => self.head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value)
                .map_err(|failure| crate::SourceError::Engine(failure.error.to_string())),
        }
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Generation<A, O> {
    /// Durably pins this exact immutable generation under supplied work limits.
    pub async fn pin<W: crate::facade::FsWork>(&self, identity: impl AsRef<str>, work: W, cancellation: &CancellationToken) -> W::Output<(WorkspaceName, Generation<A, O>), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result: Result<(WorkspaceName, Generation<A, O>), WorkspaceError> = async {
            let identity = WorkspaceName::with_accounting(identity.as_ref(), &mut accounting)?;
            let mut label = copy_workspace_label(&identity, &mut accounting)?;
            if accounting.standalone_parts().is_some() { accounting.release_owned(&mut label.allocation, label.bytes)?; }
            let publication = self.publication_closure.as_ref().and_then(|slot| {
                slot.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take()
            });
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                self.workspace.volume.fs.retain_workspace_generation(&self.workspace.volume, self.id, crate::kernel::RetentionKind::Pin, label, publication, original, cancellation).await?;
                return Ok((identity, self.clone()));
            }
            let retained = self.workspace.volume.fs.retain_workspace_generation(&self.workspace.volume, self.id, crate::kernel::RetentionKind::Pin, label, publication, accounting.remaining()?, cancellation).await;
            let (allocations, spent, budget, _) = accounting.standalone_parts().ok_or(crate::WorkError::Overflow)?;
            merge_workspace_backend(retained, spent, allocations.live_bytes())?;
            spent.verify(budget)?;
            Ok((identity, self.clone()))
        }.await;
        work.finish(result, &accounting)
    }
}
