use super::*;

/// Immutable complete filesystem state belonging to one workspace.
pub struct Generation<A, O> {
    pub(crate) workspace: Workspace<A, O>,
    pub(crate) id: GenerationId,
    pub(crate) publication_closure: Option<crate::facade::PublicationClosure<A, O>>,
}

impl<A, O> Clone for Generation<A, O> {
    fn clone(&self) -> Self {
        Self { workspace: self.workspace.clone(), id: self.id, publication_closure: self.publication_closure.clone() }
    }
}

impl<A, O> Generation<A, O> {
    /// Content-addressed generation identity.
    #[must_use]
    pub const fn id(&self) -> GenerationId { self.id }

    /// Owning workspace identity.
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId { self.workspace.id }

    /// Returns the owning workspace capability.
    #[must_use]
    pub fn workspace(&self) -> Workspace<A, O> { self.workspace.clone() }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Generation<A, O> {
    /// Verifies the exact frozen selection under supplied work limits.
    pub async fn matches_fork_selection<W: crate::facade::FsWork>(
        &self, source: &Self, paths: Option<Vec<String>>, work: W, cancellation: &CancellationToken,
    ) -> W::Output<bool, WorkspaceError> {
        self.workspace.volume.fs.verify_fork_selection(source, self, paths, work, cancellation).await
    }

    /// Computes the normalized immutable second-parent identity.
    pub async fn normalized_join_parent_for(&self, target: &Self) -> Result<GenerationId, WorkspaceError> {
        let cancellation = CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        self.workspace.volume.fs.workspace_normalized_join_parent(self, target, &mut accounting, &cancellation).await
    }

    /// Exact immutable parent generation identities.
    pub async fn parents(&self, budget: WorkBudget, cancellation: &CancellationToken) -> crate::MeasuredResult<crate::OperationReceipt<Vec<GenerationId>>, WorkspaceError> {
        self.workspace.volume.fs.generation_parents(&self.workspace.volume, self.id, budget, cancellation).await
            .map(|receipt| crate::OperationReceipt { value: receipt.value.0, work: receipt.work })
    }

    pub(crate) fn publication_retained_bytes(&self) -> u64 {
        self.publication_closure.as_ref().map_or(0, |slot| {
            let publication = slot.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::size_of::<std::sync::Mutex<Option<crate::facade::VerifiedPublication<A, O>>>>() as u64
                + publication.as_ref().map_or(0, crate::facade::VerifiedPublication::retained_bytes)
        })
    }
}
