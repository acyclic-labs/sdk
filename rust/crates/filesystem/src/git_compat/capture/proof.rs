use super::*;

/// Immutable ignore-aware capture and its represented paths.
pub struct GitCapturedGeneration<A, O> {
    /// Captured compatibility generation.
    pub generation: Generation<A, O>,
    /// Eligible non-directory paths.
    pub tracked_paths: BTreeSet<String>,
    pub(super) origin: Option<(Generation<A, O>, GenerationId)>,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> GitCapturedGeneration<A, O> {
    /// Authenticates the capture's SDK fork and registers its durable lineage.
    pub async fn authenticated_proof<S: crate::WorkspaceLineageStore>(
        &self,
        operation_id: OperationId,
        graph: &crate::WorkspaceGraph<S>,
    ) -> Result<Option<GitCaptureProof>, GitCaptureAuthenticationError<S::Error>> {
        let Some((parent, initial_generation)) = &self.origin else {
            return Ok(None);
        };
        let child = self.generation.workspace();
        let suffix = format!("-{}", hex::encode(operation_id.into_bytes()));
        if !child.name().as_str().starts_with("git-capture-")
            || !child.name().as_str().ends_with(&suffix)
        {
            return Err(GitCaptureError::Conflict.into());
        }
        let expected = parent
            .workspace()
            .volume
            .fs
            .workspace_id(child.name().as_str())
            .map_err(WorkspaceError::from)
            .map_err(GitCaptureError::from)?;
        if child.id() != expected {
            return Err(GitCaptureError::Conflict.into());
        }
        let initial = child
            .generation(
                *initial_generation,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| GitCaptureError::from(failure.error))?
            .value;
        if initial
            .parents(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| GitCaptureError::from(failure.error))?
            .value
            .as_slice()
            != [parent.id()]
        {
            return Err(GitCaptureError::Conflict.into());
        }
        let parents = self
            .generation
            .parents(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| GitCaptureError::from(failure.error))?
            .value;
        if self.generation.id() != *initial_generation
            && parents.as_slice() != [*initial_generation]
        {
            return Err(GitCaptureError::Conflict.into());
        }
        graph
            .register_existing_child_at_generation(
                &parent.workspace(),
                &child,
                parent.id(),
                *initial_generation,
            )
            .await?;
        Ok(Some(GitCaptureProof {
            fork_parent: GitTreeRef::exact(parent.workspace_id(), parent.id()),
            initial_generation: *initial_generation,
            operation_id,
        }))
    }
}

/// Failure while producing an ignore-aware compatibility snapshot.
#[derive(Debug, Error)]
pub enum GitCaptureError {
    /// Workspace traversal, mutation, or publication failed.
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    /// A host-native name cannot be represented by Git's portable UTF-8 paths.
    #[error("Git compatibility cannot represent a non-UTF-8 path name")]
    NonPortableName,
    /// A deterministic capture workspace was concurrently changed.
    #[error("Git compatibility capture workspace changed concurrently")]
    Conflict,
}

/// Failure to authenticate and durably register one filtered Git capture.
#[derive(Debug, Error)]
pub enum GitCaptureAuthenticationError<E: std::error::Error + 'static> {
    /// The immutable captured generation did not match its SDK fork.
    #[error(transparent)]
    Capture(#[from] GitCaptureError),
    /// The direct-parent lineage could not be durably registered.
    #[error(transparent)]
    Lineage(#[from] crate::WorkspaceLineageError<E>),
}
