use super::*;

/// Immutable ignore-aware capture and its represented paths.
pub struct GitCapturedGeneration<A: 'static, O: 'static> {
    /// Captured compatibility generation.
    pub generation: Generation<A, O>,
    /// Eligible non-directory paths.
    pub tracked_paths: BTreeSet<String>,
    pub(super) origin: Option<(Generation<A, O>, GenerationId)>,
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> GitCapturedGeneration<A, O> {
    /// Authenticates the capture's SDK fork and registers its durable lineage.
    pub async fn authenticated_proof<S: crate::WorkspaceLineageStore, W: crate::facade::FsWork>(
        &self,
        operation_id: OperationId,
        graph: &crate::WorkspaceGraph<S>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<GitCaptureProof>, GitCaptureAuthenticationError<S::Error>> {
        let mut accounting = work.accounting(cancellation);
        let result: Result<Option<GitCaptureProof>, GitCaptureAuthenticationError<S::Error>> =
            async {
                accounting
                    .check()
                    .map_err(WorkspaceError::from)
                    .map_err(GitCaptureError::from)?;
                let Some((parent, initial_generation)) = &self.origin else {
                    return Ok(None);
                };
                let child = self.generation.workspace();
                let mut suffix = [b'-'; 33];
                accounting
                    .perform(
                        crate::WorkCounters {
                            bytes_encoded: 32,
                            ..crate::WorkCounters::UNCHARGED
                        },
                        || hex::encode_to_slice(operation_id.into_bytes(), &mut suffix[1..]),
                    )
                    .map_err(WorkspaceError::from)
                    .map_err(GitCaptureError::from)?
                    .map_err(|_| GitCaptureError::Conflict)?;
                if !child.name().as_str().starts_with("git-capture-")
                    || !child.name().as_str().as_bytes().ends_with(&suffix)
                {
                    return Err(GitCaptureError::Conflict.into());
                }
                if self.generation.id() != *initial_generation {
                    let parents = self
                        .generation
                        .parents_accounted(&mut accounting)
                        .await
                        .map_err(GitCaptureError::from)?;
                    if parents.0.as_slice() != [*initial_generation] {
                        return Err(GitCaptureError::Conflict.into());
                    }
                }
                graph
                    .register_child_accounted(
                        &parent.workspace(),
                        &child,
                        parent.id(),
                        crate::model::GenerationSelector::Exact(*initial_generation),
                        &mut accounting,
                    )
                    .await?;
                Ok(Some(GitCaptureProof {
                    fork_parent: GitTreeRef::exact(parent.workspace_id(), parent.id()),
                    initial_generation: *initial_generation,
                    operation_id,
                }))
            }
            .await;
        work.finish(result, &accounting)
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
