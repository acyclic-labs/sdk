use super::*;

mod execution;
mod publication;
mod transitions;

/// Repository façade over one SDK workspace and private compatibility state.
pub struct GitCompatRepository<S> {
    pub(super) workspace_id: WorkspaceId,
    pub(super) store: S,
    pub(super) request: Option<GitRequest>,
}

/// Whether a request's own record is still retained; its saved result is
/// kept exactly as long.
pub type GitRequestRetention = std::sync::Arc<dyn Fn(OperationId) -> bool + Send + Sync>;

/// The request a command runs as.
#[derive(Clone)]
pub struct GitRequest {
    /// The request's operation, which keys its saved result.
    pub operation: OperationId,
    /// Whether a request's record is still retained.
    pub retained: GitRequestRetention,
}

/// Compatibility history record produced by one distributed publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitPublicationRecord {
    /// Filtered Git-visible tree.
    pub tree: GitTreeRef,
    /// Complete live workspace tree after publication.
    pub workspace_tree: GitTreeRef,
    /// Optional compatibility head from the published child.
    pub source_head: Option<GitCommitId>,
    /// Paths represented by the filtered compatibility tree.
    pub tracked_paths: BTreeSet<String>,
    /// SDK-verified origin for a filtered publication tree.
    pub capture_proof: Option<GitCaptureProof>,
    /// Merge commit message.
    pub message: String,
    /// Author identity.
    pub author: String,
    /// Unix epoch timestamp in seconds.
    pub authored_at_seconds: i64,
}

impl<S> GitCompatRepository<S> {
    /// Opens private compatibility state for one repository.
    pub const fn new(workspace_id: WorkspaceId, store: S) -> Self {
        Self {
            workspace_id,
            store,
            request: None,
        }
    }

    /// Runs commands as `request`: each state-changing command saves its
    /// result under the request that started it, with its effect, and
    /// [`Self::request_result`] returns it on replay.
    #[must_use]
    pub fn for_request(mut self, request: GitRequest) -> Self {
        self.request = Some(request);
        self
    }

    pub(super) fn request_operation(&self) -> Option<OperationId> {
        self.request.as_ref().map(|request| request.operation)
    }

    pub(super) fn retention(&self) -> Option<&GitRequestRetention> {
        self.request.as_ref().map(|request| &request.retained)
    }

    /// Returns the repository's workspace identity.
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }
}

impl<S: GitCompatStore> GitCompatRepository<S> {
    /// The saved result of this repository's request, if it already ran.
    pub async fn request_result(
        &self,
    ) -> Result<Option<GitCommandOutput>, GitCompatError<S::Error>> {
        let Some(operation) = self.request_operation() else {
            return Ok(None);
        };
        Ok(self.load().await?.request(operation).cloned())
    }

    /// Reads the selected workspace and its pending native publication at one durable cut.
    pub async fn checkout_selection(
        &self,
    ) -> Result<(WorkspaceId, Option<(OperationId, GitGenerationRef)>), GitCompatError<S::Error>>
    {
        let state = self.load().await?;
        let workspace = state
            .current()
            .map(|branch| branch.workspace_id)
            .map_err(|_| GitCompatError::InvalidState)?;
        Ok((workspace, state.checkout_reconciliation))
    }

    /// Returns the selected branch's tracked paths.
    pub async fn tracked_paths(&self) -> Result<BTreeSet<String>, GitCompatError<S::Error>> {
        self.load()
            .await?
            .current()
            .map(|branch| branch.tracked_paths.clone())
            .map_err(|_| GitCompatError::InvalidState)
    }

    /// Returns the selected branch's explicit head.
    pub async fn head(&self) -> Result<Option<GitCommitId>, GitCompatError<S::Error>> {
        self.load()
            .await?
            .current()
            .map(|branch| branch.head)
            .map_err(|_| GitCompatError::InvalidState)
    }

    async fn load(&self) -> Result<GitCompatState, GitCompatError<S::Error>> {
        let state = self
            .store
            .load(self.workspace_id)
            .await
            .map_err(GitCompatError::Store)?
            .unwrap_or_else(|| GitCompatState::new("main", self.workspace_id));
        validate_git_state(&state).map_err(|()| GitCompatError::InvalidState)?;
        Ok(state)
    }

    async fn publish(&self, mut state: GitCompatState) -> Result<bool, GitCompatError<S::Error>> {
        let expected = next_revision(&mut state.revision);
        validate_git_state(&state).map_err(|()| GitCompatError::InvalidState)?;
        self.store
            .compare_and_swap(self.workspace_id, expected, state)
            .await
            .map_err(GitCompatError::Store)
    }

    async fn update<T>(
        &self,
        mut edit: impl FnMut(&mut GitCompatState) -> Result<T, GitCompatError<S::Error>>,
    ) -> Result<T, GitCompatError<S::Error>> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let mut state = self.load().await?;
            let output = edit(&mut state)?;
            if self.publish(state).await? {
                return Ok(output);
            }
        }
        Err(GitCompatError::Contended)
    }
}
