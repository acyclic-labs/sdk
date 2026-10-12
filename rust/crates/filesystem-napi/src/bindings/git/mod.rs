use super::*;

/// Native JSON boundary for the storage-neutral Git compatibility state machine.
#[napi]
pub struct NativeGitCompatRepository {
    pub(in crate::bindings) inner: GitCompatRepository<LocalCoreStateStore>,
    pub(in crate::bindings) workspace_id: WorkspaceId,
}

pub(in crate::bindings) fn reject_unverified_git_capture(
    result: &GitFilesystemResult,
) -> Result<()> {
    if matches!(result, GitFilesystemResult::Captured { proof: Some(_), .. }) {
        return Err(Error::from_reason(
            "filtered Git captures require an SDK-authenticated filesystem executor; JSON cannot supply capture authority",
        ));
    }
    Ok(())
}

#[napi]
impl NativeGitCompatRepository {
    /// Opens compatibility state in a private companion namespace.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "N-API owns JavaScript strings and buffers at the ABI boundary"
    )]
    #[napi(factory)]
    pub fn open(state_root: String, workspace_id: Buffer) -> Result<Self> {
        let workspace_id = WorkspaceId::from_bytes(fixed_16(&workspace_id)?);
        Ok(Self {
            inner: GitCompatRepository::new(workspace_id, LocalCoreStateStore::new(state_root)),
            workspace_id,
        })
    }

    /// Parses and executes a Git-shaped argv command, returning stable JSON.
    #[napi]
    pub async fn execute_argv_json(
        &self,
        argv: Vec<String>,
        workspace_generation: Buffer,
        default_author: String,
        now_seconds: String,
    ) -> Result<String> {
        let output = self
            .inner
            .execute_argv(
                &argv,
                GitTreeRef::exact(self.workspace_id, generation_id(&workspace_generation)?),
                &default_author,
                now_seconds.parse::<i64>().map_err(napi_error)?,
            )
            .await
            .map_err(napi_error)?;
        serde_json::to_string(&output).map_err(napi_error)
    }

    /// Executes one typed command encoded with the public Rust serde contract.
    #[napi]
    pub async fn execute_json(
        &self,
        command_json: String,
        workspace_generation: Buffer,
    ) -> Result<String> {
        let command: GitCommand = serde_json::from_str(&command_json).map_err(napi_error)?;
        let output = self
            .inner
            .execute(command, GitTreeRef::exact(self.workspace_id, generation_id(&workspace_generation)?))
            .await
            .map_err(napi_error)?;
        serde_json::to_string(&output).map_err(napi_error)
    }

    /// Executes a natural JavaScript Git command through the Rust projection.
    #[napi]
    pub async fn execute_public_json(
        &self,
        command_json: String,
        workspace_generation: Buffer,
    ) -> Result<String> {
        let command = parse_git_public_command(&command_json).map_err(napi_error)?;
        let output = self
            .inner
            .execute(command, GitTreeRef::exact(self.workspace_id, generation_id(&workspace_generation)?))
            .await
            .map_err(napi_error)?;
        serde_json::to_string(&output).map_err(napi_error)
    }

    /// Validates and canonicalizes a Rust Git output before JS projection.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "N-API requires owned JavaScript strings"
    )]
    #[napi]
    pub fn canonicalize_output_json(&self, value_json: String) -> Result<String> {
        canonicalize_git_output_json(&value_json).map_err(napi_error)
    }

    /// Validates and canonicalizes a durable pending transition before JS
    #[allow(
        clippy::needless_pass_by_value,
        reason = "N-API requires owned JavaScript strings"
    )]
    #[napi]
    pub fn canonicalize_pending_transition_json(&self, value_json: String) -> Result<String> {
        canonicalize_git_pending_transition_json(&value_json).map_err(napi_error)
    }

    /// Returns any crash-recoverable prepared transition as stable JSON.
    #[napi]
    pub async fn pending_transition_json(&self) -> Result<Option<String>> {
        self.inner
            .pending_transition()
            .await
            .map_err(napi_error)?
            .map(|pending| serde_json::to_string(&pending).map_err(napi_error))
            .transpose()
    }

    /// Finalizes a prepared transition after its filesystem action succeeds.
    #[napi]
    pub async fn complete_transition_json(
        &self,
        transition: Buffer,
        resulting_generation: Option<Buffer>,
    ) -> Result<String> {
        let generation = resulting_generation
            .as_ref()
            .map(|value| generation_id(value))
            .transpose()?;
        let tree = generation.map(|generation| GitTreeRef::exact(self.workspace_id, generation));
        let output = self
            .inner
            .complete_transition_result(GitTransitionId::from_bytes(fixed_16(&transition)?), &GitFilesystemResult::Applied { tree, tracked_paths: None })
            .await
            .map_err(napi_error)?;
        serde_json::to_string(&output).map_err(napi_error)
    }

    /// Finalizes a prepared transition with the executor's complete typed result.
    #[napi]
    pub async fn complete_transition_result_json(
        &self,
        transition: Buffer,
        result_json: String,
    ) -> Result<String> {
        let result: GitFilesystemResult = serde_json::from_str(&result_json).map_err(napi_error)?;
        reject_unverified_git_capture(&result)?;
        let output = self
            .inner
            .complete_transition_result(
                GitTransitionId::from_bytes(fixed_16(&transition)?),
                &result,
            )
            .await
            .map_err(napi_error)?;
        serde_json::to_string(&output).map_err(napi_error)
    }

    /// Aborts a prepared transition after filesystem rollback.
    #[napi]
    pub async fn abort_transition(&self, transition: Buffer) -> Result<()> {
        self.inner
            .abort_transition(GitTransitionId::from_bytes(fixed_16(&transition)?))
            .await
            .map_err(napi_error)
    }

    /// Registers a real SDK workspace created for a prepared branch action.
    #[napi]
    pub async fn register_branch_workspace_json(
        &self,
        branch: String,
        workspace_id: Buffer,
        head: Option<Buffer>,
        switch_to_branch: bool,
    ) -> Result<String> {
        let head = head.as_ref().map(|value| commit_id(value)).transpose()?;
        let output = self
            .inner
            .register_branch_workspace(
                branch,
                WorkspaceId::from_bytes(fixed_16(&workspace_id)?),
                head,
                switch_to_branch,
            )
            .await
            .map_err(napi_error)?;
        serde_json::to_string(&output).map_err(napi_error)
    }

    /// Records a compatibility commit after ignore-aware generation capture.
    #[napi]
    pub async fn record_commit_json(
        &self,
        expected_head: Option<Buffer>,
        generation: Buffer,
        tracked_paths: Vec<String>,
        message: String,
        author: String,
        authored_at_seconds: String,
    ) -> Result<String> {
        let expected_head = expected_head
            .as_ref()
            .map(|value| commit_id(value))
            .transpose()?;
        let output = self
            .inner
            .record_commit(
                expected_head,
                generation_id(&generation)?,
                tracked_paths.into_iter().collect(),
                message,
                author,
                authored_at_seconds.parse::<i64>().map_err(napi_error)?,
            )
            .await
            .map_err(napi_error)?;
        serde_json::to_string(&output).map_err(napi_error)
    }
}
