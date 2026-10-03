//! Harness-owned authority boundary for the Filesystem Git compatibility view.
//!
//! The compatibility repository lives in `acyclic-fs` because it owns the
//! workspace generations, sequencer state, and recovery protocol.  Harness
//! only binds that repository to an authenticated caller and one project
//! volume.  This keeps the model-facing `acyclic git` surface small while all
//! lifecycle and integration effects still go through typed SDK operations.

use super::super::merge::{ProjectConflictSelection, ProjectJoinOutcome, ProjectJoinPlan};
use crate::{
    Error, OperationId, Result,
    conversation::{ConversationMessage, VolumeClass, VolumeOperation, VolumeRef},
    core::{Authority, AuthorityVerifier, Scope},
    resources::GenerationRef,
};
use acyclic_fs::{
    GitCommand, GitCommandOutput, GitCompatRepository, GitCompatRunError, GitCompatStore,
    GitFilesystemExecutor, IntoGitTreeRef, WorkspaceId,
};

/// Capability required to authorize an exact root writeback approval.
pub const ROOT_WRITEBACK_CAPABILITY: &str = "project:writeback";

/// Parent-issued approval for applying an inspected project join to the root.
///
/// The approval binds the operation and both immutable generations returned by
/// the inspected plan.  It is deliberately separate from a project merge
/// capability: holding a merge capability allows preparing a plan, while this
/// value is required to publish the plan into the root workspace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootWritebackApproval {
    operation_id: OperationId,
    source_generation: GenerationRef,
    expected_target_generation: GenerationRef,
    scope_id: String,
}

impl RootWritebackApproval {
    /// Issues a writeback approval from a verified parent scope.
    pub fn issue(
        verifier: &AuthorityVerifier,
        scope: &Scope,
        operation_id: OperationId,
        source_generation: GenerationRef,
        expected_target_generation: GenerationRef,
    ) -> Result<Self> {
        verifier.verify(scope)?;
        if !scope.capabilities().contains(ROOT_WRITEBACK_CAPABILITY) {
            return Err(Error::Unsupported(ROOT_WRITEBACK_CAPABILITY.into()));
        }
        if operation_id.into_bytes().iter().all(|byte| *byte == 0)
            || source_generation == expected_target_generation
            || source_generation.as_resource().provider()
                != expected_target_generation.as_resource().provider()
        {
            return Err(Error::Invalid(
                "root writeback approval is inconsistent".into(),
            ));
        }
        source_generation.validate()?;
        expected_target_generation.validate()?;
        Ok(Self {
            operation_id,
            source_generation,
            expected_target_generation,
            scope_id: scope.id().to_owned(),
        })
    }

    /// Stable operation identity bound by the approval.
    #[must_use]
    pub const fn operation_id(&self) -> OperationId {
        self.operation_id
    }
}

/// Immutable request captured before a root writeback is attempted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootWritebackRequest {
    /// Approval issued by the parent or user-facing approval flow.
    pub approval: RootWritebackApproval,
    /// The current caller scope that must match the approving parent.
    pub scope: Scope,
}

impl RootWritebackRequest {
    /// Creates a request for one exact approved publication.
    pub fn new(approval: RootWritebackApproval, scope: Scope) -> Self {
        Self { approval, scope }
    }
}

/// Thin authority wrapper around the durable Filesystem Git compatibility
/// repository.  It does not copy directories or implement merge semantics.
pub struct FilesystemGitFacade<S> {
    repository: GitCompatRepository<S>,
    workspace_id: WorkspaceId,
    volume: VolumeRef,
    verifier: AuthorityVerifier,
    scope: Scope,
}

impl<S> FilesystemGitFacade<S> {
    /// Binds one compatibility repository to an authenticated project volume.
    pub fn new(
        workspace_id: WorkspaceId,
        store: S,
        volume: VolumeRef,
        verifier: AuthorityVerifier,
        scope: Scope,
    ) -> Result<Self> {
        if volume.class() != VolumeClass::Project {
            return Err(Error::Invalid(
                "Git facade requires a project workspace volume".into(),
            ));
        }
        if volume.provider().family() != "filesystem" {
            return Err(Error::Invalid(
                "Git facade requires a Filesystem provider volume".into(),
            ));
        }
        verifier.verify(&scope)?;
        scope
            .capabilities()
            .contains(&volume.capability(VolumeOperation::Read)?)
            .then_some(())
            .ok_or_else(|| Error::Unauthorized("Git facade requires project read".into()))?;
        Ok(Self {
            repository: GitCompatRepository::new(workspace_id, store),
            workspace_id,
            volume,
            verifier,
            scope,
        })
    }

    /// Returns the immutable workspace identity bound by this facade.
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace_id
    }

    /// Returns the project volume bound by this facade.
    #[must_use]
    pub const fn volume(&self) -> &VolumeRef {
        &self.volume
    }

    /// Returns the bound caller scope without exposing any provider secret.
    #[must_use]
    pub const fn scope(&self) -> &Scope {
        &self.scope
    }

    /// Executes one model-facing `acyclic git` argv request.
    ///
    /// The underlying parser rejects shell composition and unsupported flags;
    /// no system `git` process is invoked by this method.
    pub async fn run_argv<E: GitFilesystemExecutor, T: IntoGitTreeRef>(
        &self,
        argv: &[String],
        workspace_tree: T,
        default_author: &str,
        now_seconds: i64,
        executor: &E,
    ) -> Result<GitCommandOutput>
    where
        S: GitCompatStore,
    {
        self.authorize_argv(argv)?;
        self.repository
            .run_argv(argv, workspace_tree, default_author, now_seconds, executor)
            .await
            .map_err(map_run_error)
    }

    /// Executes one already parsed typed command under the same authority.
    pub async fn run<E: GitFilesystemExecutor, T: IntoGitTreeRef>(
        &self,
        command: GitCommand,
        workspace_tree: T,
        executor: &E,
    ) -> Result<GitCommandOutput>
    where
        S: GitCompatStore,
    {
        self.authorize_command(&command)?;
        self.repository
            .run(command, workspace_tree, executor)
            .await
            .map_err(map_run_error)
    }

    /// Resumes a pending merge/rebase transition under the same caller scope.
    pub async fn resume<E: GitFilesystemExecutor>(
        &self,
        executor: &E,
    ) -> Result<Option<GitCommandOutput>>
    where
        S: GitCompatStore,
    {
        self.require_write()?;
        self.repository
            .resume(executor)
            .await
            .map_err(map_run_error)
    }

    /// Returns whether the exact scope can publish root changes.
    #[must_use]
    pub fn can_writeback(&self) -> bool {
        self.scope
            .capabilities()
            .contains(ROOT_WRITEBACK_CAPABILITY)
    }

    /// Applies a previously inspected project join only with an exact approval.
    ///
    /// The provider remains responsible for parent-only authorization, CAS,
    /// conflicts, rebase continuation, and recovery.  This method only binds
    /// the explicit user approval to that immutable plan.
    pub async fn apply_root_writeback(
        &self,
        request: &RootWritebackRequest,
        plan: &dyn ProjectJoinPlan,
        child: &Authority,
        notice: &ConversationMessage,
        selections: &[ProjectConflictSelection],
    ) -> Result<ProjectJoinOutcome> {
        self.verifier.verify(&request.scope)?;
        if request.scope.id() != request.approval.scope_id
            || request.scope != self.scope
            || !request
                .scope
                .capabilities()
                .contains(ROOT_WRITEBACK_CAPABILITY)
        {
            return Err(Error::Unauthorized(
                "root writeback approval belongs to another scope".into(),
            ));
        }
        if plan.source_generation() != &request.approval.source_generation
            || plan.expected_target_generation() != &request.approval.expected_target_generation
        {
            return Err(Error::Conflict(
                "root writeback approval does not match inspected plan".into(),
            ));
        }
        plan.apply(
            &request.scope,
            request.approval.operation_id,
            child,
            notice,
            selections,
        )
        .await
    }

    fn require_write(&self) -> Result<()> {
        if self
            .scope
            .capabilities()
            .contains(&self.volume.capability(VolumeOperation::Write)?)
        {
            Ok(())
        } else {
            Err(Error::Unauthorized(
                "Git facade requires project write".into(),
            ))
        }
    }

    fn authorize_argv(&self, argv: &[String]) -> Result<()> {
        let Some(command) = argv.first().map(String::as_str) else {
            return Err(Error::Invalid("Git command is empty".into()));
        };
        match command {
            "status" | "diff" | "log" | "show" | "blame" | "grep" | "archive" | "rev-parse"
            | "symbolic-ref" | "merge-base" | "ls-files" | "check-ignore" => self.require_read(),
            "merge" | "rebase" => {
                self.require_write()?;
                self.require_capability("project:merge")
            }
            "branch" if argv.len() == 1 => self.require_read(),
            "branch" | "switch" => self.require_fork(),
            "checkout" if argv.iter().any(|arg| arg == "--") => self.require_write(),
            "checkout" => self.require_fork(),
            "reset" | "restore" | "clean" | "stash" | "cherry-pick" | "revert" | "tag"
            | "apply" | "commit" | "add" => self.require_write(),
            _ => Err(Error::Invalid(format!(
                "unsupported Git command '{command}'"
            ))),
        }
    }

    fn authorize_command(&self, command: &GitCommand) -> Result<()> {
        match command {
            GitCommand::Status
            | GitCommand::Diff { .. }
            | GitCommand::Log { .. }
            | GitCommand::Show { .. }
            | GitCommand::Blame { .. }
            | GitCommand::Grep { .. }
            | GitCommand::Archive { .. }
            | GitCommand::RevParse { .. }
            | GitCommand::SymbolicRef { .. }
            | GitCommand::MergeBase { .. }
            | GitCommand::LsFiles
            | GitCommand::CheckIgnore { .. } => self.require_read(),
            GitCommand::Merge { .. }
            | GitCommand::MergeContinue
            | GitCommand::MergeAbort
            | GitCommand::Rebase { .. } => {
                self.require_write()?;
                self.require_capability("project:merge")
            }
            GitCommand::Branch { create: None } => self.require_read(),
            GitCommand::Branch { create: Some(_) } | GitCommand::Switch { .. } => {
                self.require_fork()
            }
            _ => self.require_write(),
        }
    }

    fn require_read(&self) -> Result<()> {
        if self
            .scope
            .capabilities()
            .contains(&self.volume.capability(VolumeOperation::Read)?)
        {
            Ok(())
        } else {
            Err(Error::Unauthorized(
                "Git facade requires project read".into(),
            ))
        }
    }

    fn require_capability(&self, capability: &str) -> Result<()> {
        if self.scope.capabilities().contains(capability) {
            Ok(())
        } else {
            Err(Error::Unsupported(capability.into()))
        }
    }

    fn require_fork(&self) -> Result<()> {
        self.require_write()?;
        self.require_capability("fork:publish")
    }
}

fn map_run_error<S, E>(error: GitCompatRunError<S, E>) -> Error
where
    S: std::error::Error,
    E: std::error::Error,
{
    Error::Storage(error.to_string())
}
