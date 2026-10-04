//! Harness-owned authority boundary for the Filesystem Git compatibility view.
//!
//! The compatibility repository lives in `acyclic-fs` because it owns the
//! workspace generations, sequencer state, and recovery protocol.  Harness
//! only binds that repository to an authenticated caller and one project
//! volume.  This keeps the model-facing `acyclic git` surface small while all
//! lifecycle and integration effects still go through typed SDK operations.

use super::super::merge::{
    ProjectConflictSelection, ProjectJoinOutcome, ProjectJoinPlan,
    ProjectMergeReceipt, ProjectMergeVerifier,
};
use super::{
    ParentMergePlan, ParentProjectController, ProjectMergeRecovery, ProjectMergeRecoveryEntry,
    ProjectMergeTerminal, ProjectMergeTerminalConflict, WorkspaceObservation,
};
use crate::{
    Error, IdempotencyKey, InteractionId, OperationId, Result,
    contract::canonical_json_digest,
    conversation::{
        Attachment, ContentGrant, ConversationMessage, ReferencedAttachments, VolumeClass,
        VolumeOperation, VolumeRef, decode_attachment_manifest,
    },
    core::{Authority, AuthorityVerifier, Reducer, Scope},
    resources::GenerationRef,
};
use serde::{Deserialize, Serialize};
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, ConflictSide, Digest, GitCommand, GitCommandOutput,
    GitCompatRepository, GitCompatRunError, GitCompatStore, GitFilesystemExecutor,
    GitPendingMutation, IntoGitTreeRef, JoinOutcome, MergeConflict, MergeDriverRegistry, MergePlan,
    MergeResolutionCache, WorkspaceId,
};
use std::sync::Arc;

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
    target_project: VolumeRef,
    child_project: Option<VolumeRef>,
    child_authority: Option<Authority>,
    source_generation: GenerationRef,
    expected_target_generation: GenerationRef,
    scope_id: String,
    action_digest: [u8; 32],
}

/// Computes the stable action identity for one exact inspected root writeback.
pub fn root_writeback_action_digest(
    target_project: &VolumeRef,
    operation_id: OperationId,
    source_generation: &GenerationRef,
    expected_target_generation: &GenerationRef,
) -> Result<[u8; 32]> {
    canonical_json_digest(&(
        "acyclic.root-writeback.v1",
        target_project,
        operation_id,
        source_generation,
        expected_target_generation,
    ))
}

/// Computes the action identity for a direct-child root publication.
pub fn root_writeback_child_action_digest(
    target_project: &VolumeRef,
    child_project: &VolumeRef,
    child_authority: &Authority,
    operation_id: OperationId,
    source_generation: &GenerationRef,
    expected_target_generation: &GenerationRef,
) -> Result<[u8; 32]> {
    canonical_json_digest(&(
        "acyclic.root-writeback-child.v1",
        target_project,
        child_project,
        child_authority,
        operation_id,
        source_generation,
        expected_target_generation,
    ))
}

impl RootWritebackApproval {
    /// Issues a writeback approval from a verified parent scope.
    pub fn issue(
        verifier: &AuthorityVerifier,
        scope: &Scope,
        target_project: VolumeRef,
        operation_id: OperationId,
        source_generation: GenerationRef,
        expected_target_generation: GenerationRef,
    ) -> Result<Self> {
        verifier.verify(scope)?;
        if !scope.capabilities().contains(ROOT_WRITEBACK_CAPABILITY) {
            return Err(Error::Unsupported(ROOT_WRITEBACK_CAPABILITY.into()));
        }
        target_project.validate()?;
        if target_project.class() != VolumeClass::Project
            || operation_id.into_bytes().iter().all(|byte| *byte == 0)
            || source_generation == expected_target_generation
            || source_generation.as_resource().provider()
                != expected_target_generation.as_resource().provider()
            || source_generation.as_resource().provider() != target_project.provider()
        {
            return Err(Error::Invalid(
                "root writeback approval is inconsistent".into(),
            ));
        }
        source_generation.validate()?;
        expected_target_generation.validate()?;
        let action_digest = root_writeback_action_digest(
            &target_project,
            operation_id,
            &source_generation,
            &expected_target_generation,
        )?;
        Ok(Self {
            operation_id,
            target_project,
            child_project: None,
            child_authority: None,
            source_generation,
            expected_target_generation,
            scope_id: scope.id().to_owned(),
            action_digest,
        })
    }

    /// Issues an approval bound to one exact direct child.
    pub fn issue_for_child(
        verifier: &AuthorityVerifier,
        scope: &Scope,
        target_project: VolumeRef,
        child_project: VolumeRef,
        child_authority: Authority,
        operation_id: OperationId,
        source_generation: GenerationRef,
        expected_target_generation: GenerationRef,
    ) -> Result<Self> {
        let mut approval = Self::issue(
            verifier,
            scope,
            target_project,
            operation_id,
            source_generation,
            expected_target_generation,
        )?;
        child_project.validate()?;
        child_authority.stream_path()?;
        if child_project.class() != VolumeClass::Project
            || child_project.provider() != approval.target_project.provider()
        {
            return Err(Error::Invalid("root writeback child is inconsistent".into()));
        }
        approval.child_project = Some(child_project);
        approval.child_authority = Some(child_authority);
        approval.action_digest = root_writeback_child_action_digest(
            approval.target_project(),
            approval.child_project.as_ref().expect("child set above"),
            approval.child_authority.as_ref().expect("authority set above"),
            approval.operation_id,
            &approval.source_generation,
            &approval.expected_target_generation,
        )?;
        Ok(approval)
    }

    /// Stable operation identity bound by the approval.
    #[must_use]
    pub const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    /// Authenticated scope identity bound to this approval.
    #[must_use]
    pub fn scope_id(&self) -> &str {
        &self.scope_id
    }

    /// Parent project bound to this approval.
    #[must_use]
    pub const fn target_project(&self) -> &VolumeRef {
        &self.target_project
    }

    /// Direct child project bound by this approval, when child binding is used.
    #[must_use]
    pub const fn child_project(&self) -> Option<&VolumeRef> {
        self.child_project.as_ref()
    }

    /// Direct child authority bound by this approval, when child binding is used.
    #[must_use]
    pub const fn child_authority(&self) -> Option<&Authority> {
        self.child_authority.as_ref()
    }

    /// Digest that must appear in the durable operator approval ticket.
    #[must_use]
    pub const fn action_digest(&self) -> &[u8; 32] {
        &self.action_digest
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

/// Durable, model-independent handle for an inspected root writeback.
/// The provider merge plan remains opaque and is rebuilt by Harness after reopen.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootWritebackInspection {
    /// Contract revision.
    pub version: u32,
    /// Stable inspection/recovery operation identity.
    pub inspection_id: OperationId,
    /// Durable approval ticket required for publication.
    pub approval_id: InteractionId,
    /// Root project receiving the publication.
    pub root_project: VolumeRef,
    /// Direct child project inspected.
    pub child_project: VolumeRef,
    /// Direct child authority inspected.
    pub child_authority: Authority,
    /// Provider operation identity.
    pub operation_id: OperationId,
    /// Child generation inspected.
    pub source_generation: GenerationRef,
    /// Root generation expected at publication.
    pub expected_target_generation: GenerationRef,
    /// Exact operator action digest.
    pub action_digest: [u8; 32],
    /// Immutable recovery identity.
    pub recovery_identity: [u8; 32],
}

impl RootWritebackInspection {
    /// Constructs an exact inspection handle.
    pub fn new(
        inspection_id: OperationId,
        approval_id: InteractionId,
        root_project: VolumeRef,
        child_project: VolumeRef,
        child_authority: Authority,
        operation_id: OperationId,
        source_generation: GenerationRef,
        expected_target_generation: GenerationRef,
    ) -> Result<Self> {
        let action_digest = root_writeback_child_action_digest(
            &root_project,
            &child_project,
            &child_authority,
            operation_id,
            &source_generation,
            &expected_target_generation,
        )?;
        let recovery_identity = canonical_json_digest(&(
            "acyclic.root-writeback-inspection.v1",
            inspection_id,
            approval_id,
            &root_project,
            &child_project,
            &child_authority,
            operation_id,
            &source_generation,
            &expected_target_generation,
            action_digest,
        ))?;
        let handle = Self {
            version: 1,
            inspection_id,
            approval_id,
            root_project,
            child_project,
            child_authority,
            operation_id,
            source_generation,
            expected_target_generation,
            action_digest,
            recovery_identity,
        };
        handle.validate()?;
        Ok(handle)
    }

    /// Revalidates every binding after loading from durable storage.
    pub fn validate(&self) -> Result<()> {
        if self.version != 1
            || self.inspection_id.into_bytes() == [0; 16]
            || self.operation_id.into_bytes() == [0; 16]
            || self.approval_id.into_bytes() == [0; 16]
        {
            return Err(Error::Invalid("root writeback inspection identity is invalid".into()));
        }
        let action = root_writeback_child_action_digest(
            &self.root_project,
            &self.child_project,
            &self.child_authority,
            self.operation_id,
            &self.source_generation,
            &self.expected_target_generation,
        )?;
        if action != self.action_digest {
            return Err(Error::Conflict("root writeback inspection action changed".into()));
        }
        let recovery = canonical_json_digest(&(
            "acyclic.root-writeback-inspection.v1",
            self.inspection_id,
            self.approval_id,
            &self.root_project,
            &self.child_project,
            &self.child_authority,
            self.operation_id,
            &self.source_generation,
            &self.expected_target_generation,
            self.action_digest,
        ))?;
        if recovery != self.recovery_identity {
            return Err(Error::Conflict("root writeback inspection identity changed".into()));
        }
        Ok(())
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
            .map_err(|error| map_run_error(&error))
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
            .map_err(|error| map_run_error(&error))
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
        let pending = self
            .repository
            .pending_transition()
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        if let Some(pending) = pending {
            match pending.mutation {
                GitPendingMutation::ForkBranch { .. } => self.require_fork()?,
                GitPendingMutation::Join { .. } => self.require_capability("project:merge")?,
                GitPendingMutation::Switch { .. } => self.require_fork()?,
                _ => {}
            }
            return self
                .repository
                .resume_pending(pending, executor)
                .await
                .map(Some)
                .map_err(|error| map_run_error(&error));
        }
        Ok(None)
    }

    /// Returns whether the exact scope can publish root changes.
    #[must_use]
    pub fn can_writeback(&self) -> bool {
        self.scope
            .capabilities()
            .contains(ROOT_WRITEBACK_CAPABILITY)
    }

    /// Forks a project through the authenticated parent controller.
    ///
    /// The facade is only the model-facing entrypoint.  Parent authority,
    /// lineage, generation pinning, and durable publication remain owned by
    /// `ParentProjectController` and the Filesystem provider.
    pub async fn fork_project<A, O>(
        &self,
        host: &super::FilesystemHost<A, O>,
        parent: &Reducer,
        source_generation: &GenerationRef,
        child: &VolumeRef,
        idempotency_key: &IdempotencyKey,
    ) -> Result<WorkspaceObservation>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let controller = ParentProjectController::new(
            host,
            parent,
            &self.verifier,
            &self.scope,
            self.volume.clone(),
        )?;
        controller
            .fork_project(source_generation, child, idempotency_key)
            .await
    }

    /// Inspects a direct child's changes under the authenticated parent.
    pub(crate) async fn prepare_project_merge<A, O>(
        &self,
        host: &super::FilesystemHost<A, O>,
        parent: &Reducer,
        child: &VolumeRef,
    ) -> Result<ParentMergePlan<A, O>>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let controller = ParentProjectController::new(
            host,
            parent,
            &self.verifier,
            &self.scope,
            self.volume.clone(),
        )?;
        controller.prepare_project_merge(child).await
    }

    /// Inspects only a child whose fork was published by this parent
    /// conversation. The reducer check is part of the facade boundary so a
    /// grandchild or sibling volume cannot be routed by capability alone.
    pub async fn prepare_project_merge_for_child<A, O>(
        &self,
        host: &super::FilesystemHost<A, O>,
        parent: &Reducer,
        child: &Authority,
        child_project: &VolumeRef,
    ) -> Result<ParentMergePlan<A, O>>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.authorize_direct_child(parent, child, child_project)?;
        self.prepare_project_merge(host, parent, child_project)
            .await
    }

    /// Publishes a previously inspected project join under parent authority.
    pub(crate) async fn apply_project_merge<A, O>(
        &self,
        host: &super::FilesystemHost<A, O>,
        parent: &Reducer,
        plan: &ParentMergePlan<A, O>,
        operation_id: OperationId,
    ) -> Result<JoinOutcome<A, O>>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let controller = ParentProjectController::new(
            host,
            parent,
            &self.verifier,
            &self.scope,
            self.volume.clone(),
        )?;
        controller.apply_project_merge(plan, operation_id).await
    }

    /// Legacy notice-less direct-child publication entrypoint.
    ///
    /// This method is retained as a source-compatible guard for older callers,
    /// but deliberately refuses to publish. Every successful merge must admit
    /// its notice content before the provider effect, so callers must use
    /// [`Self::apply_project_merge_for_child_with_notice`] or
    /// [`Self::apply_project_merge_for_child_with_receipt`].
    #[deprecated(note = "use a notice-bound direct-child merge method")]
    #[allow(
        clippy::too_many_arguments,
        reason = "publication keeps parent, child, plan, and operation identities explicit"
    )]
    pub async fn apply_project_merge_for_child<A, O>(
        &self,
        _host: &super::FilesystemHost<A, O>,
        _parent: &Reducer,
        _child: &Authority,
        _child_project: &VolumeRef,
        _plan: &ParentMergePlan<A, O>,
        _operation_id: OperationId,
    ) -> Result<JoinOutcome<A, O>>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        Err(Error::Unauthorized(
            "direct-child merge requires a validated merge notice; use apply_project_merge_for_child_with_notice or _with_receipt".into(),
        ))
    }

    /// Validates the merge notice and direct-child binding immediately before
    /// applying the inspected provider join. Callers that will publish a
    /// merge receipt should use this boundary so malformed notices cannot
    /// follow a successful Filesystem mutation.
    #[allow(
        clippy::too_many_arguments,
        reason = "the mutating boundary keeps parent, child, plan, operation, and notice explicit"
    )]
    pub async fn apply_project_merge_for_child_with_notice<A, O>(
        &self,
        host: &super::FilesystemHost<A, O>,
        parent: &Reducer,
        child: &Authority,
        child_project: &VolumeRef,
        plan: &ParentMergePlan<A, O>,
        operation_id: OperationId,
        notice: &ConversationMessage,
    ) -> Result<JoinOutcome<A, O>>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.authorize_direct_child_plan(parent, child, child_project, plan)?;
        validate_merge_receipt_inputs(child, notice)?;
        validate_merge_notice_content(host, &self.verifier, &self.scope, notice).await?;
        self.apply_project_merge(host, parent, plan, operation_id)
            .await
    }

    /// Applies a direct-child merge and constructs the matching authenticated
    /// receipt from the same validated child and notice. Taking ownership of
    /// those values keeps callers from applying one receipt intent and later
    /// publishing a different child or notice.
    #[allow(
        clippy::too_many_arguments,
        reason = "the mutating boundary keeps parent, child, plan, operation, and notice explicit"
    )]
    pub async fn apply_project_merge_for_child_with_receipt<A, O>(
        &self,
        host: &super::FilesystemHost<A, O>,
        parent: &Reducer,
        child: &Authority,
        child_project: &VolumeRef,
        plan: &ParentMergePlan<A, O>,
        operation_id: OperationId,
        notice: ConversationMessage,
    ) -> Result<ProjectMergeReceipt>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.authorize_direct_child_plan(parent, child, child_project, plan)?;
        validate_merge_receipt_inputs(child, &notice)?;
        validate_merge_notice_content(host, &self.verifier, &self.scope, &notice).await?;
        let outcome = self
            .apply_project_merge(host, parent, plan, operation_id)
            .await?;
        self.merge_receipt(
            host,
            parent,
            plan,
            &outcome,
            child.clone(),
            operation_id,
            notice,
        )
    }

    /// Converts a successful provider join into the authenticated Harness
    /// receipt used to publish the parent conversation's merge event.
    #[allow(
        clippy::too_many_arguments,
        reason = "receipt construction binds the provider outcome to every authenticated input"
    )]
    pub fn merge_receipt<A, O>(
        &self,
        host: &super::FilesystemHost<A, O>,
        parent: &Reducer,
        plan: &ParentMergePlan<A, O>,
        outcome: &JoinOutcome<A, O>,
        child: Authority,
        operation_id: OperationId,
        notice: ConversationMessage,
    ) -> Result<ProjectMergeReceipt>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.authorize_direct_child(parent, &child, plan.child_project())?;
        validate_merge_receipt_inputs(&child, &notice)?;
        let controller = ParentProjectController::new(
            host,
            parent,
            &self.verifier,
            &self.scope,
            self.volume.clone(),
        )?;
        controller.merge_receipt(plan, outcome, child, operation_id, notice)
    }

    /// Describes exact conflicts in an inspected child merge plan.
    pub async fn describe_project_merge_conflicts<A, O>(
        &self,
        host: &super::FilesystemHost<A, O>,
        parent: &Reducer,
        plan: &ParentMergePlan<A, O>,
        conflicts: &[MergeConflict],
        truncated: bool,
    ) -> Result<MergePlan>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let controller = ParentProjectController::new(
            host,
            parent,
            &self.verifier,
            &self.scope,
            self.volume.clone(),
        )?;
        controller
            .describe_project_merge_conflicts(plan, conflicts, truncated)
            .await
    }

    /// Applies explicit conflict-side choices through the provider's typed join.
    pub(crate) async fn apply_project_merge_sides<A, O>(
        &self,
        host: &super::FilesystemHost<A, O>,
        parent: &Reducer,
        plan: &ParentMergePlan<A, O>,
        operation_id: OperationId,
        selections: std::collections::BTreeMap<MergeConflict, ConflictSide>,
    ) -> Result<JoinOutcome<A, O>>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let controller = ParentProjectController::new(
            host,
            parent,
            &self.verifier,
            &self.scope,
            self.volume.clone(),
        )?;
        controller
            .apply_project_merge_sides(plan, operation_id, selections)
            .await
    }

    /// Resolves a plan only after rechecking direct-child lineage.
    pub async fn apply_project_merge_sides_for_child<A, O>(
        &self,
        host: &super::FilesystemHost<A, O>,
        parent: &Reducer,
        child: &Authority,
        child_project: &VolumeRef,
        plan: &ParentMergePlan<A, O>,
        operation_id: OperationId,
        selections: std::collections::BTreeMap<MergeConflict, ConflictSide>,
    ) -> Result<JoinOutcome<A, O>>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.authorize_direct_child_plan(parent, child, child_project, plan)?;
        self.apply_project_merge_sides(host, parent, plan, operation_id, selections)
            .await
    }

    /// Resolves conflicts with registered immutable-input drivers.
    #[allow(
        clippy::too_many_arguments,
        reason = "driver resolution keeps the inspected plan and retry inputs explicit"
    )]
    pub(crate) async fn apply_project_merge_with_drivers<A, O, C>(
        &self,
        host: &super::FilesystemHost<A, O>,
        parent: &Reducer,
        plan: &ParentMergePlan<A, O>,
        operation_id: OperationId,
        registry: &MergeDriverRegistry,
        cache: &mut C,
        replanning: bool,
    ) -> Result<JoinOutcome<A, O>>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
        C: MergeResolutionCache,
    {
        let controller = ParentProjectController::new(
            host,
            parent,
            &self.verifier,
            &self.scope,
            self.volume.clone(),
        )?;
        controller
            .apply_project_merge_with_drivers(plan, operation_id, registry, cache, replanning)
            .await
    }

    /// Resolves a direct child's conflicts only after rechecking the child's
    /// published fork and the plan's captured child project.
    #[allow(
        clippy::too_many_arguments,
        reason = "driver resolution keeps parent, child, plan, and retry inputs explicit"
    )]
    pub async fn apply_project_merge_with_drivers_for_child<A, O, C>(
        &self,
        host: &super::FilesystemHost<A, O>,
        parent: &Reducer,
        child: &Authority,
        child_project: &VolumeRef,
        plan: &ParentMergePlan<A, O>,
        operation_id: OperationId,
        registry: &MergeDriverRegistry,
        cache: &mut C,
        replanning: bool,
    ) -> Result<JoinOutcome<A, O>>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
        C: MergeResolutionCache,
    {
        self.authorize_direct_child_plan(parent, child, child_project, plan)?;
        self.apply_project_merge_with_drivers(
            host,
            parent,
            plan,
            operation_id,
            registry,
            cache,
            replanning,
        )
        .await
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
        self.verify_root_writeback(
            request,
            plan.source_generation(),
            plan.expected_target_generation(),
        )?;
        if plan.target_project() != Some(&self.volume) {
            return Err(Error::Unauthorized(
                "root writeback plan is bound to another project".into(),
            ));
        }
        validate_merge_receipt_inputs(child, notice)?;
        // This compatibility entrypoint accepts a trait object supplied by an
        // application.  A caller can otherwise implement `lineage()` and
        // `validate_notice_content()` with values that look valid while
        // routing the mutation to an arbitrary provider.  Keep validation of
        // the serialized notice above for useful malformed-input errors, but
        // require the concrete facade-owned provider path for every effect.
        let _ = (plan, child, selections);
        Err(Error::Unauthorized(
            "generic project join plans cannot perform root writeback; use the authenticated Filesystem provider facade".into(),
        ))
    }

    /// Applies an inspected native Filesystem join only with the same exact
    /// approval used by the model-facing writeback boundary.  This keeps the
    /// durable provider plan behind the facade while still binding approval
    /// to immutable source and target generations.
    pub(crate) async fn apply_root_writeback_plan<A, O>(
        &self,
        request: &RootWritebackRequest,
        host: &super::FilesystemHost<A, O>,
        parent: &Reducer,
        plan: &ParentMergePlan<A, O>,
        selections: std::collections::BTreeMap<MergeConflict, ConflictSide>,
    ) -> Result<JoinOutcome<A, O>>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.verify_root_writeback_plan(request, host, plan)?;
        let controller = ParentProjectController::new(
            host,
            parent,
            &self.verifier,
            &self.scope,
            self.volume.clone(),
        )?;
        if selections.is_empty() {
            controller
                .apply_project_merge(plan, request.approval.operation_id)
                .await
        } else {
            controller
                .apply_project_merge_sides(plan, request.approval.operation_id, selections)
                .await
        }
    }

    /// Legacy notice-less root writeback entrypoint.
    ///
    /// This method is retained as a source-compatible guard for older callers,
    /// but deliberately refuses to publish. Every successful root writeback
    /// must admit its notice content before the provider effect, so callers
    /// must use a notice-bound or receipt-bound method.
    #[deprecated(note = "use a notice-bound root writeback method")]
    #[allow(
        clippy::too_many_arguments,
        reason = "approved writeback keeps all authority and generation inputs explicit"
    )]
    pub async fn apply_root_writeback_plan_for_child<A, O>(
        &self,
        _request: &RootWritebackRequest,
        _host: &super::FilesystemHost<A, O>,
        _parent: &Reducer,
        _child: &Authority,
        _child_project: &VolumeRef,
        _plan: &ParentMergePlan<A, O>,
        _selections: std::collections::BTreeMap<MergeConflict, ConflictSide>,
    ) -> Result<JoinOutcome<A, O>>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        Err(Error::Unauthorized(
            "root writeback requires a validated merge notice; use _with_notice or _with_receipt"
                .into(),
        ))
    }

    /// Approved native writeback whose child and merge notice are validated at
    /// the same boundary immediately before the provider join. Callers that
    /// will publish a conversation receipt should use this method so an
    /// invalid notice cannot follow a successful workspace mutation.
    #[allow(
        clippy::too_many_arguments,
        reason = "writeback keeps request, parent, child, plan, selections, and notice explicit"
    )]
    pub async fn apply_root_writeback_plan_for_child_with_notice<A, O>(
        &self,
        request: &RootWritebackRequest,
        host: &super::FilesystemHost<A, O>,
        parent: &Reducer,
        child: &Authority,
        child_project: &VolumeRef,
        plan: &ParentMergePlan<A, O>,
        selections: std::collections::BTreeMap<MergeConflict, ConflictSide>,
        notice: &ConversationMessage,
    ) -> Result<JoinOutcome<A, O>>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.authorize_direct_child_plan(parent, child, child_project, plan)?;
        Self::verify_root_writeback_child(request, child, child_project)?;
        validate_merge_receipt_inputs(child, notice)?;
        self.verify_root_writeback_plan(request, host, plan)?;
        validate_merge_notice_content(host, &self.verifier, &self.scope, notice).await?;
        self.apply_root_writeback_plan(request, host, parent, plan, selections)
            .await
    }

    /// Applies approved native writeback and constructs the receipt from the
    /// same child, notice, operation, and generation approval. This is the
    /// model-facing boundary for callers that must publish the resulting
    /// conversation event after the provider join.
    #[allow(
        clippy::too_many_arguments,
        reason = "receipt-bound writeback keeps approval, parent, child, plan, selections, and notice explicit"
    )]
    pub async fn apply_root_writeback_plan_for_child_with_receipt<A, O>(
        &self,
        request: &RootWritebackRequest,
        host: &super::FilesystemHost<A, O>,
        parent: &Reducer,
        child: Authority,
        child_project: &VolumeRef,
        plan: &ParentMergePlan<A, O>,
        selections: std::collections::BTreeMap<MergeConflict, ConflictSide>,
        notice: ConversationMessage,
    ) -> Result<ProjectMergeReceipt>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        self.authorize_direct_child_plan(parent, &child, child_project, plan)?;
        Self::verify_root_writeback_child(request, &child, child_project)?;
        validate_merge_receipt_inputs(&child, &notice)?;
        self.verify_root_writeback_plan(request, host, plan)?;
        validate_merge_notice_content(host, &self.verifier, &self.scope, &notice).await?;
        let outcome = self
            .apply_root_writeback_plan(request, host, parent, plan, selections)
            .await?;
        self.merge_receipt(
            host,
            parent,
            plan,
            &outcome,
            child,
            request.approval.operation_id,
            notice,
        )
    }

    /// Applies an approved native writeback while retaining the immutable
    /// provider inputs before dispatch and the receipt before publication.
    /// A caller that is interrupted after the provider join can reopen the
    /// durable ledger and use [`Self::recover_root_writeback_receipt`] without
    /// reapplying the join.
    #[allow(
        clippy::too_many_arguments,
        reason = "recovery-bound writeback keeps approval, parent, child, plan, selections, notice, and journal explicit"
    )]
    pub async fn apply_root_writeback_plan_for_child_with_recovery<A, O>(
        &self,
        request: &RootWritebackRequest,
        host: &super::FilesystemHost<A, O>,
        parent: &Reducer,
        child: Authority,
        child_project: &VolumeRef,
        plan: &ParentMergePlan<A, O>,
        selections: std::collections::BTreeMap<MergeConflict, ConflictSide>,
        notice: ConversationMessage,
        recovery: &ProjectMergeRecovery<'_>,
    ) -> Result<ProjectMergeReceipt>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        let outcome = self
            .apply_root_writeback_plan_for_child_with_recovery_outcome(
                request,
                host,
                parent,
                child,
                child_project,
                plan,
                selections,
                notice,
                recovery,
            )
            .await?;
        match outcome {
            ProjectJoinOutcome::Applied(receipt)
            | ProjectJoinOutcome::AlreadyApplied(receipt) => Ok(receipt),
            ProjectJoinOutcome::NoChanges(_)
            | ProjectJoinOutcome::StaleTarget(_)
            | ProjectJoinOutcome::Conflicted { .. }
            | ProjectJoinOutcome::Fenced
            | ProjectJoinOutcome::IdempotencyConflict => Err(Error::Conflict(
                "project join completed without a receipt".into(),
            )),
        }
    }

    /// Applies the recovery-bound writeback and returns every typed provider
    /// result. Known non-applied results are recorded before they are returned;
    /// a retry with the same operation identity reopens that exact terminal
    /// result without dispatching the provider again.
    #[allow(
        clippy::too_many_arguments,
        reason = "recovery-bound writeback keeps approval, parent, child, plan, selections, notice, and journal explicit"
    )]
    pub async fn apply_root_writeback_plan_for_child_with_recovery_outcome<A, O>(
        &self,
        request: &RootWritebackRequest,
        host: &super::FilesystemHost<A, O>,
        parent: &Reducer,
        child: Authority,
        child_project: &VolumeRef,
        plan: &ParentMergePlan<A, O>,
        selections: std::collections::BTreeMap<MergeConflict, ConflictSide>,
        notice: ConversationMessage,
        recovery: &ProjectMergeRecovery<'_>,
    ) -> Result<ProjectJoinOutcome>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        self.authorize_direct_child_plan(parent, &child, child_project, plan)?;
        Self::verify_root_writeback_child(request, &child, child_project)?;
        validate_merge_receipt_inputs(&child, &notice)?;
        self.verify_root_writeback_plan(request, host, plan)?;
        validate_merge_notice_content(host, &self.verifier, &self.scope, &notice).await?;
        // The approval was authenticated before content residency and before
        // claiming durable recovery state. Otherwise a forged or stale request
        // could leave an intent behind that recovery later launders.
        let intent = super::ProjectMergeIntent {
            operation_id: request.approval.operation_id,
            child: child.clone(),
            source_project: child_project.clone(),
            source_generation: host.generation_ref_id(plan.source_head())?,
            target_project: self.volume.clone(),
            expected_target_generation: host.generation_ref_id(plan.target_head())?,
            expected_target_head: plan.target_authority_head(),
            resolutions_digest: plan.resolution_digest(&selections).into_bytes(),
            approval_scope_id: request.approval.scope_id.clone(),
            notice: notice.clone(),
        };
        recovery.prepare(intent).await?;
        if let Some(entry) = recovery.reopen().await? {
            if let Some(terminal) = entry.terminal {
                return Ok(terminal.outcome());
            }
            if let Some(receipt) = entry.receipt {
                return Ok(ProjectJoinOutcome::AlreadyApplied(receipt));
            }
        }
        let outcome = self
            .apply_root_writeback_plan(request, host, parent, plan, selections)
            .await?;
        match &outcome {
            JoinOutcome::Applied(_) | JoinOutcome::AlreadyApplied(_) => {
                let receipt = self.merge_receipt(
                    host,
                    parent,
                    plan,
                    &outcome,
                    child,
                    request.approval.operation_id,
                    notice,
                )?;
                let verifier = super::FilesystemProjectMergeVerifier::new(Arc::new(host.clone()));
                recovery.record_applied(receipt.clone(), &verifier).await?;
                Ok(if matches!(&outcome, JoinOutcome::AlreadyApplied(_)) {
                    ProjectJoinOutcome::AlreadyApplied(receipt)
                } else {
                    ProjectJoinOutcome::Applied(receipt)
                })
            }
            _ => {
                let terminal = terminal_from_join_outcome(host, &outcome)?;
                recovery.record_terminal(terminal.clone()).await?;
                Ok(terminal.outcome())
            }
        }
    }

    /// Reconstructs the provider receipt after a crash between the provider
    /// join and the journal's applied-receipt record. The retained intent is
    /// the only source for child, generations, notice, and resolution digest.
    pub async fn recover_root_writeback_receipt<A, O>(
        &self,
        host: &super::FilesystemHost<A, O>,
        parent: &Reducer,
        entry: &ProjectMergeRecoveryEntry,
    ) -> Result<ProjectMergeReceipt>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        entry.validate()?;
        if entry.intent.target_project != self.volume {
            return Err(Error::Unauthorized(
                "recovery target is outside this Git facade".into(),
            ));
        }
        if entry.intent.approval_scope_id != self.scope.id() {
            return Err(Error::Unauthorized(
                "recovery approval belongs to another scope".into(),
            ));
        }
        if entry.terminal.is_some() {
            return Err(Error::Conflict(
                "project provider result is a durable terminal outcome".into(),
            ));
        }
        if let Some(receipt) = &entry.receipt {
            return Ok(receipt.clone());
        }
        let approval = RootWritebackApproval {
            operation_id: entry.intent.operation_id,
            target_project: entry.intent.target_project.clone(),
            child_project: Some(entry.intent.source_project.clone()),
            child_authority: Some(entry.intent.child.clone()),
            source_generation: entry.intent.source_generation.clone(),
            expected_target_generation: entry.intent.expected_target_generation.clone(),
            scope_id: entry.intent.approval_scope_id.clone(),
            action_digest: root_writeback_child_action_digest(
                &entry.intent.target_project,
                &entry.intent.source_project,
                &entry.intent.child,
                entry.intent.operation_id,
                &entry.intent.source_generation,
                &entry.intent.expected_target_generation,
            )?,
        };
        self.verify_root_writeback(
            &RootWritebackRequest::new(approval, self.scope.clone()),
            &entry.intent.source_generation,
            &entry.intent.expected_target_generation,
        )?;
        self.authorize_direct_child(parent, &entry.intent.child, &entry.intent.source_project)?;
        validate_merge_notice_content(host, &self.verifier, &self.scope, &entry.intent.notice)
            .await?;
        let controller = ParentProjectController::new(
            host,
            parent,
            &self.verifier,
            &self.scope,
            self.volume.clone(),
        )?;
        let plan = controller
            .prepare_project_merge_at(
                &entry.intent.source_project,
                &entry.intent.source_generation,
                &entry.intent.expected_target_generation,
                entry.intent.expected_target_head,
            )
            .await?;
        if plan.child_project() != &entry.intent.source_project {
            return Err(Error::Conflict(
                "reopened merge plan changed its source project".into(),
            ));
        }
        let witness = controller
            .recover_project_merge_witness(
                &plan,
                entry.intent.operation_id,
                Digest::from_bytes(entry.intent.resolutions_digest),
            )
            .await?
            .ok_or_else(|| Error::Conflict("provider join result is not durable".into()))?;
        let receipt = controller
            .merge_receipt_from_witness(
                &plan,
                &witness,
                entry.intent.child.clone(),
                entry.intent.operation_id,
                entry.intent.notice.clone(),
            )
            .await?;
        // Keep reconstruction behind the same provider-bound verifier used
        // before journal persistence. A structurally valid witness from a
        // different target or operation must never become a recovered receipt.
        let verifier = super::FilesystemProjectMergeVerifier::new(Arc::new(host.clone()));
        verifier.verify(&receipt).await?;
        Ok(receipt)
    }

    fn verify_root_writeback_plan<A, O>(
        &self,
        request: &RootWritebackRequest,
        host: &super::FilesystemHost<A, O>,
        plan: &ParentMergePlan<A, O>,
    ) -> Result<()>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
    {
        let source = host.generation_ref_id(plan.source_head())?;
        let target = host.generation_ref_id(plan.target_head())?;
        self.verify_root_writeback(request, &source, &target)
    }

    fn verify_root_writeback(
        &self,
        request: &RootWritebackRequest,
        source: &GenerationRef,
        target: &GenerationRef,
    ) -> Result<()> {
        self.verifier.verify(&request.scope)?;
        if request.scope.id() != request.approval.scope_id
            || request.scope != self.scope
            || request.approval.target_project != self.volume
            || !request
                .scope
                .capabilities()
                .contains(ROOT_WRITEBACK_CAPABILITY)
        {
            return Err(Error::Unauthorized(
                "root writeback approval belongs to another scope".into(),
            ));
        }
        if source != &request.approval.source_generation
            || target != &request.approval.expected_target_generation
        {
            return Err(Error::Conflict(
                "root writeback approval does not match inspected plan".into(),
            ));
        }
        let expected_digest = match (
            request.approval.child_project.as_ref(),
            request.approval.child_authority.as_ref(),
        ) {
            (Some(child_project), Some(child_authority)) => root_writeback_child_action_digest(
                &request.approval.target_project,
                child_project,
                child_authority,
                request.approval.operation_id,
                &request.approval.source_generation,
                &request.approval.expected_target_generation,
            )?,
            (None, None) => root_writeback_action_digest(
                &request.approval.target_project,
                request.approval.operation_id,
                &request.approval.source_generation,
                &request.approval.expected_target_generation,
            )?,
            _ => {
                return Err(Error::Invalid(
                    "root writeback child binding is incomplete".into(),
                ));
            }
        };
        if expected_digest != request.approval.action_digest {
            return Err(Error::Conflict(
                "root writeback approval action identity changed".into(),
            ));
        }
        Ok(())
    }

    fn verify_root_writeback_child(
        request: &RootWritebackRequest,
        child: &Authority,
        child_project: &VolumeRef,
    ) -> Result<()> {
        if request.approval.child_project() != Some(child_project)
            || request.approval.child_authority() != Some(child)
        {
            return Err(Error::Unauthorized(
                "root writeback approval belongs to another child".into(),
            ));
        }
        Ok(())
    }

    fn authorize_direct_child(
        &self,
        parent: &Reducer,
        child: &Authority,
        child_project: &VolumeRef,
    ) -> Result<()> {
        if child.kind != crate::core::AggregateKind::Conversation {
            return Err(Error::Invalid(
                "project join child is not a conversation".into(),
            ));
        }
        child.stream_path()?;
        let seed = parent.fork(child).ok_or_else(|| {
            Error::Unauthorized("project join child has no published parent fork".into())
        })?;
        seed.validate()?;
        if !seed.resources.iter().any(|resource| {
            matches!(
                (&resource.source, &resource.revision),
                (
                    crate::fork::ResourceRevision::Project { volume: source, .. },
                    crate::fork::ResourceRevision::Project { volume: forked, .. }
                ) if source == &self.volume && forked == child_project
            )
        }) {
            return Err(Error::Unauthorized(
                "project join is outside the published direct fork".into(),
            ));
        }
        Ok(())
    }

    fn authorize_direct_child_plan<A, O>(
        &self,
        parent: &Reducer,
        child: &Authority,
        child_project: &VolumeRef,
        plan: &ParentMergePlan<A, O>,
    ) -> Result<()> {
        self.authorize_direct_child(parent, child, child_project)?;
        if plan.child_project() != child_project {
            return Err(Error::Unauthorized(
                "project merge plan belongs to another direct child".into(),
            ));
        }
        Ok(())
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
            "checkout" if argv.iter().any(|arg| arg == "--") => self.require_write(),
            "branch" | "switch" | "checkout" => self.require_fork(),
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
            | GitCommand::CheckIgnore { .. }
            | GitCommand::Branch { create: None } => self.require_read(),
            GitCommand::Merge { .. }
            | GitCommand::MergeContinue
            | GitCommand::MergeAbort
            | GitCommand::Rebase { .. } => {
                self.require_write()?;
                self.require_capability("project:merge")
            }
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

fn terminal_from_join_outcome<A, O>(
    host: &super::FilesystemHost<A, O>,
    outcome: &JoinOutcome<A, O>,
) -> Result<ProjectMergeTerminal>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    match outcome {
        JoinOutcome::NoChanges(generation) => Ok(ProjectMergeTerminal::NoChanges {
            generation: host.generation_ref(generation)?,
        }),
        JoinOutcome::StaleTarget(generation) => Ok(ProjectMergeTerminal::StaleTarget {
            generation: host.generation_ref(generation)?,
        }),
        JoinOutcome::Conflicted {
            conflicts,
            truncated,
        } => Ok(ProjectMergeTerminal::Conflicted {
            conflicts: conflicts
                .iter()
                .map(|conflict| ProjectMergeTerminalConflict {
                    provider: host.provider.clone(),
                    key: encode_merge_conflict(conflict),
                })
                .collect(),
            truncated: *truncated,
        }),
        JoinOutcome::Fenced => Ok(ProjectMergeTerminal::Fenced),
        JoinOutcome::IdempotencyConflict => Ok(ProjectMergeTerminal::IdempotencyConflict),
        JoinOutcome::Applied(_) | JoinOutcome::AlreadyApplied(_) => Err(Error::Invalid(
            "applied provider result cannot be a terminal recovery result".into(),
        )),
    }
}

fn encode_merge_conflict(conflict: &MergeConflict) -> Vec<u8> {
    match conflict {
        MergeConflict::File(id) => {
            let mut key = vec![1];
            key.extend_from_slice(&id.into_bytes());
            key
        }
        MergeConflict::Binding { directory_id, name } => {
            let mut key = vec![2];
            key.extend_from_slice(&directory_id.into_bytes());
            key.push(match name.encoding() {
                acyclic_fs::kernel::NameEncoding::Utf8 => 1,
                acyclic_fs::kernel::NameEncoding::PosixBytes => 2,
                acyclic_fs::kernel::NameEncoding::WindowsUtf16Le => 3,
            });
            key.extend_from_slice(name.as_bytes());
            key
        }
    }
}

fn validate_merge_receipt_inputs(child: &Authority, notice: &ConversationMessage) -> Result<()> {
    if child.kind != crate::core::AggregateKind::Conversation {
        return Err(Error::Invalid(
            "project join child is not a conversation".into(),
        ));
    }
    child.stream_path()?;
    notice.validate()?;
    if notice.kind != crate::conversation::MessageKind::Merge {
        return Err(Error::Invalid("project join notice is not a merge".into()));
    }
    Ok(())
}

/// Resolves every file reference in a merge notice through the authenticated
/// Filesystem host before a provider join or recovery intent is admitted.
/// Metadata validation alone is insufficient: a forged descriptor or a
/// reclaimed generation must fail before any workspace mutation is attempted.
pub(crate) async fn validate_merge_notice_content<A, O>(
    host: &super::FilesystemHost<A, O>,
    verifier: &AuthorityVerifier,
    scope: &Scope,
    notice: &ConversationMessage,
) -> Result<()>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    let maximum_bytes = crate::conversation::MAX_LIMIT_FILE_BYTES;
    verify_notice_file(host, verifier, scope, &notice.content, maximum_bytes).await?;
    match &notice.attachments {
        ReferencedAttachments::Inline { items } => {
            for attachment in items {
                verify_notice_attachment(host, verifier, scope, attachment, maximum_bytes).await?;
            }
        }
        ReferencedAttachments::Manifest {
            manifest,
            item_count,
        } => {
            let bytes = read_notice_file(host, verifier, scope, manifest, maximum_bytes).await?;
            let items = decode_attachment_manifest(manifest, &bytes, *item_count)?;
            for attachment in &items {
                verify_notice_attachment(host, verifier, scope, attachment, maximum_bytes).await?;
            }
        }
    }
    for reference in notice.extensions.values() {
        verify_notice_file(host, verifier, scope, reference, maximum_bytes).await?;
    }
    Ok(())
}

async fn verify_notice_attachment<A, O>(
    host: &super::FilesystemHost<A, O>,
    verifier: &AuthorityVerifier,
    scope: &Scope,
    attachment: &Attachment,
    maximum_bytes: u64,
) -> Result<()>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    attachment.validate()?;
    verify_notice_file(host, verifier, scope, &attachment.file, maximum_bytes).await
}

async fn verify_notice_file<A, O>(
    host: &super::FilesystemHost<A, O>,
    verifier: &AuthorityVerifier,
    scope: &Scope,
    reference: &crate::conversation::FileRef,
    maximum_bytes: u64,
) -> Result<()>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    let _ = read_notice_file(host, verifier, scope, reference, maximum_bytes).await?;
    Ok(())
}

async fn read_notice_file<A, O>(
    host: &super::FilesystemHost<A, O>,
    verifier: &AuthorityVerifier,
    scope: &Scope,
    reference: &crate::conversation::FileRef,
    maximum_bytes: u64,
) -> Result<Vec<u8>>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
{
    let grant = ContentGrant::verify_read(verifier, scope, reference)?;
    let bytes = host.read_content(reference, &grant, maximum_bytes).await?;
    let owned = bytes.to_vec();
    reference.descriptor().verify(&owned)?;
    Ok(owned)
}

fn map_run_error<S, E>(error: &GitCompatRunError<S, E>) -> Error
where
    S: std::error::Error,
    E: std::error::Error,
{
    Error::Storage(error.to_string())
}
