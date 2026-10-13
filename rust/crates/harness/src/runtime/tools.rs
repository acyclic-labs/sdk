use super::*;

/// An admission decision for one exact, schema-valid tool invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ToolPolicyDecision {
    /// The invocation may proceed under its existing capability grant.
    Allow,
    /// The invocation is rejected before dispatch.
    Deny {
        /// Explanation retained with the denied decision.
        reason: String,
    },
    /// An addressable approval must resolve for this exact action.
    RequireApproval {
        /// Ref-free text used to create the approval request.
        prompt: String,
    },
}

/// Replaceable policy evaluated after schema and scope validation, before dispatch.
pub trait ToolPolicy: acyclic_stream::ProviderPlatform {
    /// Immutable implementation identity pinned across admission and replay.
    fn identity(&self) -> ComponentIdentity;
    /// Policy implementations must be deterministic for an admitted revision.
    fn evaluate<'a>(
        &'a self,
        invocation: &'a ToolInvocation,
        scope: &'a RuntimeScope,
    ) -> BoxFuture<'a, Result<ToolPolicyDecision>>;

    /// Rejects opaque policies before any native callback or future allocation.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_policy(&self, _work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "policy has no original-ledger implementation",
        })
    }

    /// Returns the owned identity with custody for its inline Self and strings.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn identity_with_native_work(
        &self,
        _work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ComponentIdentity, acyclic_fs::NativeAllocation)> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "policy has no original-ledger identity",
        })
    }

    /// Evaluates the same predicate against the originally issued scope.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn evaluate_with_native_work<'a>(
        &'a self,
        _invocation: &'a ToolInvocation,
        _scope: &'a RuntimeScope,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (ToolPolicyDecision, acyclic_fs::NativeAllocation)>>
    {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "policy has no original-ledger evaluator",
        })
    }
}

pub(crate) fn validate_policy_identity(
    identity: &ComponentIdentity,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    inspect_task_metadata(io, 2)?;
    for (value, reason) in [
        (&identity.name, "policy name is invalid"),
        (&identity.version, "policy version is invalid"),
    ] {
        if !crate::contract::is_valid_component_label(value) {
            return Err(task_workflow_error(
                task_original_work(io)?,
                crate::wire::ErrorCode::Invalid,
                reason,
            ));
        }
    }
    if identity.digest == [0; 32] {
        return Err(task_workflow_error(
            task_original_work(io)?,
            crate::wire::ErrorCode::Invalid,
            "policy implementation digest is empty",
        ));
    }
    Ok(())
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "the terminal outcome is consumed by this admission boundary"
)]
pub(crate) fn check_tool_approval(outcome: InteractionOutcome) -> Result<()> {
    match outcome {
        InteractionOutcome::Approved => Ok(()),
        InteractionOutcome::Indeterminate { operation_id } => {
            Err(Error::Indeterminate(operation_id))
        }
        InteractionOutcome::Declined => Err(Error::InteractionRejected(
            crate::InteractionRejection::Declined,
        )),
        InteractionOutcome::Cancelled => Err(Error::InteractionRejected(
            crate::InteractionRejection::Cancelled,
        )),
        InteractionOutcome::Expired => Err(Error::InteractionRejected(
            crate::InteractionRejection::Expired,
        )),
        InteractionOutcome::Denied => Err(Error::InteractionRejected(
            crate::InteractionRejection::Denied,
        )),
        InteractionOutcome::Answered { .. } => Err(Error::Conflict(
            "approval resolved with a question answer".into(),
        )),
    }
}

/// Binds a paid prompt to the canonical tool action.
pub(crate) fn tool_approval_request(
    task: TaskId,
    policy: &ComponentIdentity,
    definition: &ToolDefinition,
    invocation: &ToolInvocation,
    paid: (String, Option<NativeCustody>),
    work: Option<&NativeWork>,
) -> Result<((OperationId, Interaction), Option<NativeCustody>)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        if !paid.1.as_ref().is_some_and(|custody| {
            custody.work().shares_ledger(work) && custody.bytes() >= paid.0.capacity() as u64
        }) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "approval prompt does not retain its original ledger",
            });
        }
        work.remaining()?;
        if paid.0.trim().is_empty() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "approval prompt is empty",
            });
        }
    }
    let (operation, digest) = tool_approval_binding(task, policy, definition, invocation, work)?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.is_some() {
        if digest == [0; 32] {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "approval action digest is empty",
            });
        }
        return Ok((
            (
                operation,
                Interaction::Approval {
                    prompt: paid.0,
                    operation_id: invocation.operation_id,
                    action_digest: digest,
                },
            ),
            paid.1,
        ));
    }
    Ok((
        (
            operation,
            Interaction::approval(paid.0, invocation.operation_id, digest)?,
        ),
        paid.1,
    ))
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) type NativeWork = acyclic_fs::NativeMaterializationWork;

#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
pub(super) type NativeWork = ();

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn validate_native_policy_identity(
    paid: &(ComponentIdentity, Option<NativeCustody>),
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    let custody = paid.1.as_ref().ok_or(Error::NativeBoundary {
        code: crate::wire::ErrorCode::Conflict,
        reason: "policy identity does not retain its original ledger",
    })?;
    validate_policy_identity_custody_with_native_work(&paid.0, custody, work)
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn validate_policy_identity_custody_with_native_work(
    identity: &ComponentIdentity,
    custody: &acyclic_fs::NativeAllocation,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    let bytes = (std::mem::size_of::<ComponentIdentity>() as u64)
        .checked_add(identity.name.capacity() as u64)
        .and_then(|bytes| bytes.checked_add(identity.version.capacity() as u64));
    if !custody.work().shares_ledger(work) || bytes.is_none_or(|bytes| custody.bytes() < bytes) {
        return Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Conflict,
            reason: "policy identity does not retain its original ledger",
        });
    }
    work.remaining()?;
    Ok(())
}

/// Shared SDK startup boundary for already-owned policy metadata.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn validate_policy_identity_with_native_work(
    identity: &ComponentIdentity,
    custody: &acyclic_fs::NativeAllocation,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    validate_policy_identity_custody_with_native_work(identity, custody, work)?;
    let io = crate::filesystem::PreparationIo::from_native_work(work)?;
    validate_policy_identity(identity, Some(&io))
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn validate_native_policy_decision(
    paid: &(ToolPolicyDecision, Option<NativeCustody>),
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    let bytes = match &paid.0 {
        ToolPolicyDecision::Allow => 0,
        ToolPolicyDecision::Deny { reason } => reason.capacity() as u64,
        ToolPolicyDecision::RequireApproval { prompt } => prompt.capacity() as u64,
    };
    if !paid
        .1
        .as_ref()
        .is_some_and(|custody| custody.work().shares_ledger(work) && custody.bytes() >= bytes)
    {
        return Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Conflict,
            reason: "policy decision does not retain its original ledger",
        });
    }
    work.remaining()?;
    Ok(())
}

pub(crate) fn tool_approval_binding(
    task: TaskId,
    policy: &ComponentIdentity,
    definition: &ToolDefinition,
    invocation: &ToolInvocation,
    work: Option<&NativeWork>,
) -> Result<(OperationId, [u8; 32])> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let io = work
        .map(crate::filesystem::PreparationIo::from_native_work)
        .transpose()?;
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let io = None;
    validate_policy_identity(policy, io.as_ref())?;
    crate::executor::with_history_io(io.as_ref(), |observed| {
        ToolInvocation::validate_identity(&invocation.call_id, Some(&invocation.name), observed)
    })?;
    let digest =
        crate::executor::preparation_digest(&(task, policy, definition, invocation), io.as_ref())?;
    let prefix = b"harness:tool-approval:v2";
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        work.charge(acyclic_fs::WorkCounters {
            bytes_hashed: (prefix.len() + digest.len()) as u64,
            ..Default::default()
        })?;
    }
    let mut hash = blake3::Hasher::new();
    hash.update(prefix);
    hash.update(&digest);
    let mut id_bytes = [0; 16];
    id_bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    Ok((OperationId::from_bytes(id_bytes), digest))
}

/// A task context bound to one exact tool call identity.
pub struct ToolContext {
    pub(super) task: Arc<TaskContext>,
    pub(super) operation_id: OperationId,
    pub(super) call_id: String,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) native_interactions: Option<Arc<dyn InteractionRouter>>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) effective_scope: Option<(RuntimeScope, Option<acyclic_fs::NativeAllocation>)>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) accepted_native_tool: std::sync::OnceLock<()>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) native_custody: Option<acyclic_fs::NativeAllocation>,
}

impl Clone for ToolContext {
    fn clone(&self) -> Self {
        Self {
            task: self.task.clone(),
            operation_id: self.operation_id,
            call_id: self.call_id.clone(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_interactions: self.native_interactions.clone(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            effective_scope: self
                .effective_scope
                .as_ref()
                .map(|(scope, _)| (scope.clone(), None)),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            accepted_native_tool: std::sync::OnceLock::new(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_custody: None,
        }
    }
}

impl ToolContext {
    /// Creates a tool context with separate runtime and provider call identities.
    pub fn new(
        task: Arc<TaskContext>,
        operation_id: OperationId,
        call_id: &str,
        io: Option<&crate::conversation::ContentIo>,
    ) -> Result<Self> {
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        if io.is_some() {
            return Err(Error::Unsupported(
                "native tool context is unavailable".into(),
            ));
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let (call_id, native_custody) = if let Some(io) = io {
            let work = io.original_native_work().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "tool context requires original native work",
            })?;
            if !task.native_metadata_matches(work) {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "tool context differs from its original native metadata ledger",
                });
            }
            io.check_cancellation()?;
            crate::executor::with_history_io(Some(io), |work| {
                ToolInvocation::validate_identity(call_id, None, work)
            })?;
            let mut slot = work
                .claim(
                    std::mem::size_of::<Self>() as u64,
                    0,
                    acyclic_fs::WorkCounters::UNCHARGED,
                )
                .map_err(Error::from)?;
            let (call_id, mut custody) = work.display(call_id).map_err(Error::from)?;
            custody
                .absorb(&mut slot)
                .map_err(|cause| Error::NativeWork {
                    cause,
                    custody: None,
                })?;
            (call_id, Some(custody))
        } else {
            if task.native_metadata_source.is_some() {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "native tool context requires its original work",
                });
            }
            (call_id.to_owned(), None)
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let call_id = call_id.to_owned();
        if call_id.trim().is_empty() {
            return Err(Error::Invalid("tool call ID is empty".into()));
        }
        Ok(Self {
            task,
            operation_id,
            call_id,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_interactions: None,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            effective_scope: None,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            accepted_native_tool: std::sync::OnceLock::new(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_custody,
        })
    }

    /// Returns the enclosing scoped task context.
    #[must_use]
    pub fn task(&self) -> &TaskContext {
        self.task.as_ref()
    }

    /// Borrows this tool's attenuated authority without replacing its task.
    #[must_use]
    pub fn effective_scope(&self) -> &RuntimeScope {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some((scope, _)) = &self.effective_scope {
            return scope;
        }
        self.task().scope()
    }

    /// Returns pending approval through this command's protected route.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) async fn authorize_tool(
        &self,
        definition: &ToolDefinition,
        invocation: &ToolInvocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<OperationId>> {
        let task = self.task();
        task.require_original_native_work(work)?;
        task.require_native_scope_restriction(self.effective_scope(), work)?;
        if invocation.operation_id != self.operation_id() || invocation.call_id != self.call_id() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "native policy invocation differs from its original command",
            });
        }
        task.authorize_tool_policies(
            definition,
            invocation,
            Some(work),
            self.stock_native_interactions(),
            self.effective_scope(),
        )
        .await
    }

    /// Returns the exact call identity.
    #[must_use]
    pub fn call_id(&self) -> &str {
        &self.call_id
    }

    /// Returns the stable effect identity for execution and reconciliation.
    #[must_use]
    pub const fn operation_id(&self) -> OperationId {
        self.operation_id
    }
}

impl std::ops::Deref for ToolContext {
    type Target = TaskContext;

    fn deref(&self) -> &Self::Target {
        self.task()
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl ToolContext {
    /// Checks effective READ and limits before replay or content IO.
    pub fn authorize_file_read_with_native_work(
        &self,
        file: &FileRef,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.task().authorize_file_read(
            file,
            self.effective_scope(),
            Some(&crate::filesystem::PreparationIo::from_native_work(work)?),
        )
    }

    /// Checks effective WRITE through the original bound destination.
    pub fn authorize_content_publication_with_native_work(
        &self,
        source: Option<&FileRef>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let task = self.task();
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        if let Some(source) = source {
            task.authorize_file_read(source, self.effective_scope(), Some(&io))?;
        }
        let writer = task.bound_content_writer(
            task.content_binding(source.map(FileRef::volume)),
            self.effective_scope(),
            Some(&io),
        )?;
        if source.is_some_and(|source| source.volume() != writer.volume()) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "source differs from the owner-bound destination",
            });
        }
        Ok(())
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl ToolContext {
    /// Checks the SDK-issued native dispatch context before MCP allocation or IO.
    pub fn validate_native_mcp_context(&self, work: &NativeWork) -> Result<()> {
        let context = self.validate_native_bound_context(work)?;
        if !context
            .native_metadata_source
            .as_ref()
            .is_some_and(|source| source.purpose == IssuedContextPurpose::Dispatch)
        {
            return Err(native_context::boundary(
                crate::wire::ErrorCode::Unauthorized,
                "native context purpose does not permit MCP dispatch",
            ));
        }
        Ok(())
    }

    /// Checks a genuine owner-bound native reconciliation context.
    pub fn validate_native_mcp_reconciliation_context(&self, work: &NativeWork) -> Result<()> {
        self.validate_native_bound_context(work).map(|_| ())
    }

    pub(super) fn validate_native_bound_context(&self, work: &NativeWork) -> Result<&TaskContext> {
        work.charge(acyclic_fs::WorkCounters::default())
            .map_err(|cause| Error::NativeWork {
                cause,
                custody: None,
            })?;
        if !self.native_metadata_matches(work) {
            return Err(native_context::boundary(
                crate::wire::ErrorCode::Unauthorized,
                "MCP context has no original native tool metadata source",
            ));
        }
        let Some(shared) = self.native_task() else {
            return Err(native_context::boundary(
                crate::wire::ErrorCode::Unauthorized,
                "MCP context is not the original shared native task",
            ));
        };
        let context = shared.as_ref();
        context.require_original_native_work(work)?;
        let Some((task, fence)) = context
            .owned_mailbox
            .as_ref()
            .map(|owner| owner.task_binding())
        else {
            return Err(native_context::boundary(
                crate::wire::ErrorCode::Unauthorized,
                "MCP context has no borrowed original owner binding",
            ));
        };
        if !context.original_task_binding_matches(task, fence) {
            return Err(native_context::boundary(
                crate::wire::ErrorCode::Unauthorized,
                "MCP context differs from its original task and fence",
            ));
        }
        Ok(context)
    }
}

impl ToolContext {
    /// Observes the original publication under the effective tool scope.
    pub async fn observe_staged_file_once(
        &self,
        operation_id: OperationId,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(Option<FileRef>, u64)> {
        self.task()
            .observe_bound_file_once(operation_id, io, self.effective_scope())
            .await
    }

    /// Publishes once under the effective tool scope and caller IO limits.
    pub async fn stage_file_once(
        &self,
        operation_id: OperationId,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(FileRef, u64)> {
        let task = self.task();
        task.stage_bound_file(
            task.content_binding(None),
            operation_id,
            path,
            bytes,
            media_type,
            display_name,
            super::content_write::ContentPublication::OperationBound,
            self.effective_scope(),
            io,
        )
        .await
    }

    /// Replaces the pinned source under the effective tool scope and caller IO limits.
    pub async fn stage_file_at(
        &self,
        operation_id: OperationId,
        source: &FileRef,
        bytes: &[u8],
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(FileRef, u64)> {
        let task = self.task();
        task.stage_bound_file(
            task.content_binding(Some(source.volume())),
            operation_id,
            source.path(),
            bytes,
            source.descriptor().media_type(),
            source.display_name(),
            super::content_write::ContentPublication::Replacement(source),
            self.effective_scope(),
            io,
        )
        .await
    }

    /// Reads immutable content under the effective tool scope and caller IO limits.
    pub async fn read_file(
        &self,
        file: &FileRef,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let task = self.task();
        task.read_bound_file(
            task.content_binding(Some(file.volume())),
            file,
            self.effective_scope(),
            io,
        )
        .await
    }
}
