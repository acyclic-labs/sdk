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
        &self, _work: &acyclic_fs::NativeMaterializationWork,
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
    ) -> Result<crate::NativeFutureBundle<'a, (ToolPolicyDecision, acyclic_fs::NativeAllocation)>> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "policy has no original-ledger evaluator",
        })
    }
}

pub(crate) fn validate_policy_identity(identity: &ComponentIdentity) -> Result<()> {
    validate_component_label(&identity.name, "policy name")?;
    validate_component_label(&identity.version, "policy version")?;
    if identity.digest == [0; 32] {
        return Err(Error::Invalid(
            "policy implementation digest is empty".into(),
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

/// Binds typed and stock tool approvals to the same runtime-owned action.

pub(crate) fn tool_approval_request(
    task: TaskId,
    policy: &ComponentIdentity,
    definition: &ToolDefinition,
    invocation: &ToolInvocation,
    prompt: String,
) -> Result<(OperationId, Interaction)> {
    let (operation, digest) = tool_approval_binding(task, policy, definition, invocation)?;
    Ok((
        operation,
        Interaction::approval(prompt, invocation.operation_id, digest)?,
    ))
}

/// Moves an already-paid policy prompt into the exact canonical approval action.

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn tool_approval_request_with_native_work(
    task: TaskId,
    policy: &ComponentIdentity,
    definition: &ToolDefinition,
    invocation: &ToolInvocation,
    prompt: String,
    prompt_custody: acyclic_fs::NativeAllocation,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<((OperationId, Interaction), acyclic_fs::NativeAllocation)> {
    let paid = (prompt, prompt_custody);
    if !paid.1.work().shares_ledger(work)
        || paid.1.bytes() < paid.0.capacity() as u64
    {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
            reason: "approval prompt does not retain its original ledger" });
    }
    work.remaining()?;
    if paid.0.trim().is_empty() {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
            reason: "approval prompt is empty" });
    }
    let (operation, digest) = tool_approval_binding_with_native_work(task, policy, definition, invocation, work)?;
    if digest == [0; 32] {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
            reason: "approval action digest is empty" });
    }
    let (prompt, custody) = paid;
    Ok(((operation, Interaction::Approval {
        prompt, operation_id: invocation.operation_id, action_digest: digest,
    }), custody))
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) type NativeWork = acyclic_fs::NativeMaterializationWork;

#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
pub(super) type NativeWork = ();

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn validate_native_policy_identity(
    paid: &(ComponentIdentity, Option<NativeCustody>), work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    let custody = paid.1.as_ref().ok_or(Error::NativeBoundary {
        code: crate::wire::ErrorCode::Conflict,
        reason: "policy identity does not retain its original ledger",
    })?;
    validate_policy_identity_custody_with_native_work(&paid.0, custody, work)
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn validate_policy_identity_custody_with_native_work(
    identity: &ComponentIdentity, custody: &acyclic_fs::NativeAllocation,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    let bytes = (std::mem::size_of::<ComponentIdentity>() as u64)
        .checked_add(identity.name.capacity() as u64)
        .and_then(|bytes| bytes.checked_add(identity.version.capacity() as u64));
    if !custody.work().shares_ledger(work) || bytes.is_none_or(|bytes| custody.bytes() < bytes) {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
            reason: "policy identity does not retain its original ledger" });
    }
    work.remaining()?;
    Ok(())
}

/// Shared SDK startup boundary for already-owned policy metadata.

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn validate_policy_identity_with_native_work(
    identity: &ComponentIdentity, custody: &acyclic_fs::NativeAllocation,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    validate_policy_identity_custody_with_native_work(identity, custody, work)?;
    if !crate::contract::is_valid_component_label(&identity.name) {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
            reason: "policy name is invalid" });
    }
    if !crate::contract::is_valid_component_label(&identity.version) {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
            reason: "policy version is invalid" });
    }
    if identity.digest == [0; 32] {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
            reason: "policy implementation digest is empty" });
    }
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn validate_native_policy_decision(
    paid: &(ToolPolicyDecision, Option<NativeCustody>), work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    let bytes = match &paid.0 {
        ToolPolicyDecision::Allow => 0,
        ToolPolicyDecision::Deny { reason } => reason.capacity() as u64,
        ToolPolicyDecision::RequireApproval { prompt } => prompt.capacity() as u64,
    };
    if !paid.1.as_ref().is_some_and(|custody| custody.work().shares_ledger(work)
        && custody.bytes() >= bytes)
    {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
            reason: "policy decision does not retain its original ledger" });
    }
    work.remaining()?;
    Ok(())
}

/// Derives retained approval authority without evaluating or dispatching a policy.

pub(crate) fn tool_approval_binding(
    task: TaskId,
    policy: &ComponentIdentity,
    definition: &ToolDefinition,
    invocation: &ToolInvocation,
) -> Result<(OperationId, [u8; 32])> {
    tool_approval_binding_with_work(task, policy, definition, invocation, None)
}

/// Canonical passive wake identity: no policy evaluation or prompt construction.

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn tool_approval_binding_with_native_work(
    task: TaskId, policy: &ComponentIdentity, definition: &ToolDefinition,
    invocation: &ToolInvocation, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(OperationId, [u8; 32])> {
    tool_approval_binding_with_work(task, policy, definition, invocation, Some(work))
}

pub(super) fn tool_approval_binding_with_work(
    task: TaskId, policy: &ComponentIdentity, definition: &ToolDefinition,
    invocation: &ToolInvocation, work: Option<&NativeWork>,
) -> Result<(OperationId, [u8; 32])> {
    let digest = match work {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        Some(work) => {
            work.remaining()?;
            if !crate::contract::is_valid_component_label(&policy.name)
                || !crate::contract::is_valid_component_label(&policy.version)
                || policy.digest == [0; 32]
                || !crate::contract::is_valid_component_label(&invocation.name)
                || invocation.call_id.is_empty()
                || invocation.call_id.len() > crate::COMPONENT_LABEL_MAX_BYTES
                || invocation.call_id.chars().any(char::is_control)
                || invocation.call_id.contains(crate::contract::COMPONENT_LABEL_FORBIDDEN_SEPARATORS)
                || crate::contract::COMPONENT_LABEL_FORBIDDEN_EXACT.contains(&invocation.call_id.as_str())
            {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "invalid tool approval identity" });
            }
            let mut io = crate::store::history::native_work_io(work)?;
            let mut observed: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut io);
            crate::contract::canonical_json_digest_with_work(
                &(task, policy, definition, invocation), work.remaining()?.bytes_encoded, &mut observed,
            )?
        }
        _ => {
            validate_policy_identity(policy)?;
            ToolInvocation::validate_identity(&invocation.call_id, Some(&invocation.name), &mut None)?;
            crate::contract::canonical_json_digest(&(task, policy, definition, invocation))?
        }
    };
    let prefix = b"harness:tool-approval:v2";
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        work.charge(acyclic_fs::WorkCounters {
            bytes_hashed: (prefix.len() + digest.len()) as u64, ..Default::default()
        })?;
    }
    let mut hash = blake3::Hasher::new();
    hash.update(prefix);
    hash.update(&digest);
    let mut id_bytes = [0; 16];
    id_bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    Ok((OperationId::from_bytes(id_bytes), digest))
}

#[derive(Clone)]
pub(super) enum ToolTaskContext {
    Ordinary(TaskContext),
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    Native(Arc<NativeTaskContext>),
}

/// A task context bound to one exact tool call identity.

pub struct ToolContext {
    pub(super) task: ToolTaskContext,
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
            effective_scope: self.effective_scope.as_ref().map(|(scope, _)| (scope.clone(), None)),
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
        task: TaskContext,
        operation_id: OperationId,
        call_id: impl Into<String>,
    ) -> Result<Self> {
        let call_id = call_id.into();
        if call_id.trim().is_empty() {
            return Err(Error::Invalid("tool call ID is empty".into()));
        }
        Ok(Self {
            task: ToolTaskContext::Ordinary(task),
            operation_id,
            call_id,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_interactions: None,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            effective_scope: None,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            accepted_native_tool: std::sync::OnceLock::new(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_custody: None,
        })
    }

    /// Returns the enclosing scoped task context.
    #[must_use]
    pub fn task(&self) -> &TaskContext {
        match &self.task {
            ToolTaskContext::Ordinary(task) => task,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            ToolTaskContext::Native(task) => task.context(),
        }
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
        &self, definition: &ToolDefinition, invocation: &ToolInvocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<OperationId>> {
        let task = self.task();
        task.require_original_native_work(work)?;
        task.require_native_scope_restriction(self.effective_scope(), work)?;
        if invocation.operation_id != self.operation_id() || invocation.call_id != self.call_id() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "native policy invocation differs from its original command" });
        }
        task.authorize_tool_policies(
            definition, invocation, Some(work), self.stock_native_interactions(), self.effective_scope(),
        ).await
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
    pub fn authorize_file_read_with_native_work(&self, file: &FileRef,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.task().authorize_native_file_read(file, self.effective_scope(),
            &crate::filesystem::PreparationIo::from_native_work(work)?)
    }

    /// Checks effective WRITE through the original bound destination.
    pub fn authorize_content_publication_with_native_work(&self, source: Option<&FileRef>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let task = self.task();
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        if let Some(source) = source { task.authorize_native_file_read(source, self.effective_scope(), &io)?; }
        let writer = task.bound_content_writer(task.content_binding(source.map(FileRef::volume)),
            self.effective_scope(), Some(&io))?;
        if source.is_some_and(|source| source.volume() != writer.volume()) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "source differs from the owner-bound destination" });
        }
        Ok(())
    }

    /// Reads original content under the effective tool scope.
    pub async fn read_file_with_native_work(&self, file: &FileRef,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_stream::OwnedBytes> {
        self.task().read_native_file(file, self.effective_scope(), work).await
    }

    /// Publishes once under the effective tool scope.
    pub async fn stage_file_once_with_native_work(&self, operation_id: OperationId, path: &str,
        bytes: &[u8], media_type: &str, display_name: &str, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(FileRef, acyclic_fs::NativeAllocation)> {
        self.task().stage_native_file(operation_id, path, bytes, media_type, display_name, None,
            self.effective_scope(), work).await
    }

    /// Replaces the pinned source generation under the effective tool scope.
    pub async fn stage_file_at_with_native_work(&self, operation_id: OperationId, source: &FileRef,
        bytes: &[u8], work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(FileRef, acyclic_fs::NativeAllocation)> {
        self.task().stage_native_file(operation_id, source.path(), bytes, source.descriptor().media_type(),
            source.display_name(), Some(source), self.effective_scope(), work).await
    }

    /// Observes the original publication under effective WRITE and limits.
    pub async fn observe_staged_file_once_with_native_work(&self, operation_id: OperationId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Option<FileRef>, acyclic_fs::NativeAllocation)> {
        let task = self.task();
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let scope = self.effective_scope();
        let writer = task.bound_content_writer(task.content_binding(None), scope, Some(&io))?;
        task.scope.verify_created_write(writer.volume(), Some(&io)).await?;
        let (file, hold) = writer.observe_staged_once(operation_id, scope.limits.file_bytes, Some(&io))?.await?;
        task.scope.verify_created_write(writer.volume(), Some(&io)).await?;
        if let Some(file) = &file {
            scope.limits.validate_file(file, &mut Some(&mut io.history_io()))?;
            if file.volume() != writer.volume() {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Storage,
                    reason: "content publisher observed another volume" });
            }
        }
        let retained = hold.as_ref().map_or(0, |hold| hold.bytes());
        if let Some(hold) = hold { hold.into_retained(); }
        Ok((file, io.take_native_custody(retained)?))
    }
}
