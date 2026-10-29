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
    /// Evaluates a policy on the caller's original IO ledger.
    fn evaluate<'a>(
        &'a self,
        invocation: &'a ToolInvocation,
        scope: &'a RuntimeScope,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::store::HistoryContentFuture<'a, (ToolPolicyDecision, crate::NativeCustody)>>;

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
}

pub(crate) fn validate_policy_identity(
    identity: &ComponentIdentity,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    crate::model::charge_items(io, 2)?;
    for (value, reason) in [
        (&identity.name, "policy name is invalid"),
        (&identity.version, "policy version is invalid"),
    ] {
        if !crate::contract::is_valid_component_label(value) {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                reason,
            ));
        }
    }
    if identity.digest == [0; 32] {
        return Err(crate::executor::stock_boundary(
            io,
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
        InteractionOutcome::Approved {} => Ok(()),
        InteractionOutcome::Indeterminate { operation_id } => {
            Err(Error::Indeterminate(operation_id))
        }
        InteractionOutcome::Declined {} => Err(Error::InteractionRejected(
            crate::InteractionRejection::Declined,
        )),
        InteractionOutcome::Cancelled {} => Err(Error::InteractionRejected(
            crate::InteractionRejection::Cancelled,
        )),
        InteractionOutcome::Expired {} => Err(Error::InteractionRejected(
            crate::InteractionRejection::Expired,
        )),
        InteractionOutcome::Denied {} => Err(Error::InteractionRejected(
            crate::InteractionRejection::Denied,
        )),
        InteractionOutcome::Answered { .. } => Err(Error::Conflict(
            "approval resolved with a question answer".into(),
        )),
    }
}

/// Binds a paid prompt to the canonical tool action.
#[doc(hidden)]
pub fn tool_approval_request(
    task: TaskId,
    policy: &ComponentIdentity,
    definition: &ToolDefinition,
    invocation: &ToolInvocation,
    paid: (String, NativeCustody),
    work: Option<&NativeWork>,
    route: crate::interaction::ApprovalRoute,
) -> Result<((OperationId, Interaction), NativeCustody)> {
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
    if digest == [0; 32] {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work.is_some() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "approval action digest is empty",
            });
        }
        return Err(Error::Invalid("approval action digest is empty".into()));
    }

    let (prompt, custody) = paid;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let mut custody = custody;
    let subject = copy_approval_subject(invocation, work, route)?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let mut subject = subject;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(copied) = subject.1.as_mut() {
        custody
            .as_mut()
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "approval prompt does not retain its original ledger",
            })?
            .absorb(copied)?;
    }
    Ok((
        (
            operation,
            Interaction::approval(prompt, invocation.operation_id, digest, subject.0)?,
        ),
        custody,
    ))
}

fn copy_approval_subject(
    invocation: &ToolInvocation,
    work: Option<&NativeWork>,
    route: crate::interaction::ApprovalRoute,
) -> Result<(ApprovalSubject, NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        use crate::store::history::HistoryIo as _;
        let remaining = work.remaining()?;
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        );
        let tool = io.clone_text(&invocation.name)?;
        let call_id = io.clone_text(&invocation.call_id)?;
        let arguments = crate::executor::json::json_value_copy_bounded(
            &invocation.arguments,
            crate::executor::json::JsonBounds {
                maximum_bytes: remaining.bytes_encoded,
                maximum_items: remaining.items_examined,
                maximum_depth: crate::executor::json::RECORD_MAXIMUM_DEPTH,
            },
            Some(crate::wire::ErrorCode::Invalid),
            &mut |charge| io.codec_charge(charge),
        )?;
        return Ok((
            ApprovalSubject::Tool {
                route,
                tool,
                call_id,
                arguments,
            },
            Some(io.into_custody()?),
        ));
    }
    let _ = work;
    Ok((
        ApprovalSubject::Tool {
            route,
            tool: invocation.name.clone(),
            call_id: invocation.call_id.clone(),
            arguments: invocation.arguments.clone(),
        },
        None,
    ))
}

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
#[path = "tools/approval_tests.rs"]
mod approval_tests;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) type NativeWork = acyclic_fs::NativeMaterializationWork;

#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
pub(super) type NativeWork = ();

// Negative-control seam for `hooks::mcp_identity_tests`: substitutes the
// approval subject. It lives beside its only use so it exists in every build
// that compiles this function, whatever features gate the tests reading it.
#[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
thread_local! {
    #[doc(hidden)]
    pub static SUBSTITUTE_APPROVAL_NAME: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(crate) fn tool_approval_binding(
    task: TaskId,
    policy: &ComponentIdentity,
    definition: &ToolDefinition,
    invocation: &ToolInvocation,
    work: Option<&NativeWork>,
) -> Result<(OperationId, [u8; 32])> {
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = work;
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
    #[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
    let substituted = SUBSTITUTE_APPROVAL_NAME
        .with(std::cell::Cell::get)
        .then(|| {
            let mut substituted = invocation.clone();
            substituted.name = "mcp__docs__search".into();
            substituted
        });
    #[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
    let invocation = substituted.as_ref().unwrap_or(invocation);
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

/// An agent turn context bound to one exact tool call identity.
#[derive(Clone)]
pub struct ToolContext {
    pub(super) task: Arc<TaskContext>,
    pub(super) operation_id: OperationId,
    pub(super) call_id: String,
    #[doc(hidden)]
    pub model_turn: (OperationId, u32),
}

impl ToolContext {
    /// Creates a tool context with separate runtime and provider call identities.
    pub fn new(task: Arc<TaskContext>, operation_id: OperationId, call_id: &str) -> Result<Self> {
        if call_id.trim().is_empty() {
            return Err(Error::Invalid("tool call ID is empty".into()));
        }
        Ok(Self {
            task,
            operation_id,
            call_id: call_id.to_owned(),
            // A directly issued call owns a standalone turn. The stock executor
            // binds model-issued calls to their enclosing execution and step.
            model_turn: (operation_id, 0),
        })
    }

    /// Returns the enclosing scoped turn context.
    #[must_use]
    pub fn task(&self) -> &TaskContext {
        self.task.as_ref()
    }

    /// Borrows this tool's authority.
    #[must_use]
    pub fn effective_scope(&self) -> &RuntimeScope {
        self.task().scope()
    }

    /// The operation of the model turn this call belongs to; outside a
    /// model turn, the call's own operation.
    #[must_use]
    pub const fn turn_id(&self) -> OperationId {
        self.model_turn.0
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

    /// Original model call identity to retain with a reconstructable request.
    #[must_use]
    pub fn settlement_query(&self) -> crate::executor::ToolSettlementQuery<'_> {
        crate::executor::ToolSettlementQuery {
            turn: self.model_turn.0,
            step: self.model_turn.1,
            call_id: &self.call_id,
            effect: self.operation_id,
        }
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
        self.task()
            .authorize_file_read(
                file,
                self.effective_scope(),
                self.effective_scope().grants(),
                Some(&crate::filesystem::PreparationIo::from_native_work(work)?),
            )
            .map(|_| ())
    }

    /// Checks effective WRITE through the original bound destination.
    pub fn authorize_content_publication_with_native_work(
        &self,
        path: &str,
        source: Option<&FileRef>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let task = self.task();
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        task.authorize_content_publication(path, source, self.effective_scope(), Some(&io))
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
            None,
            io,
        )
        .await
    }
    /// Reads one authenticated interval under this tool call's effective scope.
    pub async fn read_file_range(
        &self,
        file: &FileRef,
        range: acyclic_fs::ByteRange,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        self.task()
            .read_bound_range(
                file,
                range,
                self.effective_scope(),
                self.effective_scope().grants(),
                io,
            )
            .await
    }
}
