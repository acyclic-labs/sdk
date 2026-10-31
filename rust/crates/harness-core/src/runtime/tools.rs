use super::*;

/// An admission decision for one exact, schema-valid tool invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ToolPolicyDecision {
    /// The invocation may proceed under its existing capability grant.
    Allow,
    /// The invocation is rejected before dispatch and its refusal is returned
    /// to the model, which may choose another action.
    Deny {
        /// Explanation retained with the denied decision.
        reason: String,
    },
    /// An addressable approval must resolve for this exact action. A refusal
    /// returns its reason through the ordinary tool-failure result. A changed
    /// approval after dispatch remains indeterminate and never replays the effect.
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
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        self.authorize_tool_path_publication(path, source, Some(&io))
    }
}

impl ToolContext {
    /// Observes the original private publication under the effective tool scope.
    pub async fn observe_staged_file_once(
        &self,
        operation_id: OperationId,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(Option<FileRef>, u64)> {
        self.observe_once(self.task().content_binding(None), operation_id, io)
            .await
    }

    /// Observes the original publication of a file tool's write, which
    /// lands where the task's commands write.
    pub async fn observe_tool_file_once(
        &self,
        operation_id: OperationId,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(Option<FileRef>, u64)> {
        self.observe_once(self.task().tool_content(None), operation_id, io)
            .await
    }

    pub(crate) async fn observe_tool_source_file_once(
        &self,
        operation_id: OperationId,
        source: Option<&FileRef>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(Option<FileRef>, u64)> {
        self.observe_once(
            self.task().tool_content(source.map(FileRef::volume)),
            operation_id,
            io,
        )
        .await
    }

    async fn observe_once(
        &self,
        binding: Option<&ContentBindings>,
        operation_id: OperationId,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(Option<FileRef>, u64)> {
        self.task()
            .observe_bound_file_once(binding, operation_id, io, self.effective_scope())
            .await
    }

    /// Publishes once on the private volume under the effective tool scope
    /// and caller IO limits.
    pub async fn stage_file_once(
        &self,
        operation_id: OperationId,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(FileRef, u64)> {
        let binding = self.task().content_binding(None);
        self.stage_once(
            binding,
            operation_id,
            path,
            bytes,
            media_type,
            display_name,
            io,
        )
        .await
    }

    /// Publishes a file tool's new file once where the task's commands
    /// write, under the effective tool scope and caller IO limits.
    pub async fn stage_tool_file_once(
        &self,
        operation_id: OperationId,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(FileRef, u64)> {
        let binding = self.task().tool_content(None);
        self.stage_once(
            binding,
            operation_id,
            path,
            bytes,
            media_type,
            display_name,
            io,
        )
        .await
    }

    /// Publishes one conditional batch on the original selected writer. Every
    /// source is checked against this invocation's grants before publication.
    pub(crate) async fn stage_tool_changes(
        &self,
        operation: OperationId,
        volume: &VolumeRef,
        changes: &[crate::conversation::ContentChange<'_>],
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(Vec<FileRef>, u64)> {
        use crate::conversation::{ContentAccess, ContentChange};
        let content = self
            .task()
            .content_binding(Some(volume))
            .ok_or_else(|| Error::Unsupported("conditional content writer is not bound".into()))?;
        let writer =
            super::content_write::bound_content_writer(Some(content), self.effective_scope(), io)?;
        if writer.volume() != volume {
            return Err(Error::Unauthorized(
                "conditional writer differs from selected volume".into(),
            ));
        }
        for change in changes {
            crate::executor::with_history_io(io, |work| {
                crate::conversation::validate_content_path(change.path(), work)?;
                if change.path().len() > self.effective_scope().limits().path_bytes
                    || crate::conversation::is_internal_path(change.path())
                {
                    return Err(Error::tool_rejected(
                        "conditional path exceeds original task admission",
                    ));
                }
                Ok(())
            })?;
            if let Some(source) = change.source() {
                self.task().authorize_file_read(
                    source,
                    self.effective_scope(),
                    self.effective_scope().grants(),
                    io,
                )?;
                if source.volume() != volume {
                    return Err(Error::Unauthorized(
                        "conditional source differs from writer".into(),
                    ));
                }
            }
            let bytes = match change {
                ContentChange::CreateAbsent { bytes, .. }
                | ContentChange::UpdateExpected { bytes, .. } => bytes.len() as u64,
                ContentChange::DeleteExpected { .. } => 0,
            };
            if bytes > self.effective_scope().limits().file_bytes {
                return Err(Error::tool_rejected(
                    "conditional file exceeds original task capacity",
                ));
            }
        }
        writer.supports_stage(io)?;
        let (files, hold) = writer
            .stage_changes(
                operation,
                changes,
                ContentAccess {
                    ceiling: Some(self.effective_scope().grants()),
                    io,
                },
            )?
            .await?;
        let mut outputs = files.iter();
        for change in changes {
            let (bytes, media, display) = match change {
                ContentChange::CreateAbsent {
                    bytes,
                    media_type,
                    display_name,
                    ..
                } => (*bytes, *media_type, *display_name),
                ContentChange::UpdateExpected { source, bytes } => (
                    *bytes,
                    source.descriptor().media_type(),
                    source.display_name(),
                ),
                ContentChange::DeleteExpected { .. } => continue,
            };
            let file = outputs
                .next()
                .ok_or_else(|| Error::Storage("conditional publisher omitted a file".into()))?;
            if file.volume() != volume
                || file.path() != change.path()
                || file.descriptor().media_type() != media
                || file.display_name() != display
            {
                return Err(Error::Storage(
                    "conditional publisher returned another file identity".into(),
                ));
            }
            crate::executor::with_history_io(io, |work| {
                self.effective_scope().limits().validate_file(file, work)?;
                file.descriptor().verify(bytes, work)
            })?;
            content
                .reader
                .verify_publication(file, file.descriptor().byte_length(), io)?
                .await?;
        }
        if outputs.next().is_some() {
            return Err(Error::Storage(
                "conditional publisher returned extra files".into(),
            ));
        }
        #[cfg(feature = "filesystem")]
        let retained = hold.as_ref().map_or(0, |hold| hold.bytes());
        #[cfg(not(feature = "filesystem"))]
        let retained = 0;
        #[cfg(feature = "filesystem")]
        if let Some(hold) = hold {
            hold.into_retained()?;
        }
        Ok((files, retained))
    }

    /// Recovers the original batch receipt after the original journal has
    /// authenticated the complete invocation; never reconstructs it from HEAD.
    pub(crate) async fn observe_tool_changes(
        &self,
        operation: OperationId,
        volume: &VolumeRef,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(Option<Vec<FileRef>>, u64)> {
        let content = self
            .task()
            .content_binding(Some(volume))
            .ok_or_else(|| Error::Unsupported("conditional content writer is not bound".into()))?;
        let writer =
            super::content_write::bound_content_writer(Some(content), self.effective_scope(), io)?;
        #[cfg(feature = "filesystem")]
        let maximum = io
            .map(|io| io.remaining().map(|work| work.bytes_encoded))
            .transpose()?
            .unwrap_or(u64::MAX);
        #[cfg(not(feature = "filesystem"))]
        let maximum = u64::MAX;
        let (files, hold) = writer
            .observe_changes(
                operation,
                maximum,
                crate::conversation::ContentAccess {
                    ceiling: Some(self.effective_scope().grants()),
                    io,
                },
            )?
            .await?;
        if let Some(files) = &files {
            for file in files {
                crate::executor::with_history_io(io, |work| {
                    self.effective_scope().limits().validate_file(file, work)
                })?;
                if file.volume() != volume {
                    return Err(Error::Storage(
                        "conditional receipt differs from selected volume".into(),
                    ));
                }
                content
                    .reader
                    .verify_publication(file, file.descriptor().byte_length(), io)?
                    .await?;
            }
        }
        #[cfg(feature = "filesystem")]
        let retained = hold.as_ref().map_or(0, |hold| hold.bytes());
        #[cfg(not(feature = "filesystem"))]
        let retained = 0;
        #[cfg(feature = "filesystem")]
        if let Some(hold) = hold {
            hold.into_retained()?;
        }
        Ok((files, retained))
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "one publication carries its binding, identity, bytes, metadata and IO"
    )]
    async fn stage_once(
        &self,
        binding: Option<&ContentBindings>,
        operation_id: OperationId,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(FileRef, u64)> {
        self.task()
            .stage_bound_file(
                binding,
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

    /// Checks directory authority for discovery of one public project path.
    pub(crate) fn authorize_tool_path_read(
        &self,
        path: &str,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<&VolumeRef> {
        let (volume, path) = self.tool_path_selection(path)?;
        crate::executor::with_history_io(io, |history| {
            crate::conversation::validate_content_path(path, history).map_err(Error::tool_input)?;
            if path.len() > self.effective_scope().limits().path_bytes
                || crate::conversation::is_internal_path(path)
                || !crate::core::directory_read_granted_with_work(
                    self.effective_scope().grants(),
                    volume,
                    path.rsplit_once('/').map_or("", |(parent, _)| parent),
                    history,
                )?
            {
                return Err(crate::store::guarded_boundary(
                    history,
                    crate::wire::ErrorCode::Unauthorized,
                    "path read requires the original directory read grant",
                )
                .tool_input());
            }
            Ok(())
        })?;
        Ok(volume)
    }

    /// Resolves and reads one current project path through its original bound
    /// reader, without broadening this invocation's effective grants.
    pub(crate) async fn read_tool_path(
        &self,
        path: &str,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(FileRef, acyclic_stream::OwnedBytes, u64)> {
        let volume = self.authorize_tool_path_read(path, io)?;
        let (_, path) = self.tool_path_selection(path)?;
        let content = self
            .task()
            .content_binding(Some(volume))
            .ok_or_else(|| Error::Unsupported("content reader is not bound".into()))?;
        self.task()
            .read_bound_path(
                content,
                (
                    volume,
                    path.rsplit_once('/').map_or("", |(parent, _)| parent),
                    path,
                    None,
                ),
                self.effective_scope(),
                io,
            )
            .await
    }

    /// Selects an executing agent's volume from the same mounted path spelling
    /// used by shell. This selects a resource; the original read grant is still
    /// checked at the read boundary.
    pub(crate) fn tool_path_selection<'a>(
        &'a self,
        path: &'a str,
    ) -> Result<(&'a VolumeRef, &'a str)> {
        let Some(selected) = path.strip_prefix(".volumes/") else {
            return Ok((&self.task().tool_read_content().volume, path));
        };
        let (name, path) = selected
            .split_once('/')
            .ok_or_else(|| Error::tool_rejected("volume path requires a file name"))?;
        let (_, volumes) = self
            .task()
            .agent()
            .ok_or_else(|| Error::tool_rejected("volume path requires an agent binding"))?;
        let volume = volumes
            .iter()
            .find(|binding| binding.name == name)
            .ok_or_else(|| Error::tool_rejected("volume is not bound to this agent"))?;
        Ok((&volume.volume, path))
    }

    /// Checks the exact mounted destination using the executing binding and
    /// original write grant, before the original publisher can have effects.
    pub(crate) fn authorize_tool_path_publication(
        &self,
        path: &str,
        source: Option<&FileRef>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        let (selected, relative) = self.tool_path_selection(path)?;
        if let Some(source) = source {
            self.task().authorize_file_read(
                source,
                self.effective_scope(),
                self.effective_scope().grants(),
                io,
            )?;
        }
        let (_, actual, _) = super::content_write::publication_destination(
            self.task().content_binding(Some(selected)),
            self.effective_scope(),
            source,
            relative,
            io,
        )?;
        if actual != selected {
            return Err(Error::tool_rejected(
                "publisher differs from selected volume",
            ));
        }
        Ok(())
    }

    /// Decodes an immutable version against the executing agent's selected
    /// volume. The token supplies identity only; reads still require its grant.
    pub(crate) fn decode_tool_version(
        &self,
        path: &str,
        version: &str,
        work: crate::NativeWork<'_>,
    ) -> Result<(FileRef, crate::NativeCustody)> {
        let (volume, relative) = self.tool_path_selection(path)?;
        let (file, custody) = crate::tool::files::decode_file_version(relative, version, work)?;
        if file.volume() != volume {
            return Err(Error::tool_rejected(
                "version belongs to another selected volume",
            ));
        }
        Ok((file, custody))
    }

    /// Resolves one versioned model-visible path through the same original
    /// grants and immutable descriptor checks used by read/edit and send.
    pub(crate) async fn read_tool_source(
        &self,
        path: &str,
        version: Option<&str>,
        work: crate::NativeWork<'_>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(FileRef, acyclic_stream::OwnedBytes, crate::NativeCustody)> {
        if let Some(version) = version {
            let (file, custody) = self.decode_tool_version(path, version, work)?;
            let bytes = self.read_file(&file, io).await?;
            return Ok((file, bytes, custody));
        }
        let (file, bytes, retained) = self.read_tool_path(path, io).await?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let custody = io.map(|io| io.take_native_custody(retained)).transpose()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let custody = {
            let _ = (work, retained);
            None
        };
        Ok((file, bytes, custody))
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
