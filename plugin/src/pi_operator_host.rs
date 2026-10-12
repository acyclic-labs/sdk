//! Ordinary interaction resolution and original selected writeback dispatch.
use crate::{PiTaskBinding, pi_owner::PiOwnerConfig,
    pi_native_host::{PiNativeRuntime, PiNativeFilesystem},
    pi_operator::{self, PiPreparedApproval, PiOperatorRequest, PiOperatorBody, PiOperatorDecision},
    pi_fork::{original_operation, derived}};
use acyclic_fs::{NativeMaterializationWork, MaterializationPhase};
use acyclic_harness::{Error, Result, Capabilities, OperationId, runtime::NativeTaskContext,
    durable_host::TaskJournalOwner, scheduler::LeaseFence,
    filesystem::{FilesystemExecutionJournal, TaskCommandProgress},
    core::SchemaRegistry, executor::ExecutionJournal,
    interaction::{Interaction, InteractionResponse, InteractionOutcome}, tool::ToolApprovalVerifier,
    workflow::WorkflowCommand};
use acyclic_stream::{LocalStream};
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;

type Journal = FilesystemExecutionJournal<LocalStream, crate::LocalAuthorityBackend, crate::LocalObjectBackend>;
pub(crate) const PI_OPERATOR_COMMAND_KIND: &str = "acyclic.pi.operator-approval";

impl<'a> From<&'a crate::pi_selected_host::PiSelectedCommand> for crate::pi_selected_host::PiSelectedCommand<&'a str, &'a [String]> {
    fn from(command: &'a crate::pi_selected_host::PiSelectedCommand) -> Self {
        Self { caller: command.caller, sdk_operation: &command.sdk_operation, source: command.source,
            root: command.root.as_deref(), directory: &command.directory, cursor: command.cursor.as_deref(),
            maximum_entries: command.maximum_entries, destination: command.destination, paths: &command.paths,
            confirm: command.confirm, approval_digest: command.approval_digest.as_deref(),
            import_operation: command.import_operation }
    }
}
impl<'a> crate::pi_selected_host::PiSelectedCommand<&'a str, &'a [String]> {
    pub(crate) fn normalized(command: &'a crate::pi_selected_host::PiSelectedCommand, root: &'a str) -> Self {
        Self { root: Some(root), approval_digest: None, import_operation: None, ..Self::from(command) }
    }
}
impl<'a> From<&'a PiPreparedApproval> for PiPreparedApproval<&'a str, &'a acyclic_harness::conversation::VolumeRef, &'a [String], &'a acyclic_fs::MaterializationJournal> {
    fn from(descriptor: &'a PiPreparedApproval) -> Self {
        Self { version: descriptor.version, session: &descriptor.session, command: (&descriptor.command).into(),
            operation: descriptor.operation, approval_digest: &descriptor.approval_digest,
            private_volume: &descriptor.private_volume, source_snapshot: descriptor.source_snapshot,
            selected_snapshot: descriptor.selected_snapshot, imported: descriptor.imported,
            import_receipt: (&descriptor.import_receipt).into(), journal: &descriptor.journal,
            canonical_digest_input: &descriptor.canonical_digest_input,
            root_path: &descriptor.root_path, source_path: &descriptor.source_path }
    }
}
fn denied(reason: &'static str) -> Error {
    Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized, reason }
}
fn native_error(cause: acyclic_fs::NativeTreeMaterializationError) -> Error { Error::NativeWork { cause, custody: None } }
fn validate_descriptor(protected: &PiOwnerConfig, descriptor: &PiPreparedApproval<&'_ str, &'_ acyclic_harness::conversation::VolumeRef, &'_ [String], &'_ acyclic_fs::MaterializationJournal>, work: &NativeMaterializationWork) -> Result<()> {
    let root = protected.policy().root_task;
    let original = original_operation(root, root, descriptor.command.sdk_operation, work)?;
    if descriptor.version != 1 || descriptor.command.caller != root || descriptor.command.source == root
        || descriptor.operation != derived(original, "selected-writeback", work)?
        || descriptor.command.approval_digest.is_some() || descriptor.command.import_operation.is_some()
        || !descriptor.command.confirm || descriptor.command.paths.is_empty()
        || descriptor.journal.plan.operation_id != acyclic_fs::OperationId::from_bytes(descriptor.operation.into_bytes())
        || descriptor.journal.phase != MaterializationPhase::Prepared {
        return Err(denied("operator descriptor is not the original Root's actual prepared writeback"));
    }
    pi_operator::parse_digest(&descriptor.approval_digest)?;
    Ok(())
}
fn conversation_journal<'a>(filesystem: &Arc<PiNativeFilesystem>, stream: &std::sync::Arc<LocalStream>,
    protected: &PiOwnerConfig, descriptor: impl Into<PiPreparedApproval<&'a str, &'a acyclic_harness::conversation::VolumeRef, &'a [String], &'a acyclic_fs::MaterializationJournal>>, work: &NativeMaterializationWork) -> Result<Journal> {
    let descriptor = descriptor.into();
    validate_descriptor(protected, &descriptor, work)?;
    let authority = protected.conversation_authority_with_native_work(protected.policy().root_task,
        protected.policy().agent, &protected.policy().grants, work)?;
    let verifier = {
        let mut io = acyclic_harness::store::history::native_work_io(work).map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?;
        let value = authority.0.1.verifier(&mut Some(&mut io))?;
        (value, io.into_custody())
    };
    Journal::new_with_native_work(stream.clone(), Arc::clone(filesystem), descriptor.private_volume, &verifier.0, &authority.0.2,
        protected.policy().maximum_payload_bytes, work)
}

/// Called only with the real materializer preparation, before the original SDK preparation ACK.
#[allow(clippy::too_many_arguments, reason = "original task, conversation and measured producer are independent")]
pub(crate) async fn open_prepared_approval(runtime: &PiNativeRuntime, filesystem: &Arc<PiNativeFilesystem>,
    protected: &PiOwnerConfig, context: &NativeTaskContext, owner: &TaskJournalOwner<LocalStream>,
    binding: &PiTaskBinding, descriptor: &PiPreparedApproval<&'_ str, &'_ acyclic_harness::conversation::VolumeRef, &'_ [String], &'_ acyclic_fs::MaterializationJournal>, work: &NativeMaterializationWork) -> Result<()> {
    validate_descriptor(protected, descriptor, work)?;
    runtime.verify_owner_with_native_work(protected, work)?;
    protected.validate_root(binding, work)?;
    let (task, fence) = context.context().owned_task_binding_with_native_work(work)?;
    if task != protected.policy().root_task || owner.task_id() != task
        || descriptor.session != binding.native_session_id || descriptor.private_volume != &binding.scratch.volume {
        return Err(denied("prepared approval changed the authenticated original Root output allocation"));
    }
    owner.verify_with_native_work(false, work).await?;
    let storage = protected.authority_with_native_work(work)?;
    let authority = protected.conversation_authority_with_native_work(binding.task_id, binding.agent_id,
        context.context().scope().grants(), work)?;
    let verifier = {
        let mut io = acyclic_harness::store::history::native_work_io(work).map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?;
        let value = authority.0.1.verifier(&mut Some(&mut io))?;
        (value, io.into_custody())
    };
    let current = runtime.runtime.task_host().journal_owner_with_native_work(binding.task_id, fence, work).await?;
    let journal = Journal::for_task(current, descriptor.operation, context.context().id(), Arc::clone(filesystem), &protected.policy().journal, &storage.0.verifier, &storage.0.signed, protected.policy().maximum_payload_bytes, Some(&acyclic_harness::filesystem::PreparationIo::from_native_work(work)?))?
        .with_interaction_owner(&verifier.0, &SchemaRegistry::new(), &authority.0.2, descriptor.private_volume, Some(&acyclic_harness::filesystem::PreparationIo::from_native_work(work)?))?;
    let (prompt, _prompt_custody) = work.display(&format_args!("Publish selected Pi writeback {}", descriptor.command.sdk_operation))
        .map_err(native_error)?;
    let interaction = Interaction::Approval { prompt, operation_id: descriptor.operation,
        action_digest: pi_operator::parse_digest(descriptor.approval_digest)? };
    journal.open_interaction_with_native_work(acyclic_harness::durable_host::task_interaction_id_with_native_work(binding.task_id, descriptor.operation, work)?, &interaction, work)?.await?;
    owner.verify_with_native_work(false, work).await?;
    let _staged = crate::pi_root_provider::stage_pi_input(protected, filesystem, descriptor.operation,
        "operator-approval", descriptor, work).await?;
    owner.verify_with_native_work(false, work).await
}

pub(crate) async fn require_resolved_approval<'a>(filesystem: &Arc<PiNativeFilesystem>, stream: &std::sync::Arc<LocalStream>,
    protected: &PiOwnerConfig, descriptor: impl Into<PiPreparedApproval<&'a str, &'a acyclic_harness::conversation::VolumeRef, &'a [String], &'a acyclic_fs::MaterializationJournal>>, work: &NativeMaterializationWork) -> Result<()> {
    let descriptor = descriptor.into();
    let journal = conversation_journal(filesystem, stream, protected, descriptor, work)?;
    ToolApprovalVerifier::verify_with_native_work(&journal, acyclic_harness::durable_host::task_interaction_id_with_native_work(protected.policy().root_task, descriptor.operation, work)?,
        descriptor.operation, pi_operator::parse_digest(descriptor.approval_digest)?, work)?.await
}

/// Passive: no lease pull, source capture, import, preparation, launch or workflow mutation.
pub(crate) async fn read_prepared_approval(runtime: &PiNativeRuntime, filesystem: &Arc<PiNativeFilesystem>, protected: &PiOwnerConfig,
    sdk_operation: &str, work: &NativeMaterializationWork) -> Result<Option<(PiPreparedApproval, acyclic_fs::NativeAllocation)>> {
    if sdk_operation.is_empty() || sdk_operation.len() > 512 { return Err(denied("status requires the original bounded SDK operation")); }
    runtime.verify_owner_with_native_work(protected, work)?;
    let root = protected.policy().root_task;
    let operation = derived(original_operation(root, root, sdk_operation, work)?, "selected-writeback", work)?;
    let (context, _context_custody) = runtime.runtime.harness().durable_context_with_native_work(
        root, OperationId::from_bytes(root.into_bytes()), work).await?;
    let descriptor: Option<(PiPreparedApproval, acyclic_fs::NativeAllocation)> =
        crate::pi_root_provider::read_pi_input_with_native_work(protected, filesystem, operation,
            "operator-approval", crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8,
            acyclic_harness::JsonValueSchema::None, pi_operator::prepared_labels_bytes(), &context, work).await?;
    if let Some((descriptor, _custody)) = &descriptor { validate_descriptor(protected, &descriptor.into(), work)?; }
    Ok(descriptor)
}

#[derive(Serialize)]
struct PiWritebackResponse<'a> {
    status: &'static str,
    operation: &'a str,
    source: acyclic_harness::TaskId,
    root: &'a Option<String>,
    paths: &'a [String],
    approval_digest: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    receipt: Option<&'a acyclic_fs::MaterializationJournal>,
}
fn status_response<'a>(descriptor: &'a PiPreparedApproval, status: &'static str,
    native: Option<&'a acyclic_fs::MaterializationJournal>) -> PiWritebackResponse<'a> {
    PiWritebackResponse { status, operation: &descriptor.command.sdk_operation,
        source: descriptor.command.source, root: &descriptor.command.root, paths: &descriptor.command.paths,
        approval_digest: &descriptor.approval_digest,
        receipt: if status == "published" { native.map(|owned| &owned.journal) } else { None } }
}
#[derive(Serialize)]
struct PiPendingResponse<'a> {
    status: &'static str,
    operation: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<acyclic_harness::TaskId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    root: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    paths: Option<&'a [String]>,
}
#[derive(Serialize)]
struct PiPassiveRpcResponse<'a, T: Serialize> {
    session: &'a str,
    native_session: &'a str,
    current_task: acyclic_harness::TaskId,
    #[serde(flatten)]
    observation: T,
}
#[derive(Serialize)]
struct PiInspectionResponse<'a> {
    #[serde(flatten)]
    observation: PiWritebackResponse<'a>,
    prepared: &'a PiPreparedApproval,
    #[serde(skip_serializing_if = "Option::is_none")]
    imported_selected_files: Option<&'a Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    file_evidence_cursor: Option<&'a Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    file_evidence_complete: Option<&'a Value>,
}

async fn observe_writeback(filesystem: &Arc<PiNativeFilesystem>, stream: &std::sync::Arc<LocalStream>,
    resources: &crate::ServiceResources, protected: &PiOwnerConfig, descriptor: &PiPreparedApproval,
    work: &NativeMaterializationWork) -> Result<(&'static str, Option<acyclic_fs::MaterializationJournal>)> {
    let journal = conversation_journal(filesystem, stream, protected, descriptor, work)?;
    let (outcome, _outcome_custody) = journal.interaction_outcome_with_native_work(
        acyclic_harness::durable_host::task_interaction_id_with_native_work(protected.policy().root_task, descriptor.operation, work)?, work)?.await?;
    let fs_operation = acyclic_fs::OperationId::from_bytes(descriptor.operation.into_bytes());
    let store = resources.store.materialization_with_work(work.clone());
    let native = acyclic_fs::MaterializationJournalStore::load(&store, fs_operation).await
        .map_err(|error| match error {
            acyclic_fs::LocalCoreStateStoreError::MaterializationWork(cause) => native_error(cause),
            source => Error::NativeWorkspacePublication {
                source: acyclic_fs::NativeWorkspacePublicationError::State(source), custody: None,
            },
        })?;
    if let Some(journal) = &native {
        if !journal.is_valid_for(fs_operation) { return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Storage, reason: "original native writeback journal is invalid" }); }
        if journal.plan != descriptor.journal.plan || journal.root_identity != descriptor.journal.root_identity
            || journal.preparation_identity != descriptor.journal.preparation_identity
            || journal.preimages != descriptor.journal.preimages {
            return Err(denied("native writeback receipt differs from its original approved immutable preparation"));
        }
    }
    let status = if native.as_ref().is_some_and(|journal| journal.phase == MaterializationPhase::Applied) {
        require_resolved_approval(filesystem, stream, protected, descriptor, work).await?;
        "published"
    } else if matches!(outcome, Some(InteractionOutcome::Declined)) { "declined" }
    else if matches!(outcome, Some(InteractionOutcome::Approved)) { "pending" }
    else if native.as_ref().is_some_and(|journal| journal.phase == MaterializationPhase::Prepared) { "prepared" }
    else { "pending" };
    Ok((status, native))
}

pub(crate) async fn writeback_status(filesystem: &Arc<PiNativeFilesystem>, stream: &std::sync::Arc<LocalStream>,
    resources: &crate::ServiceResources, protected: &PiOwnerConfig, descriptor: &PiPreparedApproval,
    work: &NativeMaterializationWork) -> Result<(Value, acyclic_fs::NativeAllocation)> {
    let (status, native) = observe_writeback(filesystem, stream, resources, protected, descriptor, work).await?;
    crate::pi_root_provider::guarded_pi_value(&status_response(descriptor, status, native.as_ref()),
        crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8, work)
}

/// Rejects invalid operator responses before staging an immutable Root control.
pub(crate) async fn validate_approval_request(runtime: &PiNativeRuntime, filesystem: &Arc<PiNativeFilesystem>, protected: &PiOwnerConfig,
    session: &str, request: &PiOperatorRequest, work: &NativeMaterializationWork) -> Result<()> {
    pi_operator::verify_request(protected, session, request, work)?;
    let PiOperatorBody::Approval { prepared, digest, decision } = &request.body else {
        return Err(denied("inspect cannot dispatch a Root approval"));
    };
    if digest != &prepared.approval_digest { return Err(denied("operator response has the wrong prepared digest")); }
    let (original, _original_custody) = read_prepared_approval(runtime, filesystem, protected, &prepared.command.sdk_operation, work)
        .await?.ok_or_else(|| denied("operator operation has no original preparation"))?;
    let original_hash = pi_operator::approval_body_hash(&original, digest, *decision, work)?;
    if original_hash.0 != pi_operator::body_hash(&request.body, work)?.0 {
        return Err(denied("operator substituted the prepared selection, generations or digest"));
    }
    Ok(())
}

/// Signed responder authority stays in Root-only input storage.
pub(crate) async fn stage_operator_command(runtime: &PiNativeRuntime, protected: &PiOwnerConfig, filesystem: &Arc<PiNativeFilesystem>,
    request: &PiOperatorRequest, work: &NativeMaterializationWork) -> Result<(WorkflowCommand, acyclic_fs::NativeAllocation)> {
    validate_approval_request(runtime, filesystem, protected, &request.session_id, request, work).await?;
    let root = protected.policy().root_task;
    let operation_id = derived(original_operation(root, root, request.body.operation(), work)?,
        "selected-writeback-publish", work)?;
    let mut staged = crate::pi_root_provider::stage_pi_operator_input(protected, filesystem,
        operation_id, "command", request, work).await?;
    let mut kind = work.display(&format_args!("{PI_OPERATOR_COMMAND_KIND}")).map_err(native_error)?;
    staged.1.absorb(&mut kind.1).map_err(native_error)?;
    Ok((WorkflowCommand { operation_id, kind: kind.0, payload: staged.0 }, staged.1))
}

pub(crate) async fn inspect_operator(runtime: &PiNativeRuntime, filesystem: &Arc<PiNativeFilesystem>, stream: &std::sync::Arc<LocalStream>,
    resources: &crate::ServiceResources, protected: &PiOwnerConfig, session: &str,
    retained: &crate::AdapterState, roots: &std::collections::BTreeMap<String, crate::pi_root_provider::PiRootHandle>,
    request: &PiOperatorRequest, work: &NativeMaterializationWork) -> Result<(Value, acyclic_fs::NativeAllocation)> {
    pi_operator::verify_request(protected, session, request, work)?;
    let PiOperatorBody::Inspect { operation, project_files, file_evidence, .. } = &request.body else {
        return Err(denied("approval cannot use the passive inspection route"));
    };
    if !*project_files && file_evidence.is_some() { return Err(denied("file evidence requires explicit file projection")); }
    let Some((prepared, _prepared_custody)) = read_prepared_approval(runtime, filesystem, protected, operation, work).await? else {
        return crate::pi_root_provider::guarded_pi_value(&PiPendingResponse { status: "pending", operation,
            source: None, root: None, paths: None }, crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8, work);
    };
    if prepared.session != session { return Err(denied("prepared operation belongs to another original Root session")); }
    let (status, native) = observe_writeback(filesystem, stream, resources, protected, &prepared, work).await?;
    let projection = if *project_files {
        if let Some(evidence) = file_evidence {
            if !(1..=262_144).contains(&evidence.maximum_bytes) {
                return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid,
                    reason: "file evidence requires 1..262144 bytes" });
            }
            if let Some(cursor) = &evidence.cursor {
                if cursor.operation != prepared.command.sdk_operation || cursor.path != evidence.path
                    || prepared.command.root.as_deref() != Some(cursor.root.as_str()) || cursor.generation != prepared.imported {
                    return Err(denied("file evidence cursor changed its original operation, Root, path or generation"));
                }
            }
        }
        let projection = crate::pi_selected_host::read_imported_selected_files(
            protected, retained, roots, &prepared, file_evidence.as_ref(), work).await?;
        Some(projection)
    } else { None };
    let fields = projection.as_ref().map(|(value, _custody)| value);
    let field = |name: &str| -> Result<Option<&Value>> {
        fields.map(|value| value.get(name).ok_or(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Storage,
            reason: "immutable file projection omitted an authored evidence field",
        })).transpose()
    };
    crate::pi_root_provider::guarded_pi_value(&PiInspectionResponse {
        observation: status_response(&prepared, status, native.as_ref()), prepared: &prepared,
        imported_selected_files: field("files")?, file_evidence_cursor: field("cursor")?,
        file_evidence_complete: field("complete")?,
    }, crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8, work)
}

/// Caller proof is verified by the existing actor before entering this passive API.
pub(crate) async fn passive_status(runtime: &PiNativeRuntime, filesystem: &Arc<PiNativeFilesystem>, stream: &std::sync::Arc<LocalStream>,
    resources: &crate::ServiceResources, protected: &PiOwnerConfig, session: &str, native_session: &str, caller: acyclic_harness::TaskId,
    sdk_operation: &str, source: acyclic_harness::TaskId, root: &str, paths: &[String],
    work: &NativeMaterializationWork) -> Result<(Value, acyclic_fs::NativeAllocation)> {
    if caller != protected.policy().root_task { return Err(denied("writeback status belongs only to its original Root caller")); }
    let Some((prepared, _prepared_custody)) = read_prepared_approval(runtime, filesystem, protected, sdk_operation, work).await? else {
        return crate::pi_root_provider::guarded_pi_value(&PiPassiveRpcResponse {
            session, native_session, current_task: caller,
            observation: PiPendingResponse { status: "pending", operation: sdk_operation,
                source: Some(source), root: Some(root), paths: Some(paths) },
        }, crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8, work);
    };
    if prepared.session != session || prepared.command.caller != caller || prepared.command.source != source
        || prepared.command.root.as_deref() != Some(root) || prepared.command.paths != paths {
        return Err(denied("passive status changed the original operation's selected source, Root or paths"));
    }
    let (status, native) = observe_writeback(filesystem, stream, resources, protected, &prepared, work).await?;
    crate::pi_root_provider::guarded_pi_value(&PiPassiveRpcResponse {
        session, native_session, current_task: caller, observation: status_response(&prepared, status, native.as_ref()),
    }, crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8, work)
}

impl<'a> crate::pi_selected_host::PiControlHost<'a> {
    pub(crate) async fn run_operator(&self, context: Arc<NativeTaskContext>, fence: LeaseFence, command: WorkflowCommand,
        payload: Value, reconcile: bool, work: &NativeMaterializationWork) -> Result<TaskCommandProgress> {
        context.context().verify_original_execution_owner_with_native_work(work)?.await?;
        let (request, _request_custody) = PiOperatorRequest::from_value_with_native_work(&payload, work)?;
        drop(payload);
        validate_approval_request(&self.runtime, &self.filesystem, &self.protected, &self.retained.root_session_id, &request, work).await?;
        let PiOperatorBody::Approval { mut prepared, digest, decision } = request.body else { return Err(denied("inspect cannot dispatch a Root command")); };
        let root = self.protected.policy().root_task;
        let (task, current_fence) = context.context().owned_task_binding_with_native_work(work)?;
        if task != root || prepared.command.caller != root || current_fence != &fence
            || command.operation_id != derived(
                original_operation(root, root, &prepared.command.sdk_operation, work)?,
                "selected-writeback-publish", work)? {
            return Err(denied("operator control changed the original Root, current fence or SDK tool operation"));
        }
        let owner = self.runtime.runtime.task_host().journal_owner_with_native_work(root, &fence, work).await?
            .with_invocation_cancellation(work.cancellation()).into_native_shared(work)?;
        owner.verify_with_native_work(false, work).await?;
        let (admitted, _admission_custody) = self.admission_input(root, work).await?;
        self.protected.validate_root(&admitted.binding, work)?;
        let crate::pi_task::PiTaskExecution::Host { root: original_root, .. } = admitted.execution else {
            return Err(denied("operator Root became a child admission"));
        };
        if admitted.binding.native_session_id != prepared.session
            || admitted.binding.scratch.volume != prepared.private_volume {
            return Err(denied("operator approval changed the original Root's actual private allocation"));
        }
        let context = crate::pi_root_provider::bind_root_output_context_with_native_work(context, &owner,
            Arc::clone(&self.protected), self.resources, Arc::clone(&self.filesystem), self.stream.clone(),
            &admitted.binding, &original_root, work).await?;
        if !reconcile {
            let journal = conversation_journal(&self.filesystem, &self.stream, &self.protected, &prepared, work)?;
            let id = acyclic_harness::durable_host::task_interaction_id_with_native_work(root, prepared.operation, work)?;
            let capability = work.display(&format_args!("interaction:respond:{id}")).map_err(native_error)?;
            let grants = Capabilities::new_with_native_work(["interaction:resolve", capability.0.as_str()], work)?;
            let responder = self.protected.conversation_authority_with_native_work(root, self.protected.policy().agent,
                &grants.0, work)?;
            let response = InteractionResponse::Approval { approved: decision == PiOperatorDecision::Approve, reason: None };
            owner.verify_with_native_work(false, work).await?;
            journal.resolve_interaction_with_native_work(id, &response, &responder.0.2, work)?.await?;
            owner.verify_with_native_work(false, work).await?;
        }
        if decision == PiOperatorDecision::Decline {
            let journal = conversation_journal(&self.filesystem, &self.stream, &self.protected, &prepared, work)?;
            let (outcome, _outcome_custody) = journal.interaction_outcome_with_native_work(
                acyclic_harness::durable_host::task_interaction_id_with_native_work(root, prepared.operation, work)?, work)?.await?;
            if !matches!(outcome, Some(InteractionOutcome::Declined)) { return Ok(TaskCommandProgress::Pending); }
            let (value, custody) = crate::pi_root_provider::guarded_pi_value(&status_response(&prepared, "declined", None),
                crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8, work)?;
            return Ok(TaskCommandProgress::ReadyWithNativeCustody { value, custody });
        }
        require_resolved_approval(&self.filesystem, &self.stream, &self.protected, &prepared, work).await?;
        let (kind, _kind_custody) = work.display(&format_args!("acyclic.pi.writeback")).map_err(native_error)?;
        let publish = WorkflowCommand { operation_id: command.operation_id, kind, payload: command.payload };
        prepared.command.approval_digest = Some(digest);
        let (payload, _payload_custody) = crate::pi_root_provider::guarded_pi_value(&prepared.command,
            crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8, work)?;
        prepared.command.approval_digest = None;
        let progress = self.run_selected(context, fence, publish, payload, reconcile, work).await?;
        match progress {
            TaskCommandProgress::Ready(value) => drop(value),
            TaskCommandProgress::ReadyWithNativeCustody { value, custody } => { drop(value); drop(custody); }
            TaskCommandProgress::Pending => return Ok(TaskCommandProgress::Pending),
            TaskCommandProgress::Indeterminate => return Ok(TaskCommandProgress::Indeterminate),
        }
        owner.verify_with_native_work(false, work).await?;
        let (status, native) = observe_writeback(&self.filesystem, &self.stream, self.resources, &self.protected,
            &prepared, work).await?;
        if status != "published" { return Ok(TaskCommandProgress::Pending); }
        let (value, custody) = crate::pi_root_provider::guarded_pi_value(&status_response(&prepared, status, native.as_ref()),
            crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8, work)?;
        Ok(TaskCommandProgress::ReadyWithNativeCustody { value, custody })
    }
}
