//! Pi controls borrow the original task mailbox, never a service-local inbox.
use super::{pi_owner::PiOwnerConfig, pi_root_provider::{
    stage_pi_input, guarded_pi_decode, guarded_pi_value}};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, LocalAuthorityBackend, LocalObjectBackend,
    NativeAllocation, NativeMaterializationWork, NativeTreeMaterializationError, WorkCounters};
use acyclic_harness::{Error, OperationId, Outcome, Result, TaskId,
    durable_host::TaskJournalOwner,
    filesystem::{FilesystemHost, FilesystemTaskRuntime, TaskCommandHost, TaskCommandProgress, MailSendTaskCommand, MAIL_SEND_TASK_COMMAND_KIND},
    runtime::TaskContext, scheduler::{InboxItem, LeaseFence}, workflow::WorkflowCommand};
use acyclic_stream::StreamProvider;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub(crate) const MAXIMUM_PI_MAIL_BYTES: u64 = 65_536;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PiMailKind { Message, Steer }

/// First-party immutable payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiMailEnvelope {
    pub(crate) kind: PiMailKind,
    pub(crate) message: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PiMailPayload { sender: TaskId, envelope: PiMailEnvelope }

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiMailboxReceive {
    pub(crate) item: InboxItem,
    pub(crate) sender: TaskId,
    pub(crate) envelope: PiMailEnvelope,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PiWaitState { Pending, Completed }

#[derive(Serialize)]
pub(crate) struct PiWaitObservation {
    pub(crate) task: TaskId,
    pub(crate) state: PiWaitState,
    pub(crate) outcome: Option<Outcome<Value>>,
    pub(crate) mail: Option<PiMailboxReceive>,
}

fn native_error(cause: NativeTreeMaterializationError) -> Error {
    Error::NativeWork { cause, custody: None }
}

async fn authenticate<P: StreamProvider>(
    context: &TaskContext, owner: &TaskJournalOwner<P>, work: &NativeMaterializationWork,
) -> Result<TaskId> {
    if context.durable_task_id() != Some(owner.task_id()) {
        return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi mailbox caller differs from original owner" });
    }
    owner.verify_with_native_work(false, work).await?;
    Ok(owner.task_id())
}

fn validate_envelope(envelope: &PiMailEnvelope) -> Result<()> {
    if envelope.message.len() as u64 > (MAXIMUM_PI_MAIL_BYTES - 128) / 6 {
        return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid,
            reason: "Pi mailbox message exceeds bounded payload" });
    }
    Ok(())
}

/// Parent supplies its retained graph check.
#[allow(clippy::too_many_arguments, reason = "explicit original task and protected immutable input owner")]
pub(crate) async fn prepare_send<P: StreamProvider, F: FnOnce(TaskId, TaskId) -> Result<()>>(
    context: &TaskContext, owner: &TaskJournalOwner<P>, protected: &PiOwnerConfig,
    filesystem: &FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>,
    operation: OperationId, recipient: TaskId, envelope: PiMailEnvelope,
    authorize: F, work: &NativeMaterializationWork,
) -> Result<(WorkflowCommand, NativeAllocation)> {
    let caller = authenticate(context, owner, work).await?;
    authorize(caller, recipient)?;
    let command = stage_send(protected, filesystem, operation, caller, recipient, envelope, work).await?;
    owner.verify_with_native_work(false, work).await?;
    Ok((command.0, command.1))
}

/// Trusted actor input staging for a passive root.
#[allow(clippy::too_many_arguments, reason = "original protected actor and exact immutable sender/recipient input")]
pub(crate) async fn stage_send(
    protected: &PiOwnerConfig,
    filesystem: &FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>,
    operation: OperationId, caller: TaskId, recipient: TaskId, envelope: PiMailEnvelope,
    work: &NativeMaterializationWork,
) -> Result<(WorkflowCommand, NativeAllocation)> {
    validate_envelope(&envelope)?;
    let stored = PiMailPayload { sender: caller, envelope };
    let staged = stage_pi_input(
        protected, filesystem, operation, "mail-envelope", &stored, work).await?;
    let payload = MailSendTaskCommand { recipient, payload: staged.0 };
    let mut command = stage_pi_input(
        protected, filesystem, operation, "mail-send", &payload, work).await?;
    drop(payload);
    drop(staged.1);
    let kind_length = MAIL_SEND_TASK_COMMAND_KIND.len() as u64;
    work.charge(WorkCounters { bytes_copied: kind_length, allocation_operations: 1, ..Default::default() })
        .map_err(native_error)?;
    let mut kind_custody = work.retain_owned(kind_length).map_err(native_error)?;
    let kind = MAIL_SEND_TASK_COMMAND_KIND.to_owned();
    if let Err(cause) = command.1.absorb(&mut kind_custody) {
        drop(kind);
        drop(command.0);
        drop(kind_custody);
        return Err(Error::NativeWork { cause, custody: Some(command.1) });
    }
    Ok((WorkflowCommand { operation_id: operation, kind, payload: command.0 }, command.1))
}

/// A normal control command; the hosting root must resume its original passive checkpoint before executing it.
pub(crate) async fn execute_send<P, A, O>(runtime: &FilesystemTaskRuntime<P, A, O>,
    context: std::sync::Arc<acyclic_harness::NativeTaskContext>, owner: &TaskJournalOwner<P>, fence: LeaseFence, command: WorkflowCommand,
    work: &NativeMaterializationWork,
) -> Result<TaskCommandProgress>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    authenticate(context.context(), owner, work).await?;
    if command.kind != MAIL_SEND_TASK_COMMAND_KIND {
        return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid,
            reason: "Pi send requires original stock mail command" });
    }
    let bytes = context.context().read_file_with_native_work(&command.payload, work).await?;
    let payload = decode::<MailSendTaskCommand>(bytes.as_ref(), work)?;
    drop(bytes);
    if payload.0.payload.descriptor().byte_length() > MAXIMUM_PI_MAIL_BYTES {
        return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid,
            reason: "Pi send payload exceeds original bounded envelope" });
    }
    let value = guarded_pi_value(&payload.0, MAXIMUM_PI_MAIL_BYTES * 8, work)?;
    drop(payload);
    let commands = runtime.commands();
    let result = commands.execute_with_native_work(context, fence, command, value.0, work)?.await;
    drop(value.1);
    result
}

const MAIL_DIAGNOSTIC_LABEL_BYTES: u64 =
    super::pi_root_provider::ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES
    + super::pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
        "PiMailKind", "message", "steer", "PiMailEnvelope", "kind",
        "PiMailPayload", "sender", "envelope", "MailSendTaskCommand",
        "recipient", "payload",
    ]);

fn decode<T: serde::de::DeserializeOwned>(
    bytes: &[u8], work: &NativeMaterializationWork,
) -> Result<(T, NativeAllocation)> {
    guarded_pi_decode(bytes, MAXIMUM_PI_MAIL_BYTES, std::mem::size_of::<T>() as u64,
        acyclic_harness::JsonValueSchema::None, MAIL_DIAGNOSTIC_LABEL_BYTES, work)
}

/// Passive polling acquires no reservation and charges no workflow task steps.
pub(crate) async fn receive<P, A, O>(runtime: &FilesystemTaskRuntime<P, A, O>,
    caller: &super::PiTaskBinding, after: u64, maximum: u32, work: &NativeMaterializationWork,
) -> Result<(Option<PiMailboxReceive>, NativeAllocation)>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    if maximum != 1 { return Err(Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi receive selects exactly one item" }); }
    let next = after.checked_add(1).ok_or(Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi mailbox sequence overflows" })?;
    let mut items = runtime.task_host().inbox_with_native_work(caller.task_id, after, 1, work).await?;
    let Some(item) = items.0.pop() else { return Ok((None, items.1)); };
    if item.task_id != caller.task_id || item.sequence != next {
        return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Conflict,
            reason: "Pi passive receive selected a different sequence" });
    }
    if item.payload.descriptor().byte_length() > MAXIMUM_PI_MAIL_BYTES {
        return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid,
            reason: "Pi mailbox payload exceeds bounded decode" });
    }
    let bytes = runtime.task_host()
        .read_recipient_file_with_native_work(caller.task_id, &item.payload, work).await?;
    let mut payload = decode::<PiMailPayload>(&bytes.0, work)?;
    drop(bytes);
    if let Err(cause) = items.1.absorb(&mut payload.1) {
        drop(payload);
        drop(item);
        drop(items.0);
        return Err(Error::NativeWork { cause, custody: Some(items.1) });
    }
    Ok((Some(PiMailboxReceive { item, sender: payload.0.sender, envelope: payload.0.envelope }), items.1))
}

/// Passive wait uses only the original admitted task and bounded own inbox.
pub(crate) async fn wait<P, A, O, F>(runtime: &FilesystemTaskRuntime<P, A, O>,
    caller: &super::PiTaskBinding, requested: TaskId, after: u64, authorize: F,
    work: &NativeMaterializationWork,
) -> Result<(PiWaitObservation, NativeAllocation)>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static,
      F: FnOnce(TaskId, TaskId) -> Result<()> {
    authorize(caller.task_id, requested)?;
    let mut mail = receive(runtime, caller, after, 1, work).await?;
    let mut outcome = runtime.task_host().outcome_with_native_work(requested, work).await?;
    let state = match &outcome.0 {
        None | Some(Outcome::Indeterminate { .. }) => PiWaitState::Pending,
        Some(_) => PiWaitState::Completed,
    };
    if let Err(cause) = mail.1.absorb(&mut outcome.1) {
        drop(outcome);
        drop(mail.0);
        return Err(Error::NativeWork { cause, custody: Some(mail.1) });
    }
    Ok((PiWaitObservation { task: requested, state, outcome: outcome.0, mail: mail.0 }, mail.1))
}

#[cfg(test)]
#[path = "pi_mail_tests.rs"]
mod tests;
