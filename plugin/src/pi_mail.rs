//! Pi controls borrow the original task mailbox, never a service-local inbox.
use super::{pi_owner::PiOwnerConfig, pi_root_provider::{
    stage_pi_input_with_native_work, guarded_pi_decode, guarded_pi_value}};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, LocalAuthorityBackend, LocalObjectBackend,
    NativeAllocation, NativeMaterializationWork, NativeTreeMaterializationError, WorkCounters};
use acyclic_harness::{Error, OperationId, Outcome, Result, TaskId,
    durable_host::TaskJournalOwner,
    filesystem::{FilesystemHost, FilesystemTaskRuntime, TaskCommandProgress, MailSendTaskCommand, MAIL_SEND_TASK_COMMAND_KIND},
    runtime::TaskContext, scheduler::{InboxItem, LeaseFence}, workflow::WorkflowCommand};
use acyclic_stream::StreamProvider;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub(crate) const MAXIMUM_PI_MAIL_BYTES: u64 = 65_536;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PiMailKind { Message, Steer }

/// First-party immutable payload. Sender identity is the actual fenced mailbox
/// publisher, not a field supplied by Pi or the root service.
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

// Field order keeps the genuine producer claim alive while its value is
// destroyed, including early errors and cancellation of an async helper.
struct NativeOwned<T> { value: T, custody: NativeAllocation }
fn owned<T>((value, custody): (T, NativeAllocation)) -> NativeOwned<T> {
    NativeOwned { value, custody }
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

/// Parent supplies its retained graph check. This stages only immutable inputs
/// with protected original-owner authority; it does not send as that root.
/// The returned command MUST be executed by caller's original workflow/lease.
#[allow(clippy::too_many_arguments, reason = "explicit original task and protected immutable input owner")]
pub(crate) async fn prepare_send<P: StreamProvider, F: FnOnce(TaskId, TaskId) -> Result<()>>(
    context: &TaskContext, owner: &TaskJournalOwner<P>, protected: &PiOwnerConfig,
    filesystem: &FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>,
    operation: OperationId, recipient: TaskId, envelope: PiMailEnvelope,
    authorize: F, work: &NativeMaterializationWork,
) -> Result<(WorkflowCommand, NativeAllocation)> {
    let caller = authenticate(context, owner, work).await?;
    authorize(caller, recipient)?;
    let command = owned(stage_send(protected, filesystem, operation, caller, recipient, envelope, work).await?);
    owner.verify_with_native_work(false, work).await?;
    Ok((command.value, command.custody))
}

/// Trusted actor input staging for a passive root. No mail is dispatched here;
/// the resulting command still needs its original task's fresh execution owner.
#[allow(clippy::too_many_arguments, reason = "original protected actor and exact immutable sender/recipient input")]
pub(crate) async fn stage_send(
    protected: &PiOwnerConfig,
    filesystem: &FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>,
    operation: OperationId, caller: TaskId, recipient: TaskId, envelope: PiMailEnvelope,
    work: &NativeMaterializationWork,
) -> Result<(WorkflowCommand, NativeAllocation)> {
    validate_envelope(&envelope)?;
    let stored = PiMailPayload { sender: caller, envelope };
    let staged = owned(stage_pi_input_with_native_work(
        protected, filesystem, operation, "mail-envelope", &stored, work).await?);
    let payload = MailSendTaskCommand { recipient, payload: staged.value };
    let mut command = owned(stage_pi_input_with_native_work(
        protected, filesystem, operation, "mail-send", &payload, work).await?);
    drop(payload);
    drop(staged.custody);
    let kind_length = MAIL_SEND_TASK_COMMAND_KIND.len() as u64;
    work.charge(WorkCounters { bytes_copied: kind_length, allocation_operations: 1, ..Default::default() })
        .map_err(native_error)?;
    let mut kind_custody = work.retain_owned(kind_length).map_err(native_error)?;
    let kind = MAIL_SEND_TASK_COMMAND_KIND.to_owned();
    if let Err(cause) = command.custody.absorb(&mut kind_custody) {
        drop(kind);
        drop(command.value);
        drop(kind_custody);
        return Err(Error::NativeWork { cause, custody: Some(command.custody) });
    }
    Ok((WorkflowCommand { operation_id: operation, kind, payload: command.value }, command.custody))
}

/// A normal control command; the hosting root must resume its original passive
/// checkpoint before executing it. Children use their native process lease.
pub(crate) async fn execute_send<P, A, O>(runtime: &FilesystemTaskRuntime<P, A, O>,
    context: TaskContext, owner: &TaskJournalOwner<P>, fence: LeaseFence, command: WorkflowCommand,
    work: &NativeMaterializationWork,
) -> Result<TaskCommandProgress>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    authenticate(&context, owner, work).await?;
    if command.kind != MAIL_SEND_TASK_COMMAND_KIND {
        return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid,
            reason: "Pi send requires original stock mail command" });
    }
    let bytes = owned(context.read_file_with_native_work(&command.payload, work).await?);
    let payload = owned(decode::<MailSendTaskCommand>(&bytes.value, work)?);
    drop(bytes);
    if payload.value.payload.descriptor().byte_length() > MAXIMUM_PI_MAIL_BYTES {
        return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid,
            reason: "Pi send payload exceeds original bounded envelope" });
    }
    let value = owned(guarded_pi_value(&payload.value, MAXIMUM_PI_MAIL_BYTES * 8, work)?);
    drop(payload);
    let commands = runtime.commands();
    let result = commands.execute_with_native_work(context, fence, command, value.value, work)?.await;
    drop(value.custody);
    result
}

const MAIL_DIAGNOSTIC_LABEL_BYTES: u64 =
    super::pi_root_provider::ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES
    + super::pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
        "PiMailKind", "message", "steer", "PiMailEnvelope", "kind",
        "PiMailPayload", "sender", "envelope", "MailSendTaskCommand",
        "recipient", "payload",
    ]);

trait NativeMailboxDto: serde::de::DeserializeOwned {}
impl NativeMailboxDto for PiMailEnvelope {}
impl NativeMailboxDto for PiMailPayload {}
impl NativeMailboxDto for MailSendTaskCommand {}

fn decode<T: NativeMailboxDto>(
    bytes: &[u8], work: &NativeMaterializationWork,
) -> Result<(T, NativeAllocation)> {
    // These concrete DTOs contain only fixed metadata, Strings and FileRefs.
    // The SDK's issued source-shape bound covers their dynamic slots and
    // strings; this witness adds the concrete inline envelope before serde.
    guarded_pi_decode(bytes, MAXIMUM_PI_MAIL_BYTES, std::mem::size_of::<T>() as u64,
        acyclic_harness::JsonValueSchema::None, MAIL_DIAGNOSTIC_LABEL_BYTES, work)
}

/// Passive polling acquires no reservation and charges no workflow task steps.
/// The service supplies its authenticated retained caller binding, never tool JSON.
pub(crate) async fn receive<P, A, O>(runtime: &FilesystemTaskRuntime<P, A, O>,
    caller: &super::PiTaskBinding, after: u64, maximum: u32, work: &NativeMaterializationWork,
) -> Result<(Option<PiMailboxReceive>, NativeAllocation)>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    if maximum != 1 { return Err(Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi receive selects exactly one item" }); }
    let next = after.checked_add(1).ok_or(Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi mailbox sequence overflows" })?;
    let mut items = owned(runtime.task_host().inbox_with_native_work(caller.task_id, after, 1, work).await?);
    let Some(item) = items.value.pop() else { return Ok((None, items.custody)); };
    if item.task_id != caller.task_id || item.sequence != next {
        return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Conflict,
            reason: "Pi passive receive selected a different sequence" });
    }
    if item.payload.descriptor().byte_length() > MAXIMUM_PI_MAIL_BYTES {
        return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid,
            reason: "Pi mailbox payload exceeds bounded decode" });
    }
    let bytes = owned(runtime.task_host()
        .read_recipient_file_with_native_work(caller.task_id, &item.payload, work).await?);
    let mut payload = owned(decode::<PiMailPayload>(&bytes.value, work)?);
    drop(bytes);
    if let Err(cause) = items.custody.absorb(&mut payload.custody) {
        drop(payload);
        drop(item);
        drop(items.value);
        return Err(Error::NativeWork { cause, custody: Some(items.custody) });
    }
    Ok((Some(PiMailboxReceive { item, sender: payload.value.sender, envelope: payload.value.envelope }), items.custody))
}


/// Passive wait uses only the original admitted task and bounded own inbox.
/// No process future or execution lease is created by a wait/poll request.
pub(crate) async fn wait<P, A, O, F>(runtime: &FilesystemTaskRuntime<P, A, O>,
    caller: &super::PiTaskBinding, requested: TaskId, after: u64, authorize: F,
    work: &NativeMaterializationWork,
) -> Result<(PiWaitObservation, NativeAllocation)>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static,
      F: FnOnce(TaskId, TaskId) -> Result<()> {
    authorize(caller.task_id, requested)?;
    let mut mail = owned(receive(runtime, caller, after, 1, work).await?);
    let mut outcome = owned(runtime.task_host().outcome_with_native_work(requested, work).await?);
    let state = match &outcome.value {
        None | Some(Outcome::Indeterminate { .. }) => PiWaitState::Pending,
        Some(_) => PiWaitState::Completed,
    };
    if let Err(cause) = mail.custody.absorb(&mut outcome.custody) {
        drop(outcome);
        drop(mail.value);
        return Err(Error::NativeWork { cause, custody: Some(mail.custody) });
    }
    Ok((PiWaitObservation { task: requested, state, outcome: outcome.value, mail: mail.value }, mail.custody))
}

#[cfg(test)]
#[path = "pi_mail_tests.rs"]
mod tests;
