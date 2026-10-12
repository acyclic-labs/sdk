//! Mail branches share their original stock dispatch with optional native custody.

use super::*;
use crate::filesystem::{PreparationIo, PreparationRecord};
use acyclic_fs::WorkCounters;

impl PreparationRecord for MailSendTaskCommand {
    fn maximum_slot_bytes() -> u64 { crate::filesystem::fork_preparer::bounded_io::preparation_slot_bytes::<Self>() }
    fn diagnostic_label_bytes() -> u64 { crate::contract::preparation_serde_diagnostic_label_bytes() }
}
impl PreparationRecord for MailReceiveTaskCommand {
    fn maximum_slot_bytes() -> u64 { crate::filesystem::fork_preparer::bounded_io::preparation_slot_bytes::<Self>() }
    fn diagnostic_label_bytes() -> u64 { crate::contract::preparation_serde_diagnostic_label_bytes() }
}

fn require_native_observer(io: Option<&PreparationIo>) -> Result<()> {
    let Some(io) = io else { return Ok(()); };
    #[cfg(not(target_arch = "wasm32"))]
    {
        if io.original_native_work().is_some() { return io.check_cancellation(); }
        return Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "mail observer requires the original native work",
        });
    }
    #[cfg(target_arch = "wasm32")]
    {
        let _ = io;
        Err(Error::Unsupported("mail native observer is unavailable on wasm".into()))
    }
}

pub(super) fn decode_command<T: PreparationRecord>(payload: Value, io: Option<&PreparationIo>) -> Result<T> {
    let Some(io) = io else {
        return serde_json::from_value(payload)
            .map_err(|cause| Error::Invalid(format!("invalid stock command: {cause}")));
    };
    decode_borrowed_command(&payload, io)
}

pub(super) fn decode_borrowed_command<T: PreparationRecord>(payload: &Value, io: &PreparationIo) -> Result<T> {
    let encoded_hold;
    let encoded = io.encode_json(payload, acyclic_stream::MAX_RECORD_BYTES as u64, false)?;
    encoded_hold = io.guard_bytes(&encoded)?;
    let decoded = io.decode_preparation::<T>(&encoded)?.into_retained();
    drop(encoded);
    drop(encoded_hold);
    Ok(decoded)
}

fn message_identity(task: TaskId, command: OperationId, io: Option<&PreparationIo>) -> Result<OperationId> {
    if let Some(io) = io {
        let hash = WorkCounters { bytes_hashed: b"harness/v2/task-execution\0".len() as u64 + 32, ..Default::default() };
        io.preflight(hash)?;
        io.charge(hash)?;
    }
    Ok(super::super::task_runtime::execution_operation(task, command))
}

pub(super) fn mail_ready(value: impl Serialize, io: Option<&PreparationIo>) -> Result<TaskCommandProgress> {
    let Some(io) = io else { return ready(value); };
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(native) = io.original_native_work() {
        let output = PreparationIo::from_native_work(native)?;
        let encoded_hold;
        let encoded = output.encode_json(&value, acyclic_stream::MAX_RECORD_BYTES as u64, false)?;
        encoded_hold = output.guard_bytes(&encoded)?;
        let value = output.decode_preparation::<Value>(&encoded)?.into_retained();
        drop(encoded);
        drop(encoded_hold);
        let retained = output.owned_bytes()?;
        let custody = output.into_native_custody(retained)?;
        return Ok(TaskCommandProgress::ReadyWithNativeCustody { value, custody });
    }
    Err(Error::Unsupported("mail result observer requires the original native work".into()))
}

pub(super) fn reconciled(io: Option<&PreparationIo>) -> Result<TaskCommandProgress> {
    require_native_observer(io)?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io {
        let custody = io.take_native_custody(0)?;
        return Ok(TaskCommandProgress::ReadyWithNativeCustody { value: Value::Null, custody });
    }
    Ok(TaskCommandProgress::Ready(Value::Null))
}

impl<P, A, O> FilesystemTaskCommands<'_, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn mail_command_with_io(
        &self, task: TaskId, _context: &TaskContext, fence: &LeaseFence,
        command: &WorkflowCommand, payload: Value, io: Option<&PreparationIo>,
        select_only: bool,
    ) -> Result<TaskCommandProgress> {
        require_native_observer(io)?;
        if command.kind == MAIL_SEND_TASK_COMMAND_KIND {
            let input: MailSendTaskCommand = decode_command(payload, io)?;
            let message = message_identity(task, command.operation_id, io)?;
            self.runtime.task_host().send_owned_with_io(task, fence, input.recipient, message, input.payload, io).await?;
            mail_ready(Outcome::Succeeded(Value::Null), io)
        } else {
            let input: MailReceiveTaskCommand = decode_command(payload, io)?;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(io) = io && let Some(native) = io.original_native_work() {
                let (items, custody) = self.runtime.task_host().inbox_with_native_work(task, input.after, 1, native).await?;
                io.absorb_native_custody(custody)?;
                let Some(item) = items.into_iter().next() else {
                    return if select_only { mail_ready(Outcome::Succeeded(Value::Null), io) }
                        else { Ok(TaskCommandProgress::Pending) };
                };
                return mail_ready(Outcome::Succeeded(item), Some(io));
            }
            let items = self.runtime.task_host().inbox(task, input.after, 1).await?;
            let Some(item) = items.into_iter().next() else {
                    return if select_only { mail_ready(Outcome::Succeeded(Value::Null), io) }
                        else { Ok(TaskCommandProgress::Pending) };
                };
            mail_ready(Outcome::Succeeded(item), io)
        }
    }

    pub(super) async fn reconcile_mail_with_io(
        &self, task: TaskId, fence: &LeaseFence, command: &WorkflowCommand,
        payload: Value, io: Option<&PreparationIo>,
    ) -> Result<TaskCommandProgress> {
        require_native_observer(io)?;
        if command.kind == MAIL_SEND_TASK_COMMAND_KIND {
            let input: MailSendTaskCommand = decode_command(payload, io)?;
            let message = message_identity(task, command.operation_id, io)?;
            self.runtime.task_host().reconcile_sent_mail(task, fence, input.recipient, message, input.payload, io).await?;
        }
        reconciled(io)
    }

    pub(super) async fn wait_mail_with_io(
        &self, task: TaskId, _context: &TaskContext, after: u64, io: Option<&PreparationIo>,
    ) -> Result<bool> {
        require_native_observer(io)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io && let Some(native) = io.original_native_work() {
            let (items, custody) = self.runtime.task_host().inbox_with_native_work(task, after, 1, native).await?;
            let ready = !items.is_empty();
            drop(items);
            drop(custody);
            return Ok(ready);
        }
        let items = self.runtime.task_host().inbox(task, after, 1).await?;
        Ok(!items.is_empty())
    }
}
