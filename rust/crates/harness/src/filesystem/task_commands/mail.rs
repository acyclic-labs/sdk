//! Mail branches share their original stock dispatch with optional native custody.

use super::*;
use crate::filesystem::{PreparationIo, PreparationRecord};
use acyclic_fs::WorkCounters;
use std::mem::size_of;

impl PreparationRecord for MailSendTaskCommand {
    fn maximum_slot_bytes() -> u64 { size_of::<Self>().max(size_of::<Value>()) as u64 }
}
impl PreparationRecord for MailReceiveTaskCommand {
    fn maximum_slot_bytes() -> u64 { size_of::<Self>().max(size_of::<Value>()) as u64 }
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
    let encoded_hold;
    let encoded = io.encode_json(&payload, acyclic_stream::MAX_RECORD_BYTES as u64, false)?;
    encoded_hold = io.guard_owned(encoded.capacity() as u64)?;
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
        // A second custody owner, not a second ledger: both borrow the caller's
        // original work. The actual output Value is admitted before its serde
        // strings/vectors and then moved together with that same original guard.
        let output = PreparationIo::from_native_work(native)?;
        let encoded_hold;
        let encoded = output.encode_json(&value, acyclic_stream::MAX_RECORD_BYTES as u64, false)?;
        encoded_hold = output.guard_owned(encoded.capacity() as u64)?;
        let value = output.decode_preparation::<Value>(&encoded)?.into_retained();
        drop(encoded);
        drop(encoded_hold);
        let retained = output.owned_bytes()?;
        let custody = output.into_native_custody(retained)?;
        return Ok(TaskCommandProgress::ReadyWithNativeCustody { value, custody });
    }
    Err(Error::Unsupported("mail result observer requires the original native work".into()))
}

fn reconciled(io: Option<&PreparationIo>) -> Result<TaskCommandProgress> {
    require_native_observer(io)?;
    mail_ready(Value::Null, io)
}

impl<P, A, O> FilesystemTaskCommands<'_, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn mail_command_with_io(
        &self, task: TaskId, context: &TaskContext, fence: &LeaseFence,
        command: &WorkflowCommand, payload: Value, io: Option<&PreparationIo>,
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
                let Some(item) = items.into_iter().next() else { return Ok(TaskCommandProgress::Pending); };
                let (bytes, body_custody) = self.runtime.task_host()
                    .read_recipient_file_with_native_work(task, &item.payload, native).await?;
                drop(bytes);
                drop(body_custody);
                return mail_ready(Outcome::Succeeded(item), Some(io));
            }
            let items = self.runtime.task_host().inbox(task, input.after, 1).await?;
            let Some(item) = items.into_iter().next() else { return Ok(TaskCommandProgress::Pending); };
            // The first immutable item is stable across a crash before checkpoint
            // publication. Native observation neither acknowledges nor advances it.
            context.read_file(&item.payload).await?;
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
            self.runtime.task_host().reconcile_sent_mail_with_io(task, fence, input.recipient, message, input.payload, io).await?;
        }
        // Receive is a passive observation. Reconciliation never publishes mail,
        // acknowledges the recipient, advances its cursor, or opens another queue.
        reconciled(io)
    }

    pub(super) async fn wait_mail_with_io(
        &self, task: TaskId, context: &TaskContext, after: u64, io: Option<&PreparationIo>,
    ) -> Result<bool> {
        require_native_observer(io)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io && let Some(native) = io.original_native_work() {
            let (items, custody) = self.runtime.task_host().inbox_with_native_work(task, after, 1, native).await?;
            let ready = if let Some(item) = items.first() {
                let (bytes, body_custody) = self.runtime.task_host()
                    .read_recipient_file_with_native_work(task, &item.payload, native).await?;
                drop(bytes);
                drop(body_custody);
                true
            } else { false };
            drop(items);
            drop(custody);
            return Ok(ready);
        }
        let items = self.runtime.task_host().inbox(task, after, 1).await?;
        if let Some(item) = items.first() { context.read_file(&item.payload).await?; Ok(true) }
        else { Ok(false) }
    }
}
