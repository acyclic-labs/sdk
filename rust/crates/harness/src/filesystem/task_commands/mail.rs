//! Mail branches share their original stock dispatch with optional native custody.

use super::*;
use crate::runtime::TaskStateProvider;
use crate::filesystem::{PreparationIo, PreparationRecord};

impl PreparationRecord for MailSendTaskCommand {}
impl PreparationRecord for MailReceiveTaskCommand {}

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
    let encoded = io.encode(payload, acyclic_stream::MAX_RECORD_BYTES as u64, false)?;
    encoded_hold = io.guard_bytes(&encoded)?;
    let decoded = crate::filesystem::retain_decoded(io.decode(std::marker::PhantomData::<T>, &encoded, crate::wire::ErrorCode::Storage)?);
    drop(encoded);
    drop(encoded_hold);
    Ok(decoded)
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
        &self, task: TaskId, fence: &LeaseFence,
        command: &WorkflowCommand, payload: Value, io: Option<&PreparationIo>,
        select_only: bool,
    ) -> Result<TaskCommandProgress> {
        require_native_observer(io)?;
        if command.kind == MAIL_SEND_TASK_COMMAND_KIND {
            let input: MailSendTaskCommand = decode_command(payload, io)?;
            let message = super::super::task_runtime::execution_operation(task, command.operation_id, io)?;
            self.runtime.task_host().send_mail(task, fence, input.recipient, message, input.payload, io).await?;
            ready(Outcome::Succeeded(Value::Null), io)
        } else {
            let input: MailReceiveTaskCommand = decode_command(payload, io)?;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(io) = io && let Some(native) = io.original_native_work() {
                let (items, custody) = self.runtime.task_host().inbox_with_native_work(task, input.after, 1, native).await?;
                io.absorb_native_custody(custody)?;
                let Some(item) = items.into_iter().next() else {
                    return if select_only { ready(Outcome::Succeeded(Value::Null), Some(io)) }
                        else { Ok(TaskCommandProgress::Pending) };
                };
                return ready(Outcome::Succeeded(item), Some(io));
            }
            let items = self.runtime.task_host().inbox(task, input.after, 1).await?;
            let Some(item) = items.into_iter().next() else {
                    return if select_only { ready(Outcome::Succeeded(Value::Null), io) }
                        else { Ok(TaskCommandProgress::Pending) };
                };
            ready(Outcome::Succeeded(item), io)
        }
    }

    pub(super) async fn reconcile_mail_with_io(
        &self, task: TaskId, fence: &LeaseFence, command: &WorkflowCommand,
        payload: Value, io: Option<&PreparationIo>,
    ) -> Result<TaskCommandProgress> {
        require_native_observer(io)?;
        if command.kind == MAIL_SEND_TASK_COMMAND_KIND {
            let input: MailSendTaskCommand = decode_command(payload, io)?;
            let message = super::super::task_runtime::execution_operation(task, command.operation_id, io)?;
            self.runtime.task_host().reconcile_sent_mail(task, fence, input.recipient, message, input.payload, io).await?;
        }
        reconciled(io)
    }

    pub(super) async fn wait_mail_with_io(
        &self, task: TaskId, after: u64, io: Option<&PreparationIo>,
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
