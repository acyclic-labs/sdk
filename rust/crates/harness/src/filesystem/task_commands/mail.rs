//! Mail branches share their original stock dispatch with optional native custody.

use super::*;
use crate::filesystem::{PreparationIo, PreparationRecord};
use crate::runtime::TaskStateProvider;

impl PreparationRecord for MailSendTaskCommand {}
impl PreparationRecord for MailReceiveTaskCommand {}

fn require_native_observer(io: Option<&PreparationIo>) -> Result<()> {
    if let Some(io) = io {
        if io.original_native_work().is_none() {
            return Err(super::super::task_runtime::observed::boundary(
                Some(io),
                crate::wire::ErrorCode::Unsupported,
                "mail observer requires the original native work",
            ));
        }
        io.check_cancellation()?;
    }
    Ok(())
}

pub(super) fn decode_command<T: PreparationRecord>(
    payload: Value,
    io: Option<&PreparationIo>,
) -> Result<T> {
    let Some(io) = io else {
        return serde_json::from_value(payload)
            .map_err(|cause| Error::Invalid(format!("invalid stock command: {cause}")));
    };
    decode_borrowed_command(&payload, io)
}

pub(super) fn decode_borrowed_command<T: PreparationRecord>(
    payload: &Value,
    io: &PreparationIo,
) -> Result<T> {
    #[cfg(not(target_arch = "wasm32"))]
    io.check_json_byte_bound(payload, acyclic_stream::MAX_RECORD_BYTES as u64)?;
    Ok(crate::filesystem::retain_decoded(
        io.value_decode_preparation(payload, crate::wire::ErrorCode::Storage)?,
    ))
}

pub(super) fn reconciled(io: Option<&PreparationIo>) -> Result<TaskCommandProgress> {
    require_native_observer(io)?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io {
        let custody = io.take_native_custody(0)?;
        return Ok(TaskCommandProgress::ReadyWithNativeCustody {
            value: Value::Null,
            custody,
        });
    }
    Ok(TaskCommandProgress::Ready(Value::Null))
}

impl<P, A, O> FilesystemTaskCommands<'_, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn mail_command(
        &self,
        task: TaskId,
        fence: &LeaseFence,
        command: &WorkflowCommand,
        payload: Value,
        io: Option<&PreparationIo>,
        select_only: bool,
    ) -> Result<TaskCommandProgress> {
        require_native_observer(io)?;
        if command.kind == MAIL_SEND_TASK_COMMAND_KIND {
            let input: MailSendTaskCommand = decode_command(payload, io)?;
            let message =
                super::super::task_runtime::execution_operation(task, command.operation_id, io)?;
            self.runtime
                .task_host()
                .send_mail(task, fence, input.recipient, message, input.payload, io)
                .await?;
            ready(Outcome::Succeeded(Value::Null), io)
        } else {
            let input: MailReceiveTaskCommand = decode_command(payload, io)?;
            let Some(item) = self.inbox(task, input.after, io).await?.into_iter().next() else {
                return if select_only {
                    ready(Outcome::Succeeded(Value::Null), io)
                } else {
                    Ok(TaskCommandProgress::Pending)
                };
            };
            ready(Outcome::Succeeded(item), io)
        }
    }

    pub(super) async fn reconcile_mail(
        &self,
        task: TaskId,
        fence: &LeaseFence,
        command: &WorkflowCommand,
        payload: Value,
        io: Option<&PreparationIo>,
    ) -> Result<TaskCommandProgress> {
        require_native_observer(io)?;
        if command.kind == MAIL_SEND_TASK_COMMAND_KIND {
            let input: MailSendTaskCommand = decode_command(payload, io)?;
            let message =
                super::super::task_runtime::execution_operation(task, command.operation_id, io)?;
            self.runtime
                .task_host()
                .reconcile_sent_mail(task, fence, input.recipient, message, input.payload, io)
                .await?;
        }
        reconciled(io)
    }

    pub(super) async fn wait_mail(
        &self,
        task: TaskId,
        after: u64,
        io: Option<&PreparationIo>,
    ) -> Result<bool> {
        require_native_observer(io)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io
            && let Some(work) = io.original_native_work()
        {
            let (items, custody) = self
                .runtime
                .task_host()
                .inbox_with_native_work(task, after, 1, work)
                .await?;
            let ready = !items.is_empty();
            drop((items, custody));
            return Ok(ready);
        }
        Ok(!self
            .runtime
            .task_host()
            .inbox(task, after, 1)
            .await?
            .is_empty())
    }

    pub(super) async fn inbox(
        &self,
        task: TaskId,
        after: u64,
        io: Option<&PreparationIo>,
    ) -> Result<Vec<crate::scheduler::InboxItem>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io
            && let Some(work) = io.original_native_work()
        {
            let (items, custody) = self
                .runtime
                .task_host()
                .inbox_with_native_work(task, after, 1, work)
                .await?;
            io.absorb_native_custody(custody)?;
            return Ok(items);
        }
        self.runtime.task_host().inbox(task, after, 1).await
    }
}
