//! Stock mail commands over the existing task host.
use super::*;
use crate::filesystem::{PreparationIo, PreparationRecord};
use crate::runtime::TaskStateProvider;

impl PreparationRecord for MailSendTaskCommand {}
impl PreparationRecord for MailReceiveTaskCommand {}

pub(super) fn require_native_observer(io: Option<&PreparationIo>) -> Result<()> {
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
    match io {
        Some(io) => {
            #[cfg(not(target_arch = "wasm32"))]
            io.check_json_byte_bound(&payload, acyclic_stream::MAX_RECORD_BYTES as u64)?;
            Ok(crate::filesystem::retain_decoded(
                io.value_decode_preparation(&payload, crate::wire::ErrorCode::Storage)?,
            )?)
        }
        None => serde_json::from_value(payload)
            .map_err(|cause| Error::Invalid(format!("invalid stock command: {cause}"))),
    }
}

pub(super) fn reconciled(io: Option<&PreparationIo>) -> Result<TaskCommandProgress> {
    if let Some(io) = io {
        io.check_cancellation()?;
    }
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io {
        return Ok(TaskCommandProgress::ReadyWithNativeCustody {
            value: Value::Null,
            custody: io.take_native_custody(0)?,
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
        CommandInvocation {
            context,
            admission,
            fence,
            command,
            reconciliation_only,
            io,
            ..
        }: CommandInvocation<'_>,
        payload: Value,
    ) -> Result<TaskCommandProgress> {
        let task = admission.task_id();
        let select_only = admission.task.name == "acyclic.stock_turn";
        if let Some(io) = io {
            io.check_cancellation()?;
        }
        if command.kind == MAIL_SEND_TASK_COMMAND_KIND {
            let input: MailSendTaskCommand = decode_command(payload, io)?;
            let message =
                super::super::task_runtime::execution_operation(task, command.operation_id, io)?;
            let host = self.runtime.task_host();
            if reconciliation_only {
                host.reconcile_sent_mail(task, fence, input.recipient, message, input.payload, io)
                    .await?;
                return reconciled(io);
            }
            if let Err(error) = host
                .send_mail_with_preflight(task, fence, input.recipient, message, input.payload, io)
                .await?
            {
                return query::failure(error, io);
            }
            return ready(Outcome::Succeeded(Value::Null), io);
        }
        if reconciliation_only {
            return reconciled(io);
        }
        let input: MailReceiveTaskCommand = decode_command(payload, io)?;
        let after = if super::super::child_agent::is_child(admission) {
            self.child_mail_after(context, task, fence, command, io)
                .await?
        } else {
            input.after
        };
        match self.inbox(task, after, io).await?.into_iter().next() {
            Some(item) => ready(Outcome::Succeeded(item), io),
            None if select_only => ready(Outcome::Succeeded(Value::Null), io),
            None => Ok(TaskCommandProgress::Pending),
        }
    }

    pub(super) async fn wait_mail(
        &self,
        task: TaskId,
        after: u64,
        io: Option<&PreparationIo>,
    ) -> Result<bool> {
        if let Some(io) = io {
            io.check_cancellation()?;
        }
        let scratch = PreparationIo::temporary(io)?;
        let io = scratch.as_ref().or(io);
        Ok(!self.inbox(task, after, io).await?.is_empty())
    }

    pub(super) async fn inbox(
        &self,
        task: TaskId,
        after: u64,
        _native_io: Option<&PreparationIo>,
    ) -> Result<Vec<crate::scheduler::InboxItem>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(_native_io) = _native_io
            && let Some(work) = _native_io.original_native_work()
        {
            let (items, custody) = self
                .runtime
                .task_host()
                .inbox_with_native_work(task, after, 1, work)
                .await?;
            _native_io.absorb_native_custody(custody)?;
            return Ok(items);
        }
        self.runtime.task_host().inbox(task, after, 1).await
    }
}
