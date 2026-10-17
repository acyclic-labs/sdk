//! Pin caller-readable initial commands into the workflow journal before admission.
use super::*;
use crate::distributed::SchedulerPayloadStore as _;
use crate::filesystem::{PreparationIo, TaskAdmitCommand};

impl TaskAdmitCommand {
    pub(crate) fn initial_command(
        &self,
        io: Option<&PreparationIo>,
    ) -> Result<Option<WorkflowCommand>> {
        if self.version != "1"
            || !matches!(
                self.name.as_str(),
                "acyclic.command_session" | "acyclic.direct_tool"
            )
        {
            return Ok(None);
        }
        let value = self.input.get("command").ok_or_else(|| {
            observed::boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "initial command is absent",
            )
        })?;
        let command: WorkflowCommand = match io {
            Some(io) => crate::filesystem::retain_decoded(
                io.value_decode_preparation(value, crate::wire::ErrorCode::Invalid)?,
            )?,
            None => serde::Deserialize::deserialize(value)
                .map_err(|cause| Error::Invalid(format!("invalid initial command: {cause}")))?,
        };
        crate::executor::with_history_io(io, |work| command.validate(work))?;
        Ok(Some(command))
    }
}

impl<P, A, O> FilesystemTaskRuntime<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(in crate::filesystem) async fn stage_initial_command(
        &self,
        context: &TaskContext,
        operation: OperationId,
        input: &mut TaskAdmitCommand,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let command = input.initial_command(io)?;
        let _command_hold = io
            .map(|io| io.guard_owned(io.owned_since(before)?))
            .transpose()?;
        let Some(mut command) = command else {
            return Ok(());
        };
        if command.payload.volume() == &self.volume {
            return Ok(());
        }
        let bytes = context.read_file(&command.payload, io).await?;
        let _bytes = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        let publisher = FilesystemContentPublisher::new(
            Arc::clone(&self.filesystem),
            &self.volume,
            &self.verifier,
            &self.signed,
            self.maximum_payload_bytes
                .min(context.scope().limits().file_bytes),
            io,
        )?;
        let key = crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("child-command:{operation}"), work)
        })?;
        let _key = io
            .map(|io| io.guard_owned(key.capacity() as u64))
            .transpose()?;
        command.payload = publisher.stage_input(operation, &key, &bytes, io)?.await?;
        let value = match io {
            Some(io) => io.value_json(&command, self.maximum_payload_bytes)?,
            None => {
                serde_json::to_value(command).map_err(|cause| Error::Invalid(cause.to_string()))?
            }
        };
        let old = std::mem::replace(
            input
                .input
                .get_mut("command")
                .ok_or_else(|| Error::Invalid("initial command is absent".into()))?,
            value,
        );
        if let Some(io) = io {
            // The destination map already owns the inline Value slot. Release
            // the replaced graph and the temporary encoded root slot.
            io.release(
                crate::executor::json::json_value_logical_owned_bytes(&old)?
                    + std::mem::size_of::<Value>() as u64,
            )?;
        }
        Ok(())
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
