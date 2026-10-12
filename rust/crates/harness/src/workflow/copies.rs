use super::*;
use crate::{core::{copy_file, copy_text, copy_value, copy_vector}, wire_codec::CodecQuota};

macro_rules! copy {
    ($kind:ty, $source:ident, $quota:ident, $body:expr) => {
        impl $kind {
            pub(super) fn copy(&self, io: Option<&PreparationIo>) -> Result<Self> {
                if io.is_none() { return Ok(self.clone()); }
                with_history_io(io, |work| {
                    let mut quota = CodecQuota::new(work, 0)?;
                    let value = self.copy_fields(&mut quota)?;
                    quota.transfer();
                    Ok(value)
                })
            }
            fn copy_fields(&$source, $quota: &mut CodecQuota<'_, '_>) -> Result<Self> { $body }
        }
    };
}

impl MachineIdentity {
    fn copy_fields(&self, quota: &mut CodecQuota<'_, '_>) -> Result<Self> {
        Ok(Self { name: copy_text(&self.name, quota)?, version: copy_text(&self.version, quota)?, digest: self.digest })
    }
}

pub(crate) fn workflow_copy_identity(source: &MachineIdentity, io: Option<&PreparationIo>) -> Result<MachineIdentity> {
    if io.is_none() { return Ok(source.clone()); }
    with_history_io(io, |work| {
        let mut quota = CodecQuota::new(work, 0)?;
        let value = source.copy_fields(&mut quota)?;
        quota.transfer();
        Ok(value)
    })
}
copy!(MachineCheckpoint, self, quota, Ok(Self {
    machine: self.machine.copy_fields(quota)?, revision: self.revision, state: copy_value(&self.state, quota)?,
}));
copy!(MachineTransition, self, quota, Ok(Self {
    state: copy_value(&self.state, quota)?,
    commands: copy_vector(&self.commands, quota, |command, quota| Ok(WorkflowCommand {
        operation_id: command.operation_id, kind: copy_text(&command.kind, quota)?, payload: copy_file(&command.payload, quota)?,
    }))?,
    status: match &self.status {
        MachineStatus::Suspended => MachineStatus::Suspended,
        MachineStatus::Completed { value } => MachineStatus::Completed { value: copy_value(value, quota)? },
        MachineStatus::Failed { message } => MachineStatus::Failed { message: copy_text(message, quota)? },
    },
}));
copy!(WorkflowRecord, self, quota, Ok(Self {
    operation_id: self.operation_id, idempotency_key: IdempotencyKey(copy_text(&self.idempotency_key.0, quota)?),
    input_digest: self.input_digest, prior: self.prior.copy_fields(quota)?, input: copy_value(&self.input, quota)?,
    transition: self.transition.copy_fields(quota)?, next: self.next.copy_fields(quota)?,
}));
