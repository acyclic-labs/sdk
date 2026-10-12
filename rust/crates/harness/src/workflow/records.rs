use super::*;

/// Replay-safe command with immutable arguments.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowCommand {
    /// Stable dispatch and reconciliation identity.
    pub operation_id: OperationId,
    /// Pinned executor or effect kind.
    pub kind: String,
    /// Owner-resolved immutable arguments.
    pub payload: FileRef,
}
impl WorkflowCommand {
    /// Validates command structure before commit.
    pub fn validate(&self, work: &mut Option<&mut dyn crate::store::HistoryIo>) -> Result<()> {
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
            observer.codec_charge(crate::executor::json::JsonCodecCharge::Inspect { items: 1 })?;
        }
        if !crate::contract::is_valid_component_label(&self.kind) {
            return Err(crate::scheduler::diagnostic(
                work,
                Invalid,
                "workflow command kind is invalid",
            ));
        }
        self.payload.validate(work)
    }
}

/// Suspension or terminal result of a machine step.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MachineStatus {
    /// Await another durable input.
    Suspended,
    /// Successful terminal result.
    Completed {
        /// Schema-defined result.
        value: Value,
    },
    /// Deterministic terminal failure.
    Failed {
        /// Stable failure description.
        message: String,
    },
}

/// One deterministic step and its ordered outbox.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineTransition {
    /// Complete next state.
    pub state: Value,
    /// Replay-safe commands.
    pub commands: Vec<WorkflowCommand>,
    /// Suspension or completion.
    pub status: MachineStatus,
}

/// Durable state pinned to an exact implementation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineCheckpoint {
    /// Implementation identity.
    pub machine: MachineIdentity,
    /// Number of applied inputs.
    pub revision: u64,
    /// Complete durable state.
    pub state: Value,
}

/// Atomic transition, including the durable input and outbox.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowRecord {
    /// Input operation identity.
    pub operation_id: OperationId,
    /// Permanently bound retry identity.
    pub idempotency_key: IdempotencyKey,
    /// Exact prior checkpoint.
    pub prior: MachineCheckpoint,
    /// Input for deterministic replay.
    pub input: Value,
    /// Step result and commands.
    pub transition: MachineTransition,
}
impl WorkflowRecord {
    /// Borrows the checkpoint committed by this transition.
    pub fn checkpoint(&self) -> Result<(&MachineIdentity, u64, &Value)> {
        Ok((
            &self.prior.machine,
            next_revision(self.prior.revision, None)?,
            &self.transition.state,
        ))
    }
    /// All identities reserved by this atomic transition.
    pub fn operation_ids(&self) -> impl Iterator<Item = OperationId> + '_ {
        std::iter::once(self.operation_id).chain(
            self.transition
                .commands
                .iter()
                .map(|command| command.operation_id),
        )
    }
    /// Validates the envelope before storage or replay.
    pub fn validate(&self, io: Option<&PreparationIo>) -> Result<()> {
        validate_key(&self.idempotency_key, io)?;
        crate::executor::preparation_digest(&self.input, io)?;
        next_revision(self.prior.revision, io)?;
        validate_commands(&self.transition.commands, io)?;
        if self
            .transition
            .commands
            .iter()
            .any(|command| command.operation_id == self.operation_id)
        {
            return Err(stock_boundary(
                io,
                Conflict,
                "workflow command reuses its transition identity",
            ));
        }
        Ok(())
    }
}

/// Immutable admission of one workflow instance.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowAdmission {
    /// Caller operation identity.
    pub operation_id: OperationId,
    /// Canonical definition and input digest.
    pub request_digest: [u8; 32],
    /// Initial checkpoint admitted before effects.
    pub initial: MachineCheckpoint,
}
impl WorkflowAdmission {
    /// Validates the retained admission identity.
    pub fn validate(&self, io: Option<&PreparationIo>) -> Result<()> {
        if self.request_digest == [0; 32] || self.initial.revision != 0 {
            return Err(stock_boundary(
                io,
                Invalid,
                "workflow admission identity is invalid",
            ));
        }
        with_history_io(io, |work| self.initial.machine.validate(work))
    }
}

pub(super) fn validate_commands(
    commands: &[WorkflowCommand],
    io: Option<&PreparationIo>,
) -> Result<()> {
    #[cfg(feature = "filesystem")]
    let _workspace = io
        .map(|io| crate::executor::reserve_preparation_workspace(&commands, io))
        .transpose()?;
    with_history_io(io, |work| {
        let bytes = commands
            .len()
            .checked_mul(std::mem::size_of::<OperationId>())
            .ok_or_else(|| {
                crate::scheduler::diagnostic(
                    work,
                    Invalid,
                    "workflow command set ownership overflow",
                )
            })? as u64;
        let mut quota = crate::wire_codec::CodecQuota::new(work, bytes)?;
        let mut identities = BTreeSet::new();
        for command in commands {
            command.validate(quota.work())?;
            if let Some(observer) = quota.work().as_deref_mut() {
                observer.codec_charge(crate::executor::json::JsonCodecCharge::Inspect { items: 1 })?;
            }
            if !identities.insert(command.operation_id) {
                return Err(crate::scheduler::diagnostic(
                    quota.work(),
                    Conflict,
                    "workflow command identity is duplicated",
                ));
            }
        }
        Ok(())
    })
}

pub(crate) fn validate_workflow_commit(
    expected: u64,
    key: &IdempotencyKey,
    record: &WorkflowRecord,
    io: Option<&PreparationIo>,
) -> Result<()> {
    validate_key(key, io)?;
    record.validate(io)?;
    if record.idempotency_key != *key || record.prior.revision != expected {
        return Err(stock_boundary(
            io,
            Invalid,
            "workflow commit identity or revision is invalid",
        ));
    }
    Ok(())
}

pub(crate) fn validate_workflow_next(
    checkpoint: (&MachineIdentity, u64, &Value),
    terminal: bool,
    reserved: &BTreeSet<OperationId>,
    keys: &BTreeSet<IdempotencyKey>,
    record: &WorkflowRecord,
    io: Option<&PreparationIo>,
) -> Result<()> {
    record.validate(io)?;
    if (
        &record.prior.machine,
        record.prior.revision,
        &record.prior.state,
    ) != checkpoint
        || terminal
    {
        return Err(stock_boundary(
            io,
            Conflict,
            "workflow transition differs from its current checkpoint",
        ));
    }
    if keys.contains(&record.idempotency_key)
        || record.operation_ids().any(|id| reserved.contains(&id))
    {
        return Err(stock_boundary(
            io,
            Conflict,
            "workflow command, transition or retry identity is already bound",
        ));
    }
    Ok(())
}
