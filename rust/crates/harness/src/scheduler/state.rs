use super::*;
use crate::wire_codec::CodecQuota;
use physical::{closed_clone, copy_checkpoint, copy_text};

/// Durable operation lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationPhase {
    /// Dependencies are unresolved.
    WaitingForDependencies,
    /// Ready for capacity admission.
    WaitingForCapacity,
    /// Capacity and placement are pinned.
    Admitted,
    /// Executor owns the operation.
    Running,
    /// Parent waits without an execution reservation.
    WaitingForChildren,
    /// Workflow waits without an execution reservation.
    Suspended,
    /// External completion needs reconciliation.
    Reconciling,
    /// Terminal result is known.
    Terminal,
}

/// Current projection of one operation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OperationState {
    /// Immutable declaration.
    pub spec: OperationSpec,
    /// Lifecycle phase.
    pub phase: OperationPhase,
    /// Active execution reservation.
    pub reservation: Option<Reservation>,
    /// Latest immutable checkpoint.
    pub checkpoint: Option<CheckpointRef>,
    /// Workflow suspension and its retained resume input.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow: Option<WorkflowSuspension>,
    /// Terminal or uncertain observation.
    pub outcome: Option<Outcome<FileRef>>,
    /// Cancellation requested but not yet acknowledged.
    pub cancellation_requested: bool,
    /// Per-operation fencing revision.
    pub revision: u64,
}

/// One durable workflow suspension/resume slot.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowSuspension {
    /// Committed workflow revision.
    pub revision: u64,
    /// Passive outbox command that released the reservation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting_command: Option<OperationId>,
    /// Retained owner-authorized wake input.
    pub input: Option<FileRef>,
}

impl Outcome<FileRef> {
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        match self {
            Self::Succeeded(file) => file.logical_owned_bytes(),
            Self::Failed { message } => Ok(message.capacity() as u64),
            _ => Ok(0),
        }
    }
    pub(super) fn copy_fields(&self, quota: &mut CodecQuota<'_, '_>) -> Result<Self> {
        observe(quota.work())?;
        Ok(match self {
            Self::Succeeded(file) => Self::Succeeded(crate::core::copy_file(file, quota)?),
            Self::Failed { message } => Self::Failed {
                message: copy_text(message, quota)?,
            },
            Self::Cancelled => Self::Cancelled,
            Self::Indeterminate { operation_id } => Self::Indeterminate {
                operation_id: *operation_id,
            },
        })
    }
}
impl WorkflowSuspension {
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        self.input
            .as_ref()
            .map(FileRef::logical_owned_bytes)
            .transpose()
            .map(|bytes| bytes.unwrap_or(0))
    }
    pub(super) fn copy_fields(&self, quota: &mut CodecQuota<'_, '_>) -> Result<Self> {
        observe(quota.work())?;
        Ok(Self {
            revision: self.revision,
            waiting_command: self.waiting_command,
            input: self
                .input
                .as_ref()
                .map(|input| crate::core::copy_file(input, quota))
                .transpose()?,
        })
    }
}
impl OperationState {
    fn copy_fields(&self, quota: &mut CodecQuota<'_, '_>) -> Result<Self> {
        Ok(Self {
            spec: self.spec.copy_fields(quota)?,
            phase: self.phase,
            reservation: self
                .reservation
                .as_ref()
                .map(|value| value.copy_fields(quota))
                .transpose()?,
            checkpoint: self
                .checkpoint
                .as_ref()
                .map(|value| copy_checkpoint(value, quota))
                .transpose()?,
            workflow: self
                .workflow
                .as_ref()
                .map(|value| value.copy_fields(quota))
                .transpose()?,
            outcome: self
                .outcome
                .as_ref()
                .map(|value| value.copy_fields(quota))
                .transpose()?,
            cancellation_requested: self.cancellation_requested,
            revision: self.revision,
        })
    }
}
closed_clone!(Outcome<FileRef>);
closed_clone!(WorkflowSuspension);
closed_clone!(OperationState);
