use super::*;
use crate::wire_codec::CodecQuota;
use physical::{add, closed_clone, copy_checkpoint, copy_text, slots};

/// One committed deterministic scheduler transition.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SchedulerEvent {
    /// Pins the root-session budget before descendants or dispatch.
    SessionConfigured {
        /// Root operation.
        operation_id: OperationId,
        /// Immutable ceilings.
        limits: SessionLimits,
    },
    /// Charges an exact model request before dispatch.
    ModelDispatchClaimed {
        /// Owning task operation.
        operation_id: OperationId,
        /// Durable turn identity.
        attempt_id: OperationId,
        /// Turn-local model step.
        step: u32,
        /// Canonical request digest.
        request_digest: [u8; 32],
        /// Current execution owner.
        fence: LeaseFence,
        /// Admitted task ceiling within the session ceiling.
        ceiling: u64,
    },
    /// Declares a new operation.
    Declared {
        /// Immutable declaration.
        spec: Box<OperationSpec>,
    },
    /// Dependencies resolved and capacity admission is pending.
    WaitingForCapacity {
        /// Ready operation.
        operation_id: OperationId,
    },
    /// Fully pins execution capacity and placement.
    Admitted {
        /// Admitted operation.
        operation_id: OperationId,
        /// Pinned allocation.
        reservation: Reservation,
    },
    /// Reserves capacity without starting execution.
    PartiallyAdmitted {
        /// Partially admitted operation.
        operation_id: OperationId,
        /// Partial allocation.
        reservation: Reservation,
    },
    /// Rejects work before execution.
    Rejected {
        /// Rejected operation.
        operation_id: OperationId,
        /// Stable reason.
        reason: String,
    },
    /// Starts owned execution.
    Started {
        /// Started operation.
        operation_id: OperationId,
        /// Active lease fence.
        fence: LeaseFence,
    },
    /// Advances an execution checkpoint.
    Checkpointed {
        /// Owned operation.
        operation_id: OperationId,
        /// Immutable checkpoint.
        checkpoint: CheckpointRef,
        /// Active lease fence.
        fence: LeaseFence,
    },
    /// Releases a parent's capacity while its children run.
    WaitingForChildren {
        /// Suspended parent.
        operation_id: OperationId,
        /// Released lease fence.
        fence: LeaseFence,
    },
    /// Releases execution capacity at a committed workflow suspension.
    WorkflowSuspended {
        /// Owned task operation.
        operation_id: OperationId,
        /// Released lease fence.
        fence: LeaseFence,
        /// Committed workflow revision.
        workflow_revision: u64,
        /// Passive outbox wait.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        waiting_command: Option<OperationId>,
    },
    /// Fills one suspended workflow's wake-input slot.
    WorkflowResumed {
        /// Suspended operation.
        operation_id: OperationId,
        /// Exact suspension revision.
        workflow_revision: u64,
        /// Owner-verified immutable input.
        input: FileRef,
    },
    /// Resumes reconciliation without admitting another worker.
    ReconciliationResumed {
        /// Uncertain operation.
        operation_id: OperationId,
        /// Retained lease fence.
        fence: LeaseFence,
    },
    /// Releases a dead worker's lease for recovery.
    LeaseReleased {
        /// Operation returned to admission.
        operation_id: OperationId,
        /// Released lease fence.
        fence: LeaseFence,
    },
    /// Requests cancellation without acknowledging completion.
    CancellationRequested {
        /// Target operation.
        operation_id: OperationId,
        /// Propagate through the structured subtree.
        recursive: bool,
    },
    /// Records a terminal or uncertain observation.
    Completed {
        /// Observed operation.
        operation_id: OperationId,
        /// Result or uncertain state.
        outcome: Outcome<FileRef>,
        /// Required while execution capacity remains owned.
        fence: Option<LeaseFence>,
        /// Measured monotonic execution duration.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        execution_duration_ns: Option<u64>,
    },
    /// Commits a structured-concurrency decision atomically.
    Orchestrated {
        /// Parent operation.
        operation_id: OperationId,
        /// Observed parent revision.
        expected_revision: u64,
        /// Parent outcome.
        outcome: Outcome<FileRef>,
        /// Nonterminal losers to cancel.
        cancel: Vec<OperationId>,
        /// Exact reducer contract when reducing.
        reducer: Option<EntrypointRef>,
        /// Digest of the ordered materialized inputs.
        reduction_digest: Option<[u8; 32]>,
    },
}

impl SchedulerEvent {
    pub(crate) fn operation_id(&self) -> OperationId {
        match self {
            Self::Declared { spec } => spec.operation_id,
            Self::SessionConfigured { operation_id, .. }
            | Self::ModelDispatchClaimed { operation_id, .. }
            | Self::WaitingForCapacity { operation_id }
            | Self::Admitted { operation_id, .. }
            | Self::PartiallyAdmitted { operation_id, .. }
            | Self::Rejected { operation_id, .. }
            | Self::Started { operation_id, .. }
            | Self::Checkpointed { operation_id, .. }
            | Self::WaitingForChildren { operation_id, .. }
            | Self::WorkflowSuspended { operation_id, .. }
            | Self::WorkflowResumed { operation_id, .. }
            | Self::ReconciliationResumed { operation_id, .. }
            | Self::LeaseReleased { operation_id, .. }
            | Self::CancellationRequested { operation_id, .. }
            | Self::Completed { operation_id, .. }
            | Self::Orchestrated { operation_id, .. } => *operation_id,
        }
    }
}

impl SchedulerEvent {
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        match self {
            Self::SessionConfigured { .. }
            | Self::WaitingForCapacity { .. }
            | Self::CancellationRequested { .. } => Ok(0),
            Self::Declared { spec } => add(slots::<OperationSpec>(1)?, spec.logical_owned_bytes()?),
            Self::Admitted { reservation, .. } | Self::PartiallyAdmitted { reservation, .. } => {
                reservation.logical_owned_bytes()
            }
            Self::Rejected { reason, .. } => Ok(reason.capacity() as u64),
            Self::ModelDispatchClaimed { fence, .. }
            | Self::Started { fence, .. }
            | Self::WaitingForChildren { fence, .. }
            | Self::WorkflowSuspended { fence, .. }
            | Self::ReconciliationResumed { fence, .. }
            | Self::LeaseReleased { fence, .. } => fence.logical_owned_bytes(),
            Self::Checkpointed {
                checkpoint, fence, ..
            } => add(
                checkpoint.logical_owned_bytes()?,
                fence.logical_owned_bytes()?,
            ),
            Self::WorkflowResumed { input, .. } => input.logical_owned_bytes(),
            Self::Completed { outcome, fence, .. } => add(
                outcome.logical_owned_bytes()?,
                fence
                    .as_ref()
                    .map(LeaseFence::logical_owned_bytes)
                    .transpose()?
                    .unwrap_or(0),
            ),
            Self::Orchestrated {
                outcome,
                cancel,
                reducer,
                ..
            } => [
                outcome.logical_owned_bytes()?,
                slots::<OperationId>(cancel.capacity())?,
                reducer
                    .as_ref()
                    .map(EntrypointRef::logical_owned_bytes)
                    .transpose()?
                    .unwrap_or(0),
            ]
            .into_iter()
            .try_fold(0, add),
        }
    }
    pub(crate) fn retained_clone_owned_bytes(&self) -> Result<u64> {
        match self {
            Self::Declared { spec } => spec.logical_owned_bytes(),
            Self::Admitted { reservation, .. } | Self::PartiallyAdmitted { reservation, .. } => {
                reservation.logical_owned_bytes()
            }
            Self::Rejected { reason, .. } => Ok(reason.capacity() as u64),
            Self::Checkpointed { checkpoint, .. } => checkpoint.logical_owned_bytes(),
            Self::WorkflowResumed { input, .. } => input.logical_owned_bytes(),
            Self::Completed { outcome, .. } | Self::Orchestrated { outcome, .. } => {
                outcome.logical_owned_bytes()
            }
            _ => Ok(0),
        }
    }
    fn copy_fields(&self, quota: &mut CodecQuota<'_, '_>) -> Result<Self> {
        observe(quota.work())?;
        Ok(match self {
            Self::SessionConfigured { .. }
            | Self::WaitingForCapacity { .. }
            | Self::CancellationRequested { .. } => self.clone(),
            Self::Declared { spec } => Self::Declared {
                spec: crate::core::copy_boxed(spec.as_ref(), quota, OperationSpec::copy_fields)?,
            },
            Self::Admitted {
                operation_id,
                reservation,
            } => Self::Admitted {
                operation_id: *operation_id,
                reservation: reservation.copy_fields(quota)?,
            },
            Self::PartiallyAdmitted {
                operation_id,
                reservation,
            } => Self::PartiallyAdmitted {
                operation_id: *operation_id,
                reservation: reservation.copy_fields(quota)?,
            },
            Self::Rejected {
                operation_id,
                reason,
            } => Self::Rejected {
                operation_id: *operation_id,
                reason: copy_text(reason, quota)?,
            },
            Self::ModelDispatchClaimed {
                operation_id,
                attempt_id,
                step,
                request_digest,
                fence,
                ceiling,
            } => Self::ModelDispatchClaimed {
                operation_id: *operation_id,
                attempt_id: *attempt_id,
                step: *step,
                request_digest: *request_digest,
                fence: fence.copy_fields(quota)?,
                ceiling: *ceiling,
            },
            Self::Started {
                operation_id,
                fence,
            } => Self::Started {
                operation_id: *operation_id,
                fence: fence.copy_fields(quota)?,
            },
            Self::Checkpointed {
                operation_id,
                checkpoint,
                fence,
            } => Self::Checkpointed {
                operation_id: *operation_id,
                checkpoint: copy_checkpoint(checkpoint, quota)?,
                fence: fence.copy_fields(quota)?,
            },
            Self::WaitingForChildren {
                operation_id,
                fence,
            } => Self::WaitingForChildren {
                operation_id: *operation_id,
                fence: fence.copy_fields(quota)?,
            },
            Self::WorkflowSuspended {
                operation_id,
                fence,
                workflow_revision,
                waiting_command,
            } => Self::WorkflowSuspended {
                operation_id: *operation_id,
                fence: fence.copy_fields(quota)?,
                workflow_revision: *workflow_revision,
                waiting_command: *waiting_command,
            },
            Self::WorkflowResumed {
                operation_id,
                workflow_revision,
                input,
            } => Self::WorkflowResumed {
                operation_id: *operation_id,
                workflow_revision: *workflow_revision,
                input: crate::core::copy_file(input, quota)?,
            },
            Self::ReconciliationResumed {
                operation_id,
                fence,
            } => Self::ReconciliationResumed {
                operation_id: *operation_id,
                fence: fence.copy_fields(quota)?,
            },
            Self::LeaseReleased {
                operation_id,
                fence,
            } => Self::LeaseReleased {
                operation_id: *operation_id,
                fence: fence.copy_fields(quota)?,
            },
            Self::Completed {
                operation_id,
                outcome,
                fence,
                execution_duration_ns,
            } => Self::Completed {
                operation_id: *operation_id,
                outcome: outcome.copy_fields(quota)?,
                fence: fence
                    .as_ref()
                    .map(|fence| fence.copy_fields(quota))
                    .transpose()?,
                execution_duration_ns: *execution_duration_ns,
            },
            Self::Orchestrated {
                operation_id,
                expected_revision,
                outcome,
                cancel,
                reducer,
                reduction_digest,
            } => {
                let outcome = outcome.copy_fields(quota)?;
                for _ in cancel {
                    observe(quota.work())?;
                }
                let cancel = crate::core::copy_copy_vector(cancel, quota)?;
                let reducer = reducer
                    .as_ref()
                    .map(|value| value.copy_fields(quota))
                    .transpose()?;
                Self::Orchestrated {
                    operation_id: *operation_id,
                    expected_revision: *expected_revision,
                    outcome,
                    cancel,
                    reducer,
                    reduction_digest: *reduction_digest,
                }
            }
        })
    }
}
closed_clone!(SchedulerEvent);
