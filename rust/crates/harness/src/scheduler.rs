//! Deterministic durable scheduling and structured orchestration semantics.

use crate::contract::{COMPONENT_LABEL_MAX_BYTES, next_revision};
use crate::{
    Error, OperationId, Outcome, Result, TaskId, conversation::FileRef, core::Authority,
    resources::CheckpointRef,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// Versioned resumable entrypoint for durable work.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EntrypointRef {
    /// Stable namespaced entrypoint name.
    pub name: String,
    /// Semantic implementation version.
    pub version: String,
    /// Digest of the state/input schema and implementation contract.
    pub digest: [u8; 32],
    /// JSON Schema for the durable result value.
    pub result_schema: Value,
}

/// Logical resource quantities; providers decide how they map to capacity.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResourceRequest(pub BTreeMap<String, u64>);

impl ResourceRequest {
    /// Returns whether this request fits within a capacity snapshot.
    #[must_use]
    pub fn fits(&self, capacity: &ResourceSnapshot) -> bool {
        covers(&capacity.0, self)
    }
}

/// Whether `held` holds at least every quantity in `request`.
fn covers(held: &BTreeMap<String, u64>, request: &ResourceRequest) -> bool {
    request
        .0
        .iter()
        .all(|(resource, requested)| held.get(resource).unwrap_or(&0) >= requested)
}

/// Available logical resources advertised to admission policy.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResourceSnapshot(pub BTreeMap<String, u64>);

/// Explicit lifetime owner for durable work.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DurableOwner {
    /// Child remains structurally owned by this authority.
    Attached {
        /// Owning durable aggregate.
        authority: Authority,
    },
    /// Work survives its initiating call/session and has a durable owner.
    Detached {
        /// Durable owner that outlives the initiating call.
        authority: Authority,
    },
}

impl DurableOwner {
    pub(crate) fn authority(&self) -> &Authority {
        match self {
            Self::Attached { authority } | Self::Detached { authority } => authority,
        }
    }
}

/// Stable parent link and child slot.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ParentLink {
    /// Parent operation.
    pub operation_id: OperationId,
    /// Stable logical slot, independent of observation order.
    pub slot: String,
}

/// Durable orchestration behavior represented as data.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Orchestration {
    /// Executor produces the result directly.
    Leaf,
    /// All declared children must complete.
    Join,
    /// First successful child wins.
    Race,
    /// At least `required` children must succeed.
    Quorum {
        /// Number of successful children required.
        required: u32,
    },
    /// A versioned reducer entrypoint consumes ordered child values.
    Reduce {
        /// Versioned reducer entrypoint.
        reducer: EntrypointRef,
    },
}

/// Immutable durable operation declaration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OperationSpec {
    /// Stable operation identity.
    pub operation_id: OperationId,
    /// Optional structured parent.
    pub parent: Option<ParentLink>,
    /// Explicit durable owner.
    pub owner: DurableOwner,
    /// Restartable implementation identity.
    pub entrypoint: EntrypointRef,
    /// Dependencies that must succeed before admission.
    pub dependencies: BTreeSet<OperationId>,
    /// Logical capacity requirements.
    pub resources: ResourceRequest,
    /// Small provider-neutral placement labels, not process or file content.
    pub placement: BTreeMap<String, String>,
    /// Orchestration behavior.
    pub orchestration: Orchestration,
    /// Immutable, provider-owned initial state bytes.
    pub state: FileRef,
}

/// Durable lifecycle phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationPhase {
    /// Declared but dependencies have not resolved.
    WaitingForDependencies,
    /// Ready but capacity has not been admitted.
    WaitingForCapacity,
    /// Capacity and placement are durably pinned.
    Admitted,
    /// Executor owns the operation.
    Running,
    /// Parent is suspended and consumes no execution reservation.
    WaitingForChildren,
    /// External completion requires reconciliation.
    Reconciling,
    /// Terminal result is known.
    Terminal,
}

/// Pinned resource allocation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Reservation {
    /// Provider-owned stable lease identity.
    pub id: String,
    /// Selected worker/placement identity.
    pub placement: String,
    /// Resources actually admitted; partial admission remains observable.
    pub admitted: ResourceRequest,
}

/// Exact fencing token copied from the durable reservation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LeaseFence {
    /// Provider-owned lease identity.
    pub reservation_id: String,
    /// Worker/placement that owns the lease.
    pub placement: String,
}

impl From<&Reservation> for LeaseFence {
    fn from(value: &Reservation) -> Self {
        Self {
            reservation_id: value.id.clone(),
            placement: value.placement.clone(),
        }
    }
}

/// Current projection for one operation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OperationState {
    /// Immutable declaration.
    pub spec: OperationSpec,
    /// Current lifecycle phase.
    pub phase: OperationPhase,
    /// Active execution reservation, absent while a parent waits.
    pub reservation: Option<Reservation>,
    /// Latest durable resumable checkpoint.
    pub checkpoint: Option<CheckpointRef>,
    /// Terminal result when known.
    pub outcome: Option<Outcome<FileRef>>,
    /// Whether cancellation was requested but not yet observed.
    pub cancellation_requested: bool,
    /// Per-operation fencing revision advanced by every committed mutation.
    pub revision: u64,
}

/// One deterministic scheduler transition.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SchedulerEvent {
    /// A new operation was declared.
    Declared {
        /// Immutable operation declaration.
        spec: Box<OperationSpec>,
    },
    /// An operation became dependency-ready.
    WaitingForCapacity {
        /// Operation becoming capacity-ready.
        operation_id: OperationId,
    },
    /// Capacity was partially or fully admitted.
    Admitted {
        /// Admitted operation.
        operation_id: OperationId,
        /// Pinned allocation.
        reservation: Reservation,
    },
    /// Some requested capacity was reserved, but execution cannot start yet.
    PartiallyAdmitted {
        /// Partially admitted operation.
        operation_id: OperationId,
        /// Observable partial allocation.
        reservation: Reservation,
    },
    /// Work was rejected before execution, including an impossible dependency.
    Rejected {
        /// Rejected operation.
        operation_id: OperationId,
        /// Stable rejection reason.
        reason: String,
    },
    /// Execution started.
    Started {
        /// Started operation.
        operation_id: OperationId,
        /// Active reservation fence.
        fence: LeaseFence,
    },
    /// Execution checkpoint advanced.
    Checkpointed {
        /// Checkpointed operation.
        operation_id: OperationId,
        /// New immutable checkpoint.
        checkpoint: CheckpointRef,
        /// Active reservation fence.
        fence: LeaseFence,
    },
    /// Parent suspended for children and released execution capacity.
    WaitingForChildren {
        /// Suspended parent operation.
        operation_id: OperationId,
        /// Active reservation fence being released.
        fence: LeaseFence,
    },
    /// A dead worker's fenced lease was released for recovery.
    LeaseReleased {
        /// Operation returned to capacity admission.
        operation_id: OperationId,
        /// Exact lease being released.
        fence: LeaseFence,
    },
    /// Cancellation was requested; this is not a cancellation acknowledgement.
    CancellationRequested {
        /// Operation whose cancellation should propagate.
        operation_id: OperationId,
        /// Whether cancellation propagates through the complete structured subtree.
        recursive: bool,
    },
    /// A terminal or indeterminate observation was recorded.
    Completed {
        /// Observed operation.
        operation_id: OperationId,
        /// Terminal or uncertain outcome.
        outcome: Outcome<FileRef>,
        /// Required for worker-owned completion; absent for reconciliation/cancellation.
        fence: Option<LeaseFence>,
        /// Measured monotonic worker execution duration. Absent from historical events and
        /// non-worker reconciliation; never inferred from coordinator commit timestamps.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        execution_duration_ns: Option<u64>,
    },
    /// Atomically commits a structured-concurrency decision.
    Orchestrated {
        /// Parent operation.
        operation_id: OperationId,
        /// Parent revision observed while computing the decision.
        expected_revision: u64,
        /// Parent terminal outcome.
        outcome: Outcome<FileRef>,
        /// Nonterminal losers to cancel.
        cancel: Vec<OperationId>,
        /// Exact reducer contract for reduce decisions only.
        reducer: Option<EntrypointRef>,
        /// Digest of exact ordered inputs for materialized joins, quorums, or reduction.
        reduction_digest: Option<[u8; 32]>,
    },
}

/// Pure reducer for dependency, capacity, ownership, and cancellation state.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Scheduler {
    operations: BTreeMap<OperationId, OperationState>,
    child_slots: BTreeMap<(OperationId, String), OperationId>,
    completion_order: Vec<OperationId>,
}

impl Scheduler {
    /// Creates an empty scheduler projection.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            operations: BTreeMap::new(),
            child_slots: BTreeMap::new(),
            completion_order: Vec::new(),
        }
    }

    /// Returns one operation projection.
    #[must_use]
    pub fn operation(&self, id: OperationId) -> Option<&OperationState> {
        self.operations.get(&id)
    }

    /// Plans a declaration, including stable child-slot and cycle checks.
    pub fn declare(&self, spec: OperationSpec) -> Result<SchedulerEvent> {
        if spec.entrypoint.name.trim().is_empty() || spec.entrypoint.version.trim().is_empty() {
            return Err(Error::Invalid(
                "durable entrypoint name and version are required".into(),
            ));
        }
        spec.state.validate()?;
        if spec.placement.len() > 32
            || spec.placement.iter().any(|(key, value)| {
                key.is_empty()
                    || key.len() > COMPONENT_LABEL_MAX_BYTES
                    || value.len() > COMPONENT_LABEL_MAX_BYTES
                    || key.chars().any(char::is_control)
                    || value.chars().any(char::is_control)
            })
        {
            return Err(Error::Invalid(
                "placement labels exceed scheduler limits".into(),
            ));
        }
        crate::contract::compile_json_schema(&spec.entrypoint.result_schema, "entrypoint result")?;
        if self.operations.contains_key(&spec.operation_id) {
            return Err(Error::Conflict("operation identity already exists".into()));
        }
        if spec.dependencies.contains(&spec.operation_id)
            || spec
                .dependencies
                .iter()
                .any(|dependency| self.depends_on(*dependency, spec.operation_id))
        {
            return Err(Error::Conflict("operation dependency cycle".into()));
        }
        if let Some(parent) = &spec.parent {
            if parent.slot.trim().is_empty()
                || parent.slot.len() > COMPONENT_LABEL_MAX_BYTES
                || parent.slot.chars().any(char::is_control)
            {
                return Err(Error::Invalid("structured child slot is invalid".into()));
            }
            let Some(parent_operation) = self.operations.get(&parent.operation_id) else {
                return Err(Error::Invalid(
                    "structured child requires an existing parent and slot".into(),
                ));
            };
            if self
                .child_slots
                .contains_key(&(parent.operation_id, parent.slot.clone()))
            {
                return Err(Error::Conflict("parent child slot is already bound".into()));
            }
            if parent_operation.phase == OperationPhase::Terminal {
                return Err(Error::Conflict(
                    "terminal parent cannot accept children".into(),
                ));
            }
        }
        Ok(SchedulerEvent::Declared {
            spec: Box::new(spec),
        })
    }

    /// Returns unplaced dependency-ready operations whose requests fit capacity.
    /// Use `ready_for` when the worker advertises placement labels.
    #[must_use]
    pub fn ready(&self, capacity: &ResourceSnapshot) -> Vec<OperationId> {
        self.ready_for(capacity, &BTreeMap::new())
    }

    /// Returns ready operations whose required placement labels match a worker.
    #[must_use]
    pub fn ready_for(
        &self,
        capacity: &ResourceSnapshot,
        labels: &BTreeMap<String, String>,
    ) -> Vec<OperationId> {
        self.operations
            .values()
            .filter(|operation| {
                matches!(
                    operation.phase,
                    OperationPhase::WaitingForDependencies | OperationPhase::WaitingForCapacity
                ) && self.dependencies_succeeded(operation)
                    && !operation.cancellation_requested
                    && operation
                        .spec
                        .placement
                        .iter()
                        .all(|(key, value)| labels.get(key) == Some(value))
                    && remaining_resources(operation).fits(capacity)
            })
            .map(|operation| operation.spec.operation_id)
            .collect()
    }

    /// Subtracts active reservations for one placement from advertised capacity.
    #[must_use]
    pub fn available_for(
        &self,
        placement: &str,
        advertised: &ResourceSnapshot,
    ) -> ResourceSnapshot {
        let mut available = advertised.0.clone();
        for reservation in self
            .operations
            .values()
            .filter_map(|operation| operation.reservation.as_ref())
            .filter(|reservation| reservation.placement == placement)
        {
            for (resource, quantity) in &reservation.admitted.0 {
                let remaining = available.entry(resource.clone()).or_default();
                *remaining = remaining.saturating_sub(*quantity);
            }
        }
        ResourceSnapshot(available)
    }

    /// Applies one committed scheduler event.
    #[allow(
        clippy::cognitive_complexity,
        reason = "one arm per scheduler event variant; splitting would obscure the dispatch, \
                  not simplify it"
    )]
    #[allow(
        clippy::too_many_lines,
        reason = "same one-arm-per-scheduler-event-variant dispatch as above; splitting per-arm \
                  would scatter one event's application across many functions without \
                  clarifying any of them"
    )]
    pub fn apply(&mut self, event: SchedulerEvent) -> Result<()> {
        let primary = event.operation_id();
        let declared_parent = match &event {
            SchedulerEvent::Declared { spec } => {
                spec.parent.as_ref().map(|value| value.operation_id)
            }
            _ => None,
        };
        // Check every revision this event advances before mutating anything,
        // so a rejected event leaves the projection unchanged.
        let mut advanced = vec![primary];
        advanced.extend(declared_parent);
        match &event {
            SchedulerEvent::CancellationRequested {
                operation_id,
                recursive,
            } => advanced.extend(self.cancellation_frontier(*operation_id, *recursive)),
            SchedulerEvent::Orchestrated { cancel, .. } => advanced.extend(cancel),
            _ => {}
        }
        for operation in advanced.iter().filter_map(|id| self.operations.get(id)) {
            next_revision(operation.revision)?;
        }
        match event {
            SchedulerEvent::Declared { spec } => {
                let spec = *spec;
                let event = self.declare(spec.clone())?;
                debug_assert!(matches!(event, SchedulerEvent::Declared { .. }));
                if let Some(parent) = &spec.parent {
                    self.child_slots.insert(
                        (parent.operation_id, parent.slot.clone()),
                        spec.operation_id,
                    );
                }
                self.operations.insert(
                    spec.operation_id,
                    OperationState {
                        spec,
                        phase: OperationPhase::WaitingForDependencies,
                        reservation: None,
                        checkpoint: None,
                        outcome: None,
                        cancellation_requested: false,
                        revision: 0,
                    },
                );
            }
            SchedulerEvent::WaitingForCapacity { operation_id } => {
                self.require_dependencies_ready(operation_id)?;
                let operation = self.mutable(operation_id)?;
                require_phase(operation, OperationPhase::WaitingForDependencies)?;
                operation.phase = OperationPhase::WaitingForCapacity;
            }
            SchedulerEvent::Admitted {
                operation_id,
                reservation,
            } => {
                self.require_dependencies_ready(operation_id)?;
                let operation = self.mutable(operation_id)?;
                if !matches!(
                    operation.phase,
                    OperationPhase::WaitingForDependencies | OperationPhase::WaitingForCapacity
                ) {
                    return Err(Error::Conflict(
                        "operation cannot be admitted in its current phase".into(),
                    ));
                }
                if operation.cancellation_requested {
                    return Err(Error::Conflict(
                        "cancelled operation cannot be admitted".into(),
                    ));
                }
                if !covers(&reservation.admitted.0, &operation.spec.resources) {
                    return Err(Error::Conflict(
                        "partial reservation cannot start an operation".into(),
                    ));
                }
                if operation
                    .reservation
                    .as_ref()
                    .is_some_and(|partial| LeaseFence::from(partial) != (&reservation).into())
                {
                    return Err(Error::Conflict(
                        "partial admission must be completed by the same reservation".into(),
                    ));
                }
                operation.reservation = Some(reservation);
                operation.phase = OperationPhase::Admitted;
            }
            SchedulerEvent::PartiallyAdmitted {
                operation_id,
                reservation,
            } => {
                self.require_dependencies_ready(operation_id)?;
                let operation = self.mutable(operation_id)?;
                if !matches!(
                    operation.phase,
                    OperationPhase::WaitingForDependencies | OperationPhase::WaitingForCapacity
                ) || operation.cancellation_requested
                {
                    return Err(Error::Conflict(
                        "operation cannot be partially admitted in its current phase".into(),
                    ));
                }
                if !covers(&operation.spec.resources.0, &reservation.admitted)
                    || covers(&reservation.admitted.0, &operation.spec.resources)
                {
                    return Err(Error::Invalid(
                        "partial admission must be non-excessive and incomplete".into(),
                    ));
                }
                if let Some(existing) = &operation.reservation
                    && (LeaseFence::from(existing) != (&reservation).into()
                        || !covers(&reservation.admitted.0, &existing.admitted))
                {
                    return Err(Error::Conflict(
                        "partial reservation must advance the same lease".into(),
                    ));
                }
                operation.reservation = Some(reservation);
                operation.phase = OperationPhase::WaitingForCapacity;
            }
            SchedulerEvent::Rejected {
                operation_id,
                reason,
            } => {
                if reason.trim().is_empty() {
                    return Err(Error::Invalid("rejection reason is empty".into()));
                }
                let operation = self.mutable(operation_id)?;
                if !matches!(
                    operation.phase,
                    OperationPhase::WaitingForDependencies
                        | OperationPhase::WaitingForCapacity
                        | OperationPhase::Admitted
                ) {
                    return Err(Error::Conflict(
                        "operation cannot be rejected after execution starts".into(),
                    ));
                }
                operation.reservation = None;
                operation.phase = OperationPhase::Terminal;
                operation.outcome = Some(Outcome::Failed { message: reason });
                self.completion_order.push(operation_id);
            }
            SchedulerEvent::Started {
                operation_id,
                fence,
            } => {
                let operation = self.mutable(operation_id)?;
                require_phase(operation, OperationPhase::Admitted)?;
                require_fence(operation, &fence)?;
                if operation.cancellation_requested {
                    return Err(Error::Conflict("cancelled operation cannot start".into()));
                }
                operation.phase = OperationPhase::Running;
            }
            SchedulerEvent::Checkpointed {
                operation_id,
                checkpoint,
                fence,
            } => {
                let operation = self.mutable(operation_id)?;
                if !matches!(operation.phase, OperationPhase::Running) {
                    return Err(Error::Conflict(
                        "operation cannot checkpoint in its current phase".into(),
                    ));
                }
                require_fence(operation, &fence)?;
                operation.checkpoint = Some(checkpoint);
            }
            SchedulerEvent::WaitingForChildren {
                operation_id,
                fence,
            } => {
                let operation = self.mutable(operation_id)?;
                require_phase(operation, OperationPhase::Running)?;
                require_fence(operation, &fence)?;
                operation.reservation = None;
                // A waiting parent holds no work, so a pending cancellation
                // closes it, as it would on lease release.
                if operation.cancellation_requested {
                    operation.phase = OperationPhase::Terminal;
                    operation.outcome = Some(Outcome::Cancelled);
                    self.completion_order.push(operation_id);
                } else {
                    operation.phase = OperationPhase::WaitingForChildren;
                }
            }
            SchedulerEvent::LeaseReleased {
                operation_id,
                fence,
            } => {
                let operation = self.mutable(operation_id)?;
                if !matches!(
                    operation.phase,
                    OperationPhase::Admitted | OperationPhase::Running
                ) {
                    return Err(Error::Conflict(
                        "only an active lease can be released".into(),
                    ));
                }
                require_fence(operation, &fence)?;
                operation.reservation = None;
                if operation.cancellation_requested {
                    operation.phase = OperationPhase::Terminal;
                    operation.outcome = Some(Outcome::Cancelled);
                    self.completion_order.push(operation_id);
                } else {
                    operation.phase = OperationPhase::WaitingForCapacity;
                }
            }
            SchedulerEvent::CancellationRequested {
                operation_id,
                recursive,
            } => {
                let frontier = self.cancellation_frontier(operation_id, recursive);
                if !self.operations.contains_key(&operation_id) {
                    return Err(Error::NotFound(format!("operation {operation_id}")));
                }
                for target in frontier {
                    self.cancel(target)?;
                }
                return Ok(());
            }
            SchedulerEvent::Completed {
                operation_id,
                outcome,
                fence,
                execution_duration_ns,
            } => {
                if execution_duration_ns.is_some()
                    && (fence.is_none() || matches!(outcome, Outcome::Indeterminate { .. }))
                {
                    return Err(Error::Invalid(
                        "worker duration requires a fenced terminal completion".into(),
                    ));
                }
                if let Outcome::Succeeded(reference) = &outcome {
                    reference.validate()?;
                }
                let operation = self.mutable(operation_id)?;
                if operation.phase == OperationPhase::Terminal {
                    if operation.outcome.as_ref() == Some(&outcome)
                        && execution_duration_ns.is_none()
                    {
                        return Ok(());
                    }
                    return Err(Error::Conflict(
                        "terminal operation has another outcome".into(),
                    ));
                }
                if let Outcome::Indeterminate {
                    operation_id: uncertain,
                } = &outcome
                    && *uncertain != operation_id
                {
                    return Err(Error::Invalid(
                        "indeterminate outcome references another operation".into(),
                    ));
                }
                let allowed = matches!(
                    operation.phase,
                    OperationPhase::Running | OperationPhase::Reconciling
                ) || (operation.cancellation_requested
                    && matches!(&outcome, Outcome::Cancelled));
                if !allowed {
                    return Err(Error::Conflict(
                        "operation cannot complete in its current phase".into(),
                    ));
                }
                if matches!(operation.phase, OperationPhase::Running) {
                    require_fence(
                        operation,
                        fence.as_ref().ok_or_else(|| {
                            Error::Unauthorized("worker completion requires a lease fence".into())
                        })?,
                    )?;
                } else if fence.is_some() {
                    return Err(Error::Conflict("completion fence is not active".into()));
                }
                operation.reservation = None;
                operation.phase = match outcome {
                    Outcome::Indeterminate { .. } => OperationPhase::Reconciling,
                    _ => OperationPhase::Terminal,
                };
                operation.outcome = Some(outcome);
                if operation.phase == OperationPhase::Terminal
                    && !self.completion_order.contains(&operation_id)
                {
                    self.completion_order.push(operation_id);
                }
            }
            SchedulerEvent::Orchestrated {
                operation_id,
                expected_revision,
                outcome,
                cancel,
                reducer,
                reduction_digest,
            } => {
                if let Outcome::Succeeded(reference) = &outcome {
                    reference.validate()?;
                }
                let parent = self
                    .operations
                    .get(&operation_id)
                    .ok_or_else(|| Error::NotFound(format!("operation {operation_id}")))?;
                if parent.revision != expected_revision
                    || parent.phase != OperationPhase::WaitingForChildren
                    || parent.cancellation_requested
                {
                    return Err(Error::Conflict("stale orchestration decision".into()));
                }
                match self.orchestration(operation_id) {
                    OrchestrationDecision::Complete {
                        outcome: planned,
                        cancel: planned_cancel,
                    } if planned == outcome
                        && planned_cancel == cancel
                        && reducer.is_none()
                        && reduction_digest.is_none() => {}
                    OrchestrationDecision::Assemble {
                        assembly,
                        values,
                        cancel: planned_cancel,
                    } if matches!(outcome, Outcome::Succeeded(_))
                        && planned_cancel == cancel
                        && reducer.is_none()
                        && reduction_digest
                            == Some(assembly_invocation_digest(assembly, &values)?) => {}
                    OrchestrationDecision::Reduce {
                        reducer: planned,
                        values,
                    } if cancel.is_empty()
                        && reducer.as_ref() == Some(&planned)
                        && !matches!(outcome, Outcome::Indeterminate { .. })
                        && reduction_digest
                            == Some(reduction_invocation_digest(&planned, &values)?) => {}
                    _ => {
                        return Err(Error::Conflict(
                            "orchestration decision no longer matches".into(),
                        ));
                    }
                }
                for child_id in &cancel {
                    self.cancel(*child_id)?;
                }
                let parent = self.mutable(operation_id)?;
                parent.phase = match &outcome {
                    Outcome::Indeterminate { .. } => OperationPhase::Reconciling,
                    _ => OperationPhase::Terminal,
                };
                parent.outcome = Some(outcome);
                parent.reservation = None;
                if parent.phase == OperationPhase::Terminal {
                    self.completion_order.push(operation_id);
                }
            }
        }
        let operation = self.mutable(primary)?;
        operation.revision = next_revision(operation.revision)?;
        if let Some(parent) = declared_parent {
            let parent = self.mutable(parent)?;
            parent.revision = next_revision(parent.revision)?;
        }
        Ok(())
    }

    /// Returns structured children in stable slot order.
    pub fn children(&self, parent: OperationId) -> impl Iterator<Item = (&str, &OperationState)> {
        self.child_slots
            .range((parent, String::new())..)
            .take_while(move |((candidate, _), _)| *candidate == parent)
            .filter_map(|((_, slot), child)| {
                self.operations
                    .get(child)
                    .map(|state| (slot.as_str(), state))
            })
    }

    /// Computes the deterministic next decision for a structured parent.
    #[must_use]
    #[allow(
        clippy::too_many_lines,
        reason = "one match arm per Orchestration variant (Leaf, Join, Race, Quorum, Reduce), \
                  each independently evaluating its own completion rule; splitting per-arm \
                  would scatter one decision across many functions without clarifying any of \
                  them"
    )]
    pub fn orchestration(&self, parent: OperationId) -> OrchestrationDecision {
        let Some(parent_state) = self.operations.get(&parent) else {
            return OrchestrationDecision::Wait;
        };
        if parent_state.phase != OperationPhase::WaitingForChildren
            || parent_state.cancellation_requested
        {
            return OrchestrationDecision::Wait;
        }
        let children = self.children(parent).collect::<Vec<_>>();
        if children.is_empty() {
            return OrchestrationDecision::Wait;
        }
        match &parent_state.spec.orchestration {
            Orchestration::Leaf => OrchestrationDecision::Wait,
            Orchestration::Join => {
                if children
                    .iter()
                    .any(|(_, child)| child.phase != OperationPhase::Terminal)
                {
                    OrchestrationDecision::Wait
                } else {
                    complete_ordered(parent, &children)
                }
            }
            Orchestration::Race => {
                for completed in &self.completion_order {
                    if let Some((_, child)) = children
                        .iter()
                        .find(|(_, child)| child.spec.operation_id == *completed)
                        && let Some(Outcome::Succeeded(value)) = &child.outcome
                    {
                        return OrchestrationDecision::Complete {
                            outcome: Outcome::Succeeded(value.clone()),
                            cancel: children
                                .iter()
                                .filter_map(|(_, candidate)| {
                                    (candidate.phase != OperationPhase::Terminal)
                                        .then_some(candidate.spec.operation_id)
                                })
                                .collect(),
                        };
                    }
                }
                if children
                    .iter()
                    .all(|(_, child)| child.phase == OperationPhase::Terminal)
                {
                    OrchestrationDecision::Complete {
                        outcome: Outcome::Failed {
                            message: "every raced child failed".into(),
                        },
                        cancel: Vec::new(),
                    }
                } else {
                    OrchestrationDecision::Wait
                }
            }
            Orchestration::Quorum { required } => {
                let successes = self
                    .completion_order
                    .iter()
                    .filter_map(|completed| {
                        children.iter().find_map(|(_, child)| {
                            (child.spec.operation_id == *completed)
                                .then_some(child)
                                .and_then(|state| match &state.outcome {
                                    Some(Outcome::Succeeded(value)) => Some(value.clone()),
                                    _ => None,
                                })
                        })
                    })
                    .collect::<Vec<_>>();
                if successes.len() >= *required as usize {
                    return OrchestrationDecision::Assemble {
                        assembly: AssemblyKind::Quorum,
                        values: successes
                            .into_iter()
                            .take(*required as usize)
                            .enumerate()
                            .map(|(index, value)| (index.to_string(), value))
                            .collect(),
                        cancel: children
                            .iter()
                            .filter_map(|(_, child)| {
                                (child.phase != OperationPhase::Terminal)
                                    .then_some(child.spec.operation_id)
                            })
                            .collect(),
                    };
                }
                let possible = children
                    .iter()
                    .filter(|(_, child)| {
                        child.phase != OperationPhase::Terminal
                            || matches!(child.outcome, Some(Outcome::Succeeded(_)))
                    })
                    .count();
                if possible < *required as usize {
                    OrchestrationDecision::Complete {
                        outcome: Outcome::Failed {
                            message: "quorum is no longer reachable".into(),
                        },
                        cancel: Vec::new(),
                    }
                } else {
                    OrchestrationDecision::Wait
                }
            }
            Orchestration::Reduce { reducer } => {
                if children
                    .iter()
                    .any(|(_, child)| child.phase != OperationPhase::Terminal)
                {
                    return OrchestrationDecision::Wait;
                }
                let values = children
                    .iter()
                    .filter_map(|(slot, child)| match &child.outcome {
                        Some(Outcome::Succeeded(value)) => {
                            Some(((*slot).to_owned(), value.clone()))
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                if values.len() == children.len() {
                    OrchestrationDecision::Reduce {
                        reducer: reducer.clone(),
                        values,
                    }
                } else {
                    complete_ordered(parent, &children)
                }
            }
        }
    }

    /// Returns the stable propagation order for explicit cancellation commands.
    #[must_use]
    pub fn cancellation_frontier(
        &self,
        operation_id: OperationId,
        recursive: bool,
    ) -> Vec<OperationId> {
        let mut frontier = vec![operation_id];
        if recursive {
            let owner = self
                .operations
                .get(&operation_id)
                .map(|operation| operation.spec.owner.authority());
            let mut index = 0;
            while index < frontier.len() {
                let Some(&parent) = frontier.get(index) else {
                    break;
                };
                frontier.extend(
                    self.children(parent)
                        // A recursive command is authorized for the root owner. A
                        // differently owned child is an authority boundary, not a
                        // cancellation edge.
                        .filter(|(_, child)| owner == Some(child.spec.owner.authority()))
                        .map(|(_, child)| child.spec.operation_id)
                        .filter(|child| !frontier.contains(child))
                        .collect::<Vec<_>>(),
                );
                index += 1;
            }
        }
        frontier
    }

    /// Returns operations made impossible by a terminal non-success dependency.
    #[must_use]
    pub fn blocked_by_dependencies(&self) -> Vec<OperationId> {
        self.operations
            .values()
            .filter(|operation| {
                matches!(
                    operation.phase,
                    OperationPhase::WaitingForDependencies | OperationPhase::WaitingForCapacity
                ) && operation.spec.dependencies.iter().any(|dependency| {
                    self.operations.get(dependency).is_some_and(|state| {
                        state.phase == OperationPhase::Terminal
                            && !matches!(state.outcome, Some(Outcome::Succeeded(_)))
                    })
                })
            })
            .map(|operation| operation.spec.operation_id)
            .collect()
    }

    /// Closes an operation that holds no running work and asks a running one
    /// to stop. Like a duplicate completion, cancelling a terminal operation
    /// commits nothing to it, revision included.
    fn cancel(&mut self, id: OperationId) -> Result<()> {
        let operation = self.mutable(id)?;
        let close = match operation.phase {
            OperationPhase::Terminal => return Ok(()),
            OperationPhase::Running | OperationPhase::Reconciling => false,
            OperationPhase::WaitingForDependencies
            | OperationPhase::WaitingForCapacity
            | OperationPhase::Admitted
            | OperationPhase::WaitingForChildren => true,
        };
        operation.revision = next_revision(operation.revision)?;
        if close {
            operation.reservation = None;
            operation.phase = OperationPhase::Terminal;
            operation.outcome = Some(Outcome::Cancelled);
            self.completion_order.push(id);
        } else {
            operation.cancellation_requested = true;
        }
        Ok(())
    }

    fn mutable(&mut self, id: OperationId) -> Result<&mut OperationState> {
        self.operations
            .get_mut(&id)
            .ok_or_else(|| Error::NotFound(format!("operation {id}")))
    }

    fn require_dependencies_ready(&self, id: OperationId) -> Result<()> {
        let operation = self
            .operations
            .get(&id)
            .ok_or_else(|| Error::NotFound(format!("operation {id}")))?;
        if self.dependencies_succeeded(operation) {
            Ok(())
        } else {
            Err(Error::Conflict(
                "operation dependencies are not ready".into(),
            ))
        }
    }

    fn dependencies_succeeded(&self, operation: &OperationState) -> bool {
        operation.spec.dependencies.iter().all(|dependency| {
            matches!(
                self.operations
                    .get(dependency)
                    .and_then(|state| state.outcome.as_ref()),
                Some(Outcome::Succeeded(_))
            )
        })
    }

    fn depends_on(&self, candidate: OperationId, target: OperationId) -> bool {
        candidate == target
            || self.operations.get(&candidate).is_some_and(|operation| {
                operation
                    .spec
                    .dependencies
                    .iter()
                    .any(|dependency| self.depends_on(*dependency, target))
            })
    }
}

fn remaining_resources(operation: &OperationState) -> ResourceRequest {
    let mut remaining = operation.spec.resources.0.clone();
    if let Some(reservation) = &operation.reservation {
        for (resource, admitted) in &reservation.admitted.0 {
            let value = remaining.entry(resource.clone()).or_default();
            *value = value.saturating_sub(*admitted);
        }
    }
    ResourceRequest(remaining)
}

/// Deterministic output of join/race/quorum/reduce inspection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OrchestrationDecision {
    /// More child observations are required.
    Wait,
    /// Parent may commit this outcome and request loser cancellation.
    Complete {
        /// Derived parent outcome.
        outcome: Outcome<FileRef>,
        /// Nonterminal children to cancel.
        cancel: Vec<OperationId>,
    },
    /// Materialize an ordered aggregate before publishing its result reference.
    Assemble {
        /// Join preserves child slots; quorum emits values in completion order.
        assembly: AssemblyKind,
        /// Exact ordered child success references.
        values: Vec<(String, FileRef)>,
        /// Nonterminal quorum losers to cancel.
        cancel: Vec<OperationId>,
    },
    /// Invoke the pinned reducer with values in stable child-slot order.
    Reduce {
        /// Versioned reducer implementation.
        reducer: EntrypointRef,
        /// Ordered `(slot, value)` inputs.
        values: Vec<(String, FileRef)>,
    },
}

/// Deterministic aggregate encoding selected by an orchestration plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssemblyKind {
    /// Ordered child-slot objects with `slot` and resolved JSON `value`.
    Join,
    /// Ordered successful JSON values selected by completion order.
    Quorum,
}

fn complete_ordered(
    parent: OperationId,
    children: &[(&str, &OperationState)],
) -> OrchestrationDecision {
    let mut values = Vec::with_capacity(children.len());
    for (slot, child) in children {
        match &child.outcome {
            Some(Outcome::Succeeded(value)) => {
                values.push(((*slot).to_owned(), value.clone()));
            }
            Some(Outcome::Failed { message }) => {
                return OrchestrationDecision::Complete {
                    outcome: Outcome::Failed {
                        message: format!("child {slot} failed: {message}"),
                    },
                    cancel: Vec::new(),
                };
            }
            Some(Outcome::Cancelled) => {
                return OrchestrationDecision::Complete {
                    outcome: Outcome::Cancelled,
                    cancel: Vec::new(),
                };
            }
            Some(Outcome::Indeterminate { .. }) => {
                return OrchestrationDecision::Complete {
                    outcome: Outcome::Indeterminate {
                        operation_id: parent,
                    },
                    cancel: Vec::new(),
                };
            }
            None => return OrchestrationDecision::Wait,
        }
    }
    OrchestrationDecision::Assemble {
        assembly: AssemblyKind::Join,
        values,
        cancel: Vec::new(),
    }
}

fn require_phase(operation: &OperationState, expected: OperationPhase) -> Result<()> {
    if operation.phase == expected {
        Ok(())
    } else {
        Err(Error::Conflict(format!(
            "operation is {:?}, expected {expected:?}",
            operation.phase
        )))
    }
}

fn require_fence(operation: &OperationState, fence: &LeaseFence) -> Result<()> {
    let reservation = operation
        .reservation
        .as_ref()
        .ok_or_else(|| Error::Conflict("operation has no active lease".into()))?;
    if LeaseFence::from(reservation) != *fence {
        return Err(Error::Unauthorized("stale or foreign lease fence".into()));
    }
    Ok(())
}

/// Canonical identity of one exact versioned reducer invocation.
pub fn reduction_invocation_digest(
    reducer: &EntrypointRef,
    values: &[(String, FileRef)],
) -> Result<[u8; 32]> {
    crate::contract::canonical_json_digest(&(reducer, values))
}

/// Stable identity for the exact aggregate inputs and encoding rule.
pub fn assembly_invocation_digest(
    kind: AssemblyKind,
    values: &[(String, FileRef)],
) -> Result<[u8; 32]> {
    crate::contract::canonical_json_digest(&(kind, values))
}

impl SchedulerEvent {
    /// The operation this event advances.
    pub(crate) fn operation_id(&self) -> OperationId {
        match self {
            Self::Declared { spec } => spec.operation_id,
            Self::WaitingForCapacity { operation_id }
            | Self::Admitted { operation_id, .. }
            | Self::PartiallyAdmitted { operation_id, .. }
            | Self::Rejected { operation_id, .. }
            | Self::Started { operation_id, .. }
            | Self::Checkpointed { operation_id, .. }
            | Self::WaitingForChildren { operation_id, .. }
            | Self::LeaseReleased { operation_id, .. }
            | Self::CancellationRequested { operation_id, .. }
            | Self::Completed { operation_id, .. }
            | Self::Orchestrated { operation_id, .. } => *operation_id,
        }
    }
}

/// One ref-only durable inbox item with a gapless per-task sequence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InboxItem {
    /// Owning task.
    pub task_id: TaskId,
    /// Gapless one-based sequence.
    pub sequence: u64,
    /// Sender-defined idempotency identity.
    pub message_id: String,
    /// Immutable payload bytes staged before Stream publication.
    pub payload: FileRef,
}

/// In-memory projection of a durable task inbox; items themselves belong in Stream.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskInbox {
    task_id: TaskId,
    items: Vec<InboxItem>,
}

impl TaskInbox {
    /// Creates the inbox projection for one durable task.
    #[must_use]
    pub const fn new(task_id: TaskId) -> Self {
        Self {
            task_id,
            items: Vec::new(),
        }
    }

    /// Applies one committed item, deduplicating exact message identities.
    pub fn apply(&mut self, item: InboxItem) -> Result<()> {
        if item.task_id != self.task_id || item.message_id.trim().is_empty() {
            return Err(Error::Invalid(
                "inbox item has the wrong task or an empty message identity".into(),
            ));
        }
        item.payload.validate()?;
        if let Some(existing) = self
            .items
            .iter()
            .find(|existing| existing.message_id == item.message_id)
        {
            return if existing == &item {
                Ok(())
            } else {
                Err(Error::Conflict("inbox message identity reused".into()))
            };
        }
        let expected = self.items.len() as u64 + 1;
        if item.sequence != expected {
            return Err(Error::Conflict(format!(
                "expected inbox sequence {expected}"
            )));
        }
        self.items.push(item);
        Ok(())
    }

    /// Reads a bounded page after a sequence.
    #[must_use]
    pub fn after(&self, sequence: u64, limit: usize) -> Vec<InboxItem> {
        self.items
            .iter()
            .skip(usize::try_from(sequence).unwrap_or(usize::MAX))
            .take(limit)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef},
        core::AggregateKind,
        resources::ProviderRef,
    };

    fn id(value: u8) -> OperationId {
        OperationId::from_bytes([value; 16])
    }

    fn state_ref() -> Result<FileRef> {
        let volume = VolumeRef::new(
            ProviderRef::new("test", "filesystem", "2")?,
            "project",
            VolumeClass::Project,
            VolumeOwner::Project("project".into()),
        )?;
        FileRef::new(
            volume,
            "state/initial.json",
            "generation-1",
            FileDescriptor::from_bytes(b"null", "application/json")?,
            "initial.json",
        )
    }

    fn result_ref(bytes: &[u8]) -> Result<FileRef> {
        FileRef::new(
            state_ref()?.volume().clone(),
            "results/value.json",
            "generation-2",
            FileDescriptor::from_bytes(bytes, "application/json")?,
            "value.json",
        )
    }

    fn spec(operation_id: OperationId, orchestration: Orchestration) -> Result<OperationSpec> {
        Ok(OperationSpec {
            operation_id,
            parent: None,
            owner: DurableOwner::Detached {
                authority: Authority {
                    kind: AggregateKind::Task,
                    id: operation_id.to_string(),
                },
            },
            entrypoint: EntrypointRef {
                name: "example.task".into(),
                version: "1".into(),
                digest: [1; 32],
                result_schema: Value::Object(Default::default()),
            },
            dependencies: BTreeSet::new(),
            resources: ResourceRequest::default(),
            placement: BTreeMap::new(),
            orchestration,
            state: state_ref()?,
        })
    }

    #[test]
    fn exhausted_child_revision_rejects_cancellation_atomically() -> Result<()> {
        let mut scheduler = Scheduler::new();
        scheduler.apply(scheduler.declare(spec(id(1), Orchestration::Join)?)?)?;
        let mut child = spec(id(2), Orchestration::Leaf)?;
        child.owner = spec(id(1), Orchestration::Leaf)?.owner;
        child.parent = Some(ParentLink {
            operation_id: id(1),
            slot: "child".into(),
        });
        scheduler.apply(scheduler.declare(child)?)?;
        if let Some(child) = scheduler.operations.get_mut(&id(2)) {
            child.revision = u64::MAX;
        }
        let before = scheduler.clone();
        assert!(
            scheduler
                .apply(SchedulerEvent::CancellationRequested {
                    operation_id: id(1),
                    recursive: true,
                })
                .is_err()
        );
        assert_eq!(scheduler, before);
        Ok(())
    }

    /// One generated scheduler input over a small operation pool.
    #[derive(Clone, Copy, Debug)]
    enum Step {
        Declare {
            op: u8,
            parent: Option<u8>,
            dependency: Option<u8>,
            orchestration: u8,
            resources: u64,
        },
        WaitForCapacity(u8),
        Admit(u8, u8, u64),
        PartiallyAdmit(u8, u8, u64),
        Reject(u8),
        Start(u8, u8),
        Checkpoint(u8, u8),
        WaitForChildren(u8, u8),
        ReleaseLease(u8, u8),
        Cancel(u8, bool),
        Complete(u8, u8, Option<u8>),
        Orchestrate(u8),
        /// The next lifecycle event for the operation's current phase.
        Advance(u8, u8),
    }

    fn step() -> impl proptest::strategy::Strategy<Value = Step> {
        use proptest::prelude::*;
        let op = || 1..6_u8;
        let lease = || prop_oneof![9 => Just(0_u8), 1 => Just(1_u8)];
        let declare = (
            4..6_u8,
            proptest::option::of(op()),
            proptest::option::weighted(0.2, op()),
        );
        prop_oneof![
            1 => (declare, 0..3_u8, 0..2_u64).prop_map(
                |((op, parent, dependency), orchestration, resources)| Step::Declare {
                    op,
                    parent,
                    dependency,
                    orchestration,
                    resources,
                },
            ),
            2 => op().prop_map(Step::WaitForCapacity),
            4 => (op(), lease(), 0..3_u64)
                .prop_map(|(op, lease, amount)| Step::Admit(op, lease, amount)),
            1 => (op(), lease(), 0..3_u64)
                .prop_map(|(op, lease, amount)| Step::PartiallyAdmit(op, lease, amount)),
            1 => op().prop_map(Step::Reject),
            4 => (op(), lease()).prop_map(|(op, lease)| Step::Start(op, lease)),
            1 => (op(), lease()).prop_map(|(op, lease)| Step::Checkpoint(op, lease)),
            3 => (op(), lease()).prop_map(|(op, lease)| Step::WaitForChildren(op, lease)),
            1 => (op(), lease()).prop_map(|(op, lease)| Step::ReleaseLease(op, lease)),
            1 => (op(), any::<bool>()).prop_map(|(op, recursive)| Step::Cancel(op, recursive)),
            4 => (op(), 0..4_u8, proptest::option::weighted(0.8, lease()))
                .prop_map(|(op, outcome, fence)| Step::Complete(op, outcome, fence)),
            4 => op().prop_map(Step::Orchestrate),
            12 => (op(), any::<u8>()).prop_map(|(op, choice)| Step::Advance(op, choice)),
        ]
    }

    fn reservation(lease: u8, amount: u64) -> Reservation {
        Reservation {
            id: format!("lease-{lease}"),
            placement: "worker".into(),
            admitted: ResourceRequest(BTreeMap::from([("cpu".into(), amount)])),
        }
    }

    fn fence(lease: u8) -> LeaseFence {
        LeaseFence::from(&reservation(lease, 0))
    }

    fn event(scheduler: &Scheduler, step: Step) -> Result<Option<SchedulerEvent>> {
        Ok(Some(match step {
            Step::Declare {
                op,
                parent,
                dependency,
                orchestration,
                resources,
            } => {
                let orchestration = match orchestration {
                    0 => Orchestration::Leaf,
                    1 => Orchestration::Join,
                    _ => Orchestration::Race,
                };
                let mut declared = spec(id(op), orchestration)?;
                declared.dependencies.extend(dependency.map(id));
                declared.resources = ResourceRequest(BTreeMap::from([("cpu".into(), resources)]));
                if let Some(parent) = parent {
                    declared.owner = spec(id(parent), Orchestration::Leaf)?.owner;
                    declared.parent = Some(ParentLink {
                        operation_id: id(parent),
                        slot: format!("slot-{op}"),
                    });
                }
                SchedulerEvent::Declared {
                    spec: Box::new(declared),
                }
            }
            Step::WaitForCapacity(op) => SchedulerEvent::WaitingForCapacity {
                operation_id: id(op),
            },
            Step::Admit(op, lease, amount) => SchedulerEvent::Admitted {
                operation_id: id(op),
                reservation: reservation(lease, amount),
            },
            Step::PartiallyAdmit(op, lease, amount) => SchedulerEvent::PartiallyAdmitted {
                operation_id: id(op),
                reservation: reservation(lease, amount),
            },
            Step::Reject(op) => SchedulerEvent::Rejected {
                operation_id: id(op),
                reason: "rejected".into(),
            },
            Step::Start(op, lease) => SchedulerEvent::Started {
                operation_id: id(op),
                fence: fence(lease),
            },
            Step::Checkpoint(op, lease) => SchedulerEvent::Checkpointed {
                operation_id: id(op),
                checkpoint: crate::resources::CheckpointRef::new(
                    ProviderRef::new("example", "machines", "1")?,
                    vec![1],
                    None,
                )?,
                fence: fence(lease),
            },
            Step::WaitForChildren(op, lease) => SchedulerEvent::WaitingForChildren {
                operation_id: id(op),
                fence: fence(lease),
            },
            Step::ReleaseLease(op, lease) => SchedulerEvent::LeaseReleased {
                operation_id: id(op),
                fence: fence(lease),
            },
            Step::Cancel(op, recursive) => SchedulerEvent::CancellationRequested {
                operation_id: id(op),
                recursive,
            },
            Step::Complete(op, outcome, lease) => SchedulerEvent::Completed {
                operation_id: id(op),
                outcome: match outcome {
                    0 => Outcome::Succeeded(result_ref(b"2")?),
                    1 => Outcome::Failed {
                        message: "failed".into(),
                    },
                    2 => Outcome::Cancelled,
                    _ => Outcome::Indeterminate {
                        operation_id: id(op),
                    },
                },
                fence: lease.map(fence),
                execution_duration_ns: None,
            },
            Step::Advance(op, choice) => {
                let Some(state) = scheduler.operation(id(op)) else {
                    return Ok(None);
                };
                let lease = u8::from(
                    state
                        .reservation
                        .as_ref()
                        .is_some_and(|held| held.id == "lease-1"),
                );
                let next = match state.phase {
                    OperationPhase::WaitingForDependencies | OperationPhase::WaitingForCapacity => {
                        Step::Admit(op, lease, 1)
                    }
                    OperationPhase::Admitted => Step::Start(op, lease),
                    OperationPhase::Running if state.spec.orchestration != Orchestration::Leaf => {
                        Step::WaitForChildren(op, lease)
                    }
                    OperationPhase::Running => Step::Complete(op, choice % 4, Some(lease)),
                    OperationPhase::WaitingForChildren => Step::Orchestrate(op),
                    OperationPhase::Reconciling => Step::Complete(op, choice % 3, None),
                    OperationPhase::Terminal => return Ok(None),
                };
                return event(scheduler, next);
            }
            Step::Orchestrate(op) => {
                let Some(parent) = scheduler.operation(id(op)) else {
                    return Ok(None);
                };
                let (outcome, cancel, reduction_digest) = match scheduler.orchestration(id(op)) {
                    OrchestrationDecision::Complete { outcome, cancel } => (outcome, cancel, None),
                    OrchestrationDecision::Assemble {
                        assembly,
                        values,
                        cancel,
                    } => (
                        Outcome::Succeeded(result_ref(b"3")?),
                        cancel,
                        Some(assembly_invocation_digest(assembly, &values)?),
                    ),
                    OrchestrationDecision::Wait | OrchestrationDecision::Reduce { .. } => {
                        return Ok(None);
                    }
                };
                SchedulerEvent::Orchestrated {
                    operation_id: id(op),
                    expected_revision: parent.revision,
                    outcome,
                    cancel,
                    reducer: None,
                    reduction_digest,
                }
            }
        }))
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(128))]

        #[test]
        fn scheduler_apply_preserves_lifecycle_invariants(
            orchestration in 0..3_u8,
            steps in proptest::collection::vec(step(), 1..64),
        ) {
            use proptest::prelude::*;
            let fail = |error: Error| TestCaseError::fail(error.to_string());
            let mut scheduler = Scheduler::new();
            // A parent with two children gives every step something to act on.
            for (op, parent) in [(1, None), (2, Some(1)), (3, Some(1))] {
                let declared = Step::Declare {
                    op,
                    parent,
                    dependency: None,
                    orchestration: if parent.is_none() { orchestration } else { 0 },
                    resources: 1,
                };
                if let Some(event) = event(&scheduler, declared).map_err(fail)? {
                    scheduler.apply(event).map_err(fail)?;
                }
            }
            for step in steps {
                let Some(event) = event(&scheduler, step).map_err(fail)? else {
                    continue;
                };
                let before = scheduler.clone();
                if scheduler.apply(event).is_err() {
                    prop_assert_eq!(&scheduler, &before);
                    continue;
                }
                for (id, prior) in &before.operations {
                    let current = scheduler.operations.get(id);
                    prop_assert!(current.is_some_and(|current| current.revision >= prior.revision));
                    if prior.phase == OperationPhase::Terminal {
                        prop_assert_eq!(current, Some(prior), "terminal is absorbing");
                    }
                }
                for operation in scheduler.operations.values() {
                    // No worker holds such a parent to observe the request.
                    prop_assert!(
                        !operation.cancellation_requested
                            || operation.phase != OperationPhase::WaitingForChildren
                    );
                    prop_assert_eq!(
                        operation.outcome.is_some(),
                        matches!(
                            operation.phase,
                            OperationPhase::Terminal | OperationPhase::Reconciling
                        )
                    );
                    prop_assert!(operation.reservation.is_none() || matches!(
                        operation.phase,
                        OperationPhase::WaitingForCapacity
                            | OperationPhase::Admitted
                            | OperationPhase::Running
                    ));
                }
                let completed = scheduler.completion_order.iter().collect::<BTreeSet<_>>();
                prop_assert_eq!(completed.len(), scheduler.completion_order.len());
                let terminal = scheduler
                    .operations
                    .values()
                    .filter(|operation| operation.phase == OperationPhase::Terminal)
                    .map(|operation| &operation.spec.operation_id)
                    .collect::<BTreeSet<_>>();
                prop_assert_eq!(completed, terminal);
            }
        }
    }

    #[test]
    fn declarations_store_state_references_and_bound_placement_metadata() -> Result<()> {
        let scheduler = Scheduler::new();
        let event = scheduler.declare(spec(id(1), Orchestration::Leaf)?)?;
        let encoded =
            serde_json::to_value(&event).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(encoded["spec"]["state"]["path"], "state/initial.json");
        assert_eq!(encoded["spec"]["state"]["descriptor"]["byte_length"], 4);
        let mut oversized = spec(id(2), Orchestration::Leaf)?;
        oversized.placement.insert("region".into(), "x".repeat(256));
        assert!(matches!(
            scheduler.declare(oversized),
            Err(Error::Invalid(_))
        ));
        Ok(())
    }

    #[test]
    fn worker_duration_is_additive_and_fenced() -> Result<()> {
        let legacy = SchedulerEvent::Completed {
            operation_id: id(1),
            outcome: Outcome::Succeeded(result_ref(b"null")?),
            fence: Some(LeaseFence {
                reservation_id: "lease-1".into(),
                placement: "worker-1".into(),
            }),
            execution_duration_ns: None,
        };
        let legacy_json =
            serde_json::to_value(&legacy).map_err(|error| Error::Invalid(error.to_string()))?;
        assert!(legacy_json.get("execution_duration_ns").is_none());
        assert_eq!(
            serde_json::from_value::<SchedulerEvent>(legacy_json)
                .map_err(|error| Error::Invalid(error.to_string()))?,
            legacy
        );

        let mut measured = legacy;
        let SchedulerEvent::Completed {
            execution_duration_ns,
            ..
        } = &mut measured
        else {
            return Err(Error::Invalid("expected completion".into()));
        };
        *execution_duration_ns = Some(1_234_567_890);
        let measured_json =
            serde_json::to_value(&measured).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(measured_json["execution_duration_ns"], 1_234_567_890_u64);
        assert_eq!(
            serde_json::from_value::<SchedulerEvent>(measured_json)
                .map_err(|error| Error::Invalid(error.to_string()))?,
            measured
        );
        let mut scheduler = Scheduler::new();
        scheduler.apply(scheduler.declare(spec(id(1), Orchestration::Leaf)?)?)?;
        scheduler.apply(SchedulerEvent::Admitted {
            operation_id: id(1),
            reservation: Reservation {
                id: "lease-1".into(),
                placement: "worker-1".into(),
                admitted: ResourceRequest::default(),
            },
        })?;
        scheduler.apply(SchedulerEvent::Started {
            operation_id: id(1),
            fence: LeaseFence {
                reservation_id: "lease-1".into(),
                placement: "worker-1".into(),
            },
        })?;
        scheduler.apply(measured.clone())?;
        assert_eq!(
            scheduler.operation(id(1)).map(|operation| operation.phase),
            Some(OperationPhase::Terminal)
        );
        assert!(matches!(
            scheduler.apply(measured.clone()),
            Err(Error::Conflict(_))
        ));
        let mut unfenced = measured;
        let SchedulerEvent::Completed { fence, .. } = &mut unfenced else {
            return Err(Error::Invalid("expected completion".into()));
        };
        *fence = None;
        assert!(matches!(
            Scheduler::new().apply(unfenced),
            Err(Error::Invalid(_))
        ));
        Ok(())
    }

    #[test]
    fn waiting_parent_releases_execution_capacity() -> Result<()> {
        let mut scheduler = Scheduler::new();
        scheduler.apply(scheduler.declare(spec(id(1), Orchestration::Join)?)?)?;
        scheduler.apply(SchedulerEvent::Admitted {
            operation_id: id(1),
            reservation: Reservation {
                id: "lease-1".into(),
                placement: "worker-1".into(),
                admitted: ResourceRequest::default(),
            },
        })?;
        scheduler.apply(SchedulerEvent::Started {
            operation_id: id(1),
            fence: LeaseFence {
                reservation_id: "lease-1".into(),
                placement: "worker-1".into(),
            },
        })?;
        scheduler.apply(SchedulerEvent::WaitingForChildren {
            operation_id: id(1),
            fence: LeaseFence {
                reservation_id: "lease-1".into(),
                placement: "worker-1".into(),
            },
        })?;
        let parent = scheduler
            .operation(id(1))
            .ok_or_else(|| Error::NotFound("parent".into()))?;
        assert_eq!(parent.phase, OperationPhase::WaitingForChildren);
        assert!(parent.reservation.is_none());
        Ok(())
    }

    #[test]
    fn race_uses_first_observed_success_and_cancels_losers() -> Result<()> {
        let mut scheduler = Scheduler::new();
        scheduler.apply(scheduler.declare(spec(id(1), Orchestration::Race)?)?)?;
        scheduler.apply(SchedulerEvent::Admitted {
            operation_id: id(1),
            reservation: Reservation {
                id: "parent-lease".into(),
                placement: "parent-worker".into(),
                admitted: ResourceRequest::default(),
            },
        })?;
        let parent_fence = LeaseFence {
            reservation_id: "parent-lease".into(),
            placement: "parent-worker".into(),
        };
        scheduler.apply(SchedulerEvent::Started {
            operation_id: id(1),
            fence: parent_fence.clone(),
        })?;
        scheduler.apply(SchedulerEvent::WaitingForChildren {
            operation_id: id(1),
            fence: parent_fence,
        })?;
        for (child_id, slot) in [(id(2), "a"), (id(3), "b")] {
            let mut child = spec(child_id, Orchestration::Leaf)?;
            child.parent = Some(ParentLink {
                operation_id: id(1),
                slot: slot.into(),
            });
            scheduler.apply(scheduler.declare(child)?)?;
            scheduler.apply(SchedulerEvent::Admitted {
                operation_id: child_id,
                reservation: Reservation {
                    id: format!("lease-{slot}"),
                    placement: "worker".into(),
                    admitted: ResourceRequest::default(),
                },
            })?;
            scheduler.apply(SchedulerEvent::Started {
                operation_id: child_id,
                fence: LeaseFence {
                    reservation_id: format!("lease-{slot}"),
                    placement: "worker".into(),
                },
            })?;
        }
        scheduler.apply(SchedulerEvent::Completed {
            operation_id: id(3),
            outcome: Outcome::Succeeded(result_ref(b"\"winner\"")?),
            fence: Some(LeaseFence {
                reservation_id: "lease-b".into(),
                placement: "worker".into(),
            }),
            execution_duration_ns: None,
        })?;
        assert_eq!(
            scheduler.orchestration(id(1)),
            OrchestrationDecision::Complete {
                outcome: Outcome::Succeeded(result_ref(b"\"winner\"")?),
                cancel: vec![id(2)],
            }
        );
        let expected_revision = scheduler
            .operation(id(1))
            .ok_or_else(|| Error::NotFound("parent".into()))?
            .revision;
        scheduler.apply(SchedulerEvent::Orchestrated {
            operation_id: id(1),
            expected_revision,
            outcome: Outcome::Succeeded(result_ref(b"\"winner\"")?),
            cancel: vec![id(2)],
            reducer: None,
            reduction_digest: None,
        })?;
        assert_eq!(
            scheduler.operation(id(1)).map(|state| state.phase),
            Some(OperationPhase::Terminal)
        );
        assert_eq!(
            scheduler
                .operation(id(2))
                .and_then(|state| state.outcome.as_ref()),
            None
        );
        assert!(
            scheduler
                .operation(id(2))
                .is_some_and(|state| state.cancellation_requested)
        );
        Ok(())
    }

    #[test]
    fn task_inbox_is_gapless_and_idempotent() -> Result<()> {
        let task_id = TaskId::from_bytes([4; 16]);
        let item = InboxItem {
            task_id,
            sequence: 1,
            message_id: "message-1".into(),
            payload: state_ref()?,
        };
        let mut inbox = TaskInbox::new(task_id);
        inbox.apply(item.clone())?;
        inbox.apply(item)?;
        assert_eq!(inbox.after(0, 10).len(), 1);
        Ok(())
    }

    #[test]
    fn terminal_parent_rejects_late_children() -> Result<()> {
        let mut scheduler = Scheduler::new();
        scheduler.apply(scheduler.declare(spec(id(1), Orchestration::Join)?)?)?;
        scheduler.apply(SchedulerEvent::CancellationRequested {
            operation_id: id(1),
            recursive: false,
        })?;
        let mut child = spec(id(2), Orchestration::Leaf)?;
        child.parent = Some(ParentLink {
            operation_id: id(1),
            slot: "late".into(),
        });
        assert!(matches!(scheduler.declare(child), Err(Error::Conflict(_))));
        Ok(())
    }

    #[test]
    fn recursive_cancellation_stops_at_owner_boundaries() -> Result<()> {
        let mut scheduler = Scheduler::new();
        let mut parent = spec(id(20), Orchestration::Join)?;
        let authority = parent.owner.authority().clone();
        parent.owner = DurableOwner::Attached {
            authority: authority.clone(),
        };
        scheduler.apply(scheduler.declare(parent)?)?;

        let mut owned_child = spec(id(21), Orchestration::Leaf)?;
        owned_child.owner = DurableOwner::Detached {
            authority: authority.clone(),
        };
        owned_child.parent = Some(ParentLink {
            operation_id: id(20),
            slot: "owned".into(),
        });
        scheduler.apply(scheduler.declare(owned_child)?)?;

        let mut owned_grandchild = spec(id(23), Orchestration::Leaf)?;
        owned_grandchild.owner = DurableOwner::Attached { authority };
        owned_grandchild.parent = Some(ParentLink {
            operation_id: id(21),
            slot: "owned-grandchild".into(),
        });
        scheduler.apply(scheduler.declare(owned_grandchild)?)?;

        let mut foreign_child = spec(id(22), Orchestration::Leaf)?;
        foreign_child.parent = Some(ParentLink {
            operation_id: id(20),
            slot: "foreign".into(),
        });
        scheduler.apply(scheduler.declare(foreign_child)?)?;

        scheduler.apply(SchedulerEvent::CancellationRequested {
            operation_id: id(20),
            recursive: true,
        })?;

        assert_eq!(
            scheduler
                .operation(id(21))
                .and_then(|state| state.outcome.as_ref()),
            Some(&Outcome::Cancelled)
        );
        assert_eq!(
            scheduler
                .operation(id(23))
                .and_then(|state| state.outcome.as_ref()),
            Some(&Outcome::Cancelled)
        );
        assert_eq!(
            scheduler.operation(id(22)).map(|state| state.phase),
            Some(OperationPhase::WaitingForDependencies)
        );
        Ok(())
    }

    #[test]
    fn releasing_a_cancelled_running_lease_terminalizes_it() -> Result<()> {
        let mut scheduler = Scheduler::new();
        scheduler.apply(scheduler.declare(spec(id(9), Orchestration::Leaf)?)?)?;
        let reservation = Reservation {
            id: "cancelled-lease".into(),
            placement: "worker".into(),
            admitted: ResourceRequest::default(),
        };
        let fence = LeaseFence::from(&reservation);
        scheduler.apply(SchedulerEvent::Admitted {
            operation_id: id(9),
            reservation,
        })?;
        scheduler.apply(SchedulerEvent::Started {
            operation_id: id(9),
            fence: fence.clone(),
        })?;
        scheduler.apply(SchedulerEvent::CancellationRequested {
            operation_id: id(9),
            recursive: false,
        })?;
        scheduler.apply(SchedulerEvent::LeaseReleased {
            operation_id: id(9),
            fence,
        })?;
        assert_eq!(
            scheduler
                .operation(id(9))
                .and_then(|state| state.outcome.as_ref()),
            Some(&Outcome::Cancelled)
        );
        Ok(())
    }

    #[test]
    fn cancellation_closes_a_waiting_parent_before_orchestration() -> Result<()> {
        let mut scheduler = Scheduler::new();
        scheduler.apply(scheduler.declare(spec(id(10), Orchestration::Join)?)?)?;
        let reservation = Reservation {
            id: "parent".into(),
            placement: "worker".into(),
            admitted: ResourceRequest::default(),
        };
        let fence = LeaseFence::from(&reservation);
        scheduler.apply(SchedulerEvent::Admitted {
            operation_id: id(10),
            reservation,
        })?;
        scheduler.apply(SchedulerEvent::Started {
            operation_id: id(10),
            fence: fence.clone(),
        })?;
        scheduler.apply(SchedulerEvent::WaitingForChildren {
            operation_id: id(10),
            fence,
        })?;
        let expected_revision = scheduler
            .operation(id(10))
            .ok_or_else(|| Error::NotFound("parent".into()))?
            .revision;
        scheduler.apply(SchedulerEvent::CancellationRequested {
            operation_id: id(10),
            recursive: false,
        })?;
        assert_eq!(
            scheduler
                .operation(id(10))
                .and_then(|state| state.outcome.as_ref()),
            Some(&Outcome::Cancelled)
        );
        assert!(
            scheduler
                .apply(SchedulerEvent::Orchestrated {
                    operation_id: id(10),
                    expected_revision,
                    outcome: Outcome::Succeeded(state_ref()?),
                    cancel: Vec::new(),
                    reducer: None,
                    reduction_digest: None,
                })
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn reduction_is_bound_to_the_exact_contract_and_inputs() -> Result<()> {
        let reducer = EntrypointRef {
            name: "example.reducer".into(),
            version: "1".into(),
            digest: [4; 32],
            result_schema: serde_json::json!({"type": "number"}),
        };
        let mut scheduler = Scheduler::new();
        scheduler.apply(scheduler.declare(spec(
            id(11),
            Orchestration::Reduce {
                reducer: reducer.clone(),
            },
        )?)?)?;
        let parent_reservation = Reservation {
            id: "reduce-parent".into(),
            placement: "worker".into(),
            admitted: ResourceRequest::default(),
        };
        let parent_fence = LeaseFence::from(&parent_reservation);
        scheduler.apply(SchedulerEvent::Admitted {
            operation_id: id(11),
            reservation: parent_reservation,
        })?;
        scheduler.apply(SchedulerEvent::Started {
            operation_id: id(11),
            fence: parent_fence.clone(),
        })?;
        scheduler.apply(SchedulerEvent::WaitingForChildren {
            operation_id: id(11),
            fence: parent_fence,
        })?;
        let mut child = spec(id(12), Orchestration::Leaf)?;
        child.parent = Some(ParentLink {
            operation_id: id(11),
            slot: "value".into(),
        });
        scheduler.apply(scheduler.declare(child)?)?;
        let child_reservation = Reservation {
            id: "reduce-child".into(),
            placement: "worker".into(),
            admitted: ResourceRequest::default(),
        };
        let child_fence = LeaseFence::from(&child_reservation);
        scheduler.apply(SchedulerEvent::Admitted {
            operation_id: id(12),
            reservation: child_reservation,
        })?;
        scheduler.apply(SchedulerEvent::Started {
            operation_id: id(12),
            fence: child_fence.clone(),
        })?;
        scheduler.apply(SchedulerEvent::Completed {
            operation_id: id(12),
            outcome: Outcome::Succeeded(result_ref(b"2")?),
            fence: Some(child_fence),
            execution_duration_ns: None,
        })?;
        let OrchestrationDecision::Reduce { values, .. } = scheduler.orchestration(id(11)) else {
            return Err(Error::Conflict("reducer not ready".into()));
        };
        let expected_revision = scheduler
            .operation(id(11))
            .ok_or_else(|| Error::NotFound("parent".into()))?
            .revision;
        assert!(
            scheduler
                .apply(SchedulerEvent::Orchestrated {
                    operation_id: id(11),
                    expected_revision,
                    outcome: Outcome::Succeeded(result_ref(b"2")?),
                    cancel: Vec::new(),
                    reducer: Some(reducer.clone()),
                    reduction_digest: Some([0; 32]),
                })
                .is_err()
        );
        scheduler.apply(SchedulerEvent::Orchestrated {
            operation_id: id(11),
            expected_revision,
            outcome: Outcome::Succeeded(result_ref(b"2")?),
            cancel: Vec::new(),
            reducer: Some(reducer.clone()),
            reduction_digest: Some(reduction_invocation_digest(&reducer, &values)?),
        })?;
        Ok(())
    }

    #[test]
    fn failed_dependencies_are_explicitly_rejectable() -> Result<()> {
        let mut scheduler = Scheduler::new();
        scheduler.apply(scheduler.declare(spec(id(1), Orchestration::Leaf)?)?)?;
        scheduler.apply(SchedulerEvent::Admitted {
            operation_id: id(1),
            reservation: Reservation {
                id: "dependency".into(),
                placement: "worker".into(),
                admitted: ResourceRequest::default(),
            },
        })?;
        scheduler.apply(SchedulerEvent::Started {
            operation_id: id(1),
            fence: LeaseFence {
                reservation_id: "dependency".into(),
                placement: "worker".into(),
            },
        })?;
        scheduler.apply(SchedulerEvent::Completed {
            operation_id: id(1),
            outcome: Outcome::Failed {
                message: "failed".into(),
            },
            fence: Some(LeaseFence {
                reservation_id: "dependency".into(),
                placement: "worker".into(),
            }),
            execution_duration_ns: None,
        })?;
        let mut dependent = spec(id(2), Orchestration::Leaf)?;
        dependent.dependencies.insert(id(1));
        scheduler.apply(scheduler.declare(dependent)?)?;
        assert_eq!(scheduler.blocked_by_dependencies(), vec![id(2)]);
        scheduler.apply(SchedulerEvent::Rejected {
            operation_id: id(2),
            reason: "dependency failed".into(),
        })?;
        assert_eq!(
            scheduler
                .operation(id(2))
                .and_then(|state| state.outcome.as_ref()),
            Some(&Outcome::Failed {
                message: "dependency failed".into(),
            })
        );
        Ok(())
    }

    #[test]
    fn partial_admission_advances_one_lease_and_only_requires_the_remainder() -> Result<()> {
        let mut scheduler = Scheduler::new();
        let mut operation = spec(id(5), Orchestration::Leaf)?;
        operation.resources = ResourceRequest(BTreeMap::from([("cpu".into(), 4)]));
        scheduler.apply(scheduler.declare(operation)?)?;
        scheduler.apply(SchedulerEvent::PartiallyAdmitted {
            operation_id: id(5),
            reservation: Reservation {
                id: "lease".into(),
                placement: "worker".into(),
                admitted: ResourceRequest(BTreeMap::from([("cpu".into(), 2)])),
            },
        })?;
        assert_eq!(
            scheduler.ready(&ResourceSnapshot(BTreeMap::from([("cpu".into(), 2)]))),
            vec![id(5)]
        );
        assert!(matches!(
            scheduler.apply(SchedulerEvent::PartiallyAdmitted {
                operation_id: id(5),
                reservation: Reservation {
                    id: "another".into(),
                    placement: "worker".into(),
                    admitted: ResourceRequest(BTreeMap::from([("cpu".into(), 3)])),
                },
            }),
            Err(Error::Conflict(_))
        ));
        Ok(())
    }
}
