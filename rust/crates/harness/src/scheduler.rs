//! Deterministic durable scheduling and structured orchestration semantics.

use crate::{
    Error, IdempotencyKey, OperationId, Outcome, Result, TaskId,
    conversation::FileRef,
    core::Authority,
    resources::CheckpointRef,
    swarm_budget::{
        ForkPublication, SwarmBudget, SwarmBudgetEvent, SwarmBudgetLimits, SwarmBudgetUsage,
        SwarmForkRequest, SwarmOwnerFence, SwarmReservationState, SwarmResourceRequest, SwarmUsage,
        SwarmUsageReceipt,
    },
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
        self.0
            .iter()
            .all(|(resource, requested)| capacity.0.get(resource).unwrap_or(&0) >= requested)
    }
}

/// Available logical resources advertised to admission policy.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResourceSnapshot(pub BTreeMap<String, u64>);

/// Maps the recursive swarm budget dimensions to the scheduler's canonical
/// resource names. Both projections must carry this exact request before a
/// child can be admitted through the combined coordinator event.
#[must_use]
pub fn canonical_swarm_resources(request: SwarmResourceRequest) -> ResourceRequest {
    ResourceRequest(BTreeMap::from([
        ("model_steps".to_owned(), request.model_steps),
        ("output_bytes".to_owned(), request.output_bytes),
        ("execution_time_ms".to_owned(), request.execution_time_ms),
    ]))
}

/// Recognizes an operation that has opted into the recursive swarm resource
/// contract and therefore must use the authenticated swarm admission API.
#[must_use]
pub fn is_canonical_swarm_resources(request: &ResourceRequest) -> bool {
    request
        .0
        .keys()
        .map(String::as_str)
        .eq(["execution_time_ms", "model_steps", "output_bytes"])
}

/// Returns true when an operation mentions any dimension owned by swarm
/// admission. Extra provider resources cannot be used to evade the lifecycle
/// gate by adding another key to an otherwise swarm-shaped request.
#[must_use]
pub fn contains_swarm_resource(request: &ResourceRequest) -> bool {
    request.0.keys().any(|key| {
        matches!(
            key.as_str(),
            "model_steps" | "output_bytes" | "execution_time_ms"
        )
    })
}

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
    /// Dispatch identity bound to provider usage receipts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub swarm_dispatch_id: Option<IdempotencyKey>,
    /// Last provider usage receipt sequence accepted for this operation.
    #[serde(default)]
    pub swarm_usage_sequence: u64,
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
    /// Atomically admits a recursive swarm child in the scheduler and budget
    /// projection. The task declaration and its dependency graph remain the
    /// source of readiness; this event only adds the resource reservation.
    SwarmAdmitted {
        /// Child operation identity.
        operation_id: OperationId,
        /// Session budget identity shared by root and descendants.
        session_id: OperationId,
        /// Immutable session limits carried on every event for replay checks.
        limits: SwarmBudgetLimits,
        /// Owner fence for the reservation.
        owner: SwarmOwnerFence,
        /// Canonical resource reservation request.
        request: SwarmForkRequest,
        /// Durable canonical task admission envelope staged before this
        /// event; replay verifies the declaration still points at it.
        admission_reference: FileRef,
        /// Existing scheduler lease reservation.
        reservation: Reservation,
    },
    /// Atomically records verified fork publication and starts child dispatch.
    SwarmDispatchStarted {
        /// Child operation identity.
        operation_id: OperationId,
        /// Stable provider dispatch identity retained for unknown-result
        /// reconciliation and exact retries.
        dispatch_id: IdempotencyKey,
        /// Scheduler lease fence.
        fence: LeaseFence,
        /// Swarm owner fence.
        owner: SwarmOwnerFence,
        /// Exact publication evidence produced by the fork helper.
        publication: ForkPublication,
    },
    /// Binds the authenticated root provider source before any root claim or receipt.
    SwarmRootProviderBound {
        /// Budget session and root operation identity.
        session_id: OperationId,
        /// Swarm owner fence for the budget session.
        owner: SwarmOwnerFence,
        /// Active scheduler lease for the session root.
        fence: LeaseFence,
        /// Stable provider identity selected by the host.
        provider: String,
        /// Durable source capability fingerprint.
        fingerprint: [u8; 32],
    },
    /// Records cumulative child usage while retaining the scheduler lease.
    SwarmUsageReported {
        /// Child operation identity.
        operation_id: OperationId,
        /// Scheduler lease fence.
        fence: LeaseFence,
        /// Swarm owner fence.
        owner: SwarmOwnerFence,
        /// Cumulative measured usage.
        usage: SwarmUsage,
        /// Provider-issued usage evidence; legacy reducer fixtures may omit it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        receipt: Option<SwarmUsageReceipt>,
    },
    /// Records cumulative measured usage from the session root.
    SwarmRootUsageReported {
        /// Budget session identity and root operation identity.
        session_id: OperationId,
        /// Swarm owner fence.
        owner: SwarmOwnerFence,
        /// Active scheduler lease for the session root.
        fence: LeaseFence,
        /// Cumulative measured root usage.
        usage: SwarmUsage,
        /// Provider-issued usage evidence; legacy reducer fixtures may omit it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        receipt: Option<SwarmUsageReceipt>,
        /// Authenticated source capability that issued the root receipt.
        #[serde(default)]
        fingerprint: [u8; 32],
    },
    /// Completes a child and releases only its unconsumed budget reservation.
    SwarmCompleted {
        /// Child operation identity.
        operation_id: OperationId,
        /// Scheduler lease fence for known worker completion.
        fence: Option<LeaseFence>,
        /// Swarm owner fence.
        owner: SwarmOwnerFence,
        /// Cumulative measured usage.
        usage: SwarmUsage,
        /// Provider-issued usage evidence; legacy reducer fixtures may omit it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        receipt: Option<SwarmUsageReceipt>,
        /// Terminal or indeterminate child outcome.
        outcome: Outcome<FileRef>,
    },
    /// Requests cancellation and derives the complete structured subtree.
    SwarmCancelled {
        /// Root operation whose subtree is cancelled.
        operation_id: OperationId,
        /// Whether descendants are included.
        recursive: bool,
        /// Swarm owner fence.
        owner: SwarmOwnerFence,
    },
    /// Advances the session owner generation after authenticated recovery.
    SwarmTakeover {
        /// Budget session identity.
        session_id: OperationId,
        /// Exact owner fence observed before takeover.
        expected_owner: SwarmOwnerFence,
        /// New owner fence.
        owner: SwarmOwnerFence,
    },
}

/// Pure reducer for dependency, capacity, ownership, and cancellation state.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Scheduler {
    operations: BTreeMap<OperationId, OperationState>,
    child_slots: BTreeMap<(OperationId, String), OperationId>,
    completion_order: Vec<OperationId>,
    /// Budget events projected in the same coordinator history as scheduler
    /// lifecycle events. Empty means this scheduler has no swarm session.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    swarm_events: Vec<SwarmBudgetEvent>,
}

impl Scheduler {
    /// Creates an empty scheduler projection.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            operations: BTreeMap::new(),
            child_slots: BTreeMap::new(),
            completion_order: Vec::new(),
            swarm_events: Vec::new(),
        }
    }

    /// Returns one operation projection.
    #[must_use]
    pub fn operation(&self, id: OperationId) -> Option<&OperationState> {
        self.operations.get(&id)
    }

    /// Returns the budget projection committed in this coordinator history.
    pub fn swarm_budget_usage(&self) -> Result<Option<SwarmBudgetUsage>> {
        if self.swarm_events.is_empty() {
            Ok(None)
        } else {
            SwarmBudget::replay(self.swarm_events.clone())
                .and_then(|budget| budget.usage().map(Some))
        }
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
                    || key.len() > 255
                    || value.len() > 255
                    || key.chars().any(char::is_control)
                    || value.chars().any(char::is_control)
            })
        {
            return Err(Error::Invalid(
                "placement labels exceed scheduler limits".into(),
            ));
        }
        jsonschema::validator_for(&spec.entrypoint.result_schema).map_err(|error| {
            Error::Invalid(format!("invalid entrypoint result schema: {error}"))
        })?;
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
                || parent.slot.len() > 255
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
    pub(crate) fn apply(&mut self, event: SchedulerEvent) -> Result<()> {
        self.apply_with_swarm_guard(event, false)
    }

    fn apply_with_swarm_guard(
        &mut self,
        event: SchedulerEvent,
        allow_swarm_generic: bool,
    ) -> Result<()> {
        let primary = event_operation(&event);
        if !allow_swarm_generic
            && self.has_swarm_lifecycle(primary)?
            && matches!(
                &event,
                SchedulerEvent::Admitted { .. }
                    | SchedulerEvent::PartiallyAdmitted { .. }
                    | SchedulerEvent::Started { .. }
                    | SchedulerEvent::Checkpointed { .. }
                    | SchedulerEvent::WaitingForChildren { .. }
                    | SchedulerEvent::LeaseReleased { .. }
                    | SchedulerEvent::CancellationRequested { .. }
                    | SchedulerEvent::Completed { .. }
                    | SchedulerEvent::Orchestrated { .. }
            )
        {
            return Err(Error::Conflict(
                "swarm operations require compound lifecycle transitions".into(),
            ));
        }
        let compound_swarm = matches!(
            &event,
            SchedulerEvent::SwarmAdmitted { .. }
                | SchedulerEvent::SwarmDispatchStarted { .. }
                | SchedulerEvent::SwarmRootProviderBound { .. }
                | SchedulerEvent::SwarmUsageReported { .. }
                | SchedulerEvent::SwarmRootUsageReported { .. }
                | SchedulerEvent::SwarmCompleted { .. }
                | SchedulerEvent::SwarmCancelled { .. }
                | SchedulerEvent::SwarmTakeover { .. }
        );
        let declared_parent = match &event {
            SchedulerEvent::Declared { spec } => {
                spec.parent.as_ref().map(|value| value.operation_id)
            }
            _ => None,
        };
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
                        swarm_dispatch_id: None,
                        swarm_usage_sequence: 0,
                    },
                );
            }
            SchedulerEvent::WaitingForCapacity { operation_id } => {
                let dependencies_ready = {
                    let operation = self
                        .operations
                        .get(&operation_id)
                        .ok_or_else(|| Error::NotFound(format!("operation {operation_id}")))?;
                    self.dependencies_succeeded(operation)
                };
                if !dependencies_ready {
                    return Err(Error::Conflict(
                        "operation dependencies are not ready".into(),
                    ));
                }
                let operation = self.mutable(operation_id)?;
                require_phase(operation, OperationPhase::WaitingForDependencies)?;
                operation.phase = OperationPhase::WaitingForCapacity;
            }
            SchedulerEvent::Admitted {
                operation_id,
                reservation,
            } => {
                let dependencies_ready = {
                    let operation = self
                        .operations
                        .get(&operation_id)
                        .ok_or_else(|| Error::NotFound(format!("operation {operation_id}")))?;
                    self.dependencies_succeeded(operation)
                };
                if !dependencies_ready {
                    return Err(Error::Conflict(
                        "operation dependencies are not ready".into(),
                    ));
                }
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
                if !operation
                    .spec
                    .resources
                    .0
                    .iter()
                    .all(|(resource, requested)| {
                        reservation.admitted.0.get(resource).unwrap_or(&0) >= requested
                    })
                {
                    return Err(Error::Conflict(
                        "partial reservation cannot start an operation".into(),
                    ));
                }
                if operation.reservation.as_ref().is_some_and(|partial| {
                    partial.id != reservation.id || partial.placement != reservation.placement
                }) {
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
                let dependencies_ready = {
                    let operation = self
                        .operations
                        .get(&operation_id)
                        .ok_or_else(|| Error::NotFound(format!("operation {operation_id}")))?;
                    self.dependencies_succeeded(operation)
                };
                if !dependencies_ready {
                    return Err(Error::Conflict(
                        "operation dependencies are not ready".into(),
                    ));
                }
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
                let within_request = reservation.admitted.0.iter().all(|(resource, admitted)| {
                    operation.spec.resources.0.get(resource).unwrap_or(&0) >= admitted
                });
                if !within_request
                    || operation
                        .spec
                        .resources
                        .0
                        .iter()
                        .all(|(resource, requested)| {
                            reservation.admitted.0.get(resource).unwrap_or(&0) >= requested
                        })
                {
                    return Err(Error::Invalid(
                        "partial admission must be non-excessive and incomplete".into(),
                    ));
                }
                if let Some(existing) = &operation.reservation
                    && (existing.id != reservation.id
                        || existing.placement != reservation.placement
                        || existing.admitted.0.iter().any(|(resource, quantity)| {
                            reservation.admitted.0.get(resource).unwrap_or(&0) < quantity
                        }))
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
                operation.phase = OperationPhase::WaitingForChildren;
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
                    let mut terminalized = false;
                    let retained_for_live_descendant = self.has_live_swarm_descendant(target)?;
                    {
                        let operation = self.mutable(target)?;
                        if operation.phase == OperationPhase::Terminal {
                            continue;
                        }
                        if matches!(operation.phase, OperationPhase::WaitingForChildren)
                            && retained_for_live_descendant
                        {
                            operation.cancellation_requested = true;
                        } else if matches!(
                            operation.phase,
                            OperationPhase::WaitingForDependencies
                                | OperationPhase::WaitingForCapacity
                                | OperationPhase::Admitted
                                | OperationPhase::WaitingForChildren
                        ) {
                            operation.reservation = None;
                            operation.phase = OperationPhase::Terminal;
                            operation.outcome = Some(Outcome::Cancelled);
                            terminalized = true;
                        } else {
                            operation.cancellation_requested = true;
                        }
                        if target != operation_id {
                            operation.revision =
                                operation.revision.checked_add(1).ok_or_else(|| {
                                    Error::Invalid("operation revision exhausted".into())
                                })?;
                        }
                    }
                    if terminalized {
                        self.completion_order.push(target);
                    }
                }
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
                if !matches!(outcome, Outcome::Indeterminate { .. }) {
                    operation.reservation = None;
                }
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
            SchedulerEvent::SwarmAdmitted {
                operation_id,
                session_id,
                limits,
                owner,
                request,
                admission_reference,
                reservation,
            } => {
                let mut next = self.clone();
                next.apply_with_swarm_guard(
                    SchedulerEvent::Admitted {
                        operation_id,
                        reservation: reservation.clone(),
                    },
                    true,
                )?;
                next.apply_swarm_admitted(
                    session_id,
                    limits,
                    owner,
                    request,
                    admission_reference,
                    reservation,
                )?;
                *self = next;
            }
            SchedulerEvent::SwarmDispatchStarted {
                operation_id,
                dispatch_id,
                fence,
                owner,
                publication,
            } => {
                IdempotencyKey::new(dispatch_id.0.clone())?;
                let mut next = self.clone();
                next.apply_with_swarm_guard(
                    SchedulerEvent::Started {
                        operation_id,
                        fence,
                    },
                    true,
                )?;
                next.mutable(operation_id)?.swarm_dispatch_id = Some(dispatch_id.clone());
                next.apply_swarm_dispatch_started(operation_id, dispatch_id, owner, publication)?;
                *self = next;
            }
            SchedulerEvent::SwarmRootProviderBound {
                session_id,
                owner,
                fence,
                provider,
                fingerprint,
            } => {
                let mut next = self.clone();
                let root = next.mutable(session_id)?;
                require_phase(root, OperationPhase::Running)?;
                require_fence(root, &fence)?;
                next.apply_swarm_root_provider_bound(session_id, owner, provider, fingerprint)?;
                if let Some(operation) = next.operations.get_mut(&session_id) {
                    operation.revision = operation
                        .revision
                        .checked_add(1)
                        .ok_or_else(|| Error::Invalid("operation revision exhausted".into()))?;
                }
                *self = next;
            }
            SchedulerEvent::SwarmUsageReported {
                operation_id,
                fence,
                owner,
                usage,
                receipt,
            } => {
                let mut next = self.clone();
                let operation = next.mutable(operation_id)?;
                require_phase(operation, OperationPhase::Running)?;
                require_fence(operation, &fence)?;
                next.validate_swarm_usage_receipt(operation_id, usage, receipt.as_ref())?;
                next.apply_swarm_usage(operation_id, owner, usage, receipt.as_ref())?;
                if let Some(receipt) = receipt {
                    next.mutable(operation_id)?.swarm_usage_sequence = receipt.sequence;
                }
                let operation = next.mutable(operation_id)?;
                operation.revision = operation
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| Error::Invalid("operation revision exhausted".into()))?;
                *self = next;
            }
            SchedulerEvent::SwarmRootUsageReported {
                session_id,
                owner,
                fence,
                usage,
                receipt,
                fingerprint,
            } => {
                let mut next = self.clone();
                let root = next.mutable(session_id)?;
                require_phase(root, OperationPhase::Running)?;
                require_fence(root, &fence)?;
                next.validate_swarm_root_usage_receipt(
                    session_id,
                    &fence,
                    usage,
                    receipt.as_ref(),
                )?;
                next.apply_swarm_root_usage(
                    session_id,
                    owner,
                    usage,
                    receipt.as_ref(),
                    fingerprint,
                )?;
                if let Some(receipt) = receipt {
                    next.mutable(session_id)?.swarm_usage_sequence = receipt.sequence;
                }
                *self = next;
            }
            SchedulerEvent::SwarmCompleted {
                operation_id,
                fence,
                owner,
                usage,
                receipt,
                outcome,
            } => {
                let mut next = self.clone();
                next.validate_swarm_usage_receipt(operation_id, usage, receipt.as_ref())?;
                next.apply_with_swarm_guard(
                    SchedulerEvent::Completed {
                        operation_id,
                        outcome: outcome.clone(),
                        fence,
                        execution_duration_ns: None,
                    },
                    true,
                )?;
                next.apply_swarm_completed(operation_id, owner, usage, receipt.as_ref(), &outcome)?;
                if let Some(receipt) = receipt {
                    next.mutable(operation_id)?.swarm_usage_sequence = receipt.sequence;
                }
                *self = next;
            }
            SchedulerEvent::SwarmCancelled {
                operation_id,
                recursive,
                owner,
            } => {
                let mut next = self.clone();
                let frontier = next.cancellation_frontier(operation_id, recursive);
                next.apply_with_swarm_guard(
                    SchedulerEvent::CancellationRequested {
                        operation_id,
                        recursive,
                    },
                    true,
                )?;
                next.apply_swarm_cancelled(frontier, owner)?;
                *self = next;
            }
            SchedulerEvent::SwarmTakeover {
                session_id,
                expected_owner,
                owner,
            } => {
                let mut next = self.clone();
                next.apply_swarm_takeover(session_id, &expected_owner, owner)?;
                *self = next;
            }
            SchedulerEvent::Orchestrated {
                operation_id,
                expected_revision,
                outcome,
                cancel,
                reducer,
                reduction_digest,
            } => {
                if self.has_swarm_lifecycle(operation_id)? {
                    return Err(Error::Conflict(
                        "swarm operations require compound lifecycle transitions".into(),
                    ));
                }
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
                    let mut terminalized = false;
                    let child = self.mutable(*child_id)?;
                    if matches!(
                        child.phase,
                        OperationPhase::WaitingForDependencies
                            | OperationPhase::WaitingForCapacity
                            | OperationPhase::Admitted
                    ) {
                        child.reservation = None;
                        child.phase = OperationPhase::Terminal;
                        child.outcome = Some(Outcome::Cancelled);
                        terminalized = true;
                    } else if child.phase != OperationPhase::Terminal {
                        child.cancellation_requested = true;
                    }
                    child.revision = child
                        .revision
                        .checked_add(1)
                        .ok_or_else(|| Error::Invalid("operation revision exhausted".into()))?;
                    if terminalized {
                        self.completion_order.push(*child_id);
                    }
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
        if !compound_swarm {
            let operation = self.mutable(primary)?;
            operation.revision = operation
                .revision
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("operation revision exhausted".into()))?;
            if let Some(parent) = declared_parent {
                let parent = self.mutable(parent)?;
                parent.revision = parent
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| Error::Invalid("operation revision exhausted".into()))?;
            }
        }
        Ok(())
    }

    fn apply_swarm_admitted(
        &mut self,
        session_id: OperationId,
        limits: SwarmBudgetLimits,
        owner: SwarmOwnerFence,
        request: SwarmForkRequest,
        admission_reference: FileRef,
        reservation: Reservation,
    ) -> Result<()> {
        let expected_resources = canonical_swarm_resources(request.resources);
        let operation = self
            .operations
            .get(&request.operation_id)
            .ok_or_else(|| Error::NotFound(format!("operation {}", request.operation_id)))?;
        if operation.spec.resources != expected_resources
            || reservation.admitted != expected_resources
            || operation.spec.state != admission_reference
        {
            return Err(Error::Conflict(
                "scheduler and swarm reservations must use canonical resources".into(),
            ));
        }
        let first = self.swarm_events.is_empty();
        let session_operation = self
            .operations
            .get(&session_id)
            .ok_or_else(|| Error::NotFound(format!("swarm session operation {session_id}")))?;
        if session_operation.spec.parent.is_some()
            || session_operation.spec.owner.authority().id != owner.owner
        {
            return Err(Error::Unauthorized(
                "swarm budget session must bind its declared root owner".into(),
            ));
        }
        if session_operation.phase == OperationPhase::Terminal
            || session_operation.cancellation_requested
        {
            return Err(Error::Conflict(
                "terminal or cancelled swarm session cannot admit children".into(),
            ));
        }
        if session_operation.phase != OperationPhase::Running
            || session_operation.reservation.is_none()
        {
            return Err(Error::Conflict(
                "swarm child admission requires a running root lease".into(),
            ));
        }
        let root_dispatch_id = session_operation
            .reservation
            .as_ref()
            .map(|reservation| IdempotencyKey::new(reservation.id.clone()))
            .transpose()?;
        let budget = if first {
            SwarmBudget::new_with_root_dispatch(
                session_id,
                owner.clone(),
                limits,
                root_dispatch_id.clone(),
            )?
        } else {
            let budget = SwarmBudget::replay(self.swarm_events.clone())?;
            let (observed_session, observed_owner, observed_limits) = budget.descriptor()?;
            if observed_session != session_id
                || observed_owner != owner
                || observed_limits != limits
                || budget.root_dispatch_id()? != root_dispatch_id
            {
                return Err(Error::Conflict(
                    "swarm budget descriptor differs from scheduler projection".into(),
                ));
            }
            budget
        };
        let receipt = budget.reserve_child(request)?;
        if first {
            self.swarm_events.push(SwarmBudgetEvent::Started {
                session_id,
                owner,
                limits,
                root_dispatch_id,
            });
        }
        self.swarm_events.push(receipt.durable_event());
        Ok(())
    }

    fn has_live_swarm_descendant(&self, operation_id: OperationId) -> Result<bool> {
        if self.swarm_events.is_empty() {
            return Ok(false);
        }
        let budget = SwarmBudget::replay(self.swarm_events.clone())?;
        let mut pending = self
            .children(operation_id)
            .map(|(_, child)| child.spec.operation_id)
            .collect::<Vec<_>>();
        while let Some(candidate) = pending.pop() {
            if budget.reservation(candidate)?.is_some_and(|reservation| {
                matches!(
                    reservation.state,
                    SwarmReservationState::Reserved | SwarmReservationState::Active
                )
            }) {
                return Ok(true);
            }
            pending.extend(
                self.children(candidate)
                    .map(|(_, child)| child.spec.operation_id),
            );
        }
        Ok(false)
    }

    fn apply_swarm_dispatch_started(
        &mut self,
        operation_id: OperationId,
        dispatch_id: IdempotencyKey,
        owner: SwarmOwnerFence,
        publication: ForkPublication,
    ) -> Result<()> {
        let budget = SwarmBudget::replay(self.swarm_events.clone())?;
        let token =
            budget.activate_with_dispatch(operation_id, owner, publication, Some(dispatch_id))?;
        self.swarm_events.push(SwarmBudgetEvent::ChildActivated {
            operation_id,
            owner: token.owner().clone(),
            publication: token.publication(),
            dispatch_id: token.dispatch_id().cloned(),
        });
        Ok(())
    }

    fn apply_swarm_usage(
        &mut self,
        operation_id: OperationId,
        owner: SwarmOwnerFence,
        usage: SwarmUsage,
        receipt: Option<&SwarmUsageReceipt>,
    ) -> Result<()> {
        let receipt = receipt.ok_or_else(|| {
            Error::Unauthorized("provider usage receipt required for swarm usage".into())
        })?;
        let budget = SwarmBudget::replay(self.swarm_events.clone())?;
        budget.report_usage_event(operation_id, &owner, usage, Some(receipt))?;
        self.swarm_events.push(SwarmBudgetEvent::UsageReported {
            operation_id,
            owner,
            usage,
            receipt: receipt.clone(),
        });
        Ok(())
    }

    fn apply_swarm_root_provider_bound(
        &mut self,
        session_id: OperationId,
        owner: SwarmOwnerFence,
        provider: String,
        fingerprint: [u8; 32],
    ) -> Result<()> {
        let budget = SwarmBudget::replay(self.swarm_events.clone())?;
        let (observed_session, _, _) = budget.descriptor()?;
        if observed_session != session_id {
            return Err(Error::Conflict(
                "root provider binding belongs to another swarm session".into(),
            ));
        }
        let root_operation = self
            .operations
            .get(&session_id)
            .ok_or_else(|| Error::NotFound(format!("operation {session_id}")))?;
        if root_operation.spec.parent.is_some() {
            return Err(Error::Unauthorized(
                "root provider binding requires the session root operation".into(),
            ));
        }
        budget.bind_root_provider_identity(&owner, provider.clone(), fingerprint)?;
        self.swarm_events.push(SwarmBudgetEvent::RootProviderBound {
            owner,
            provider,
            fingerprint,
        });
        Ok(())
    }

    fn apply_swarm_root_usage(
        &mut self,
        session_id: OperationId,
        owner: SwarmOwnerFence,
        usage: SwarmUsage,
        receipt: Option<&SwarmUsageReceipt>,
        fingerprint: [u8; 32],
    ) -> Result<()> {
        let receipt = receipt.ok_or_else(|| {
            Error::Unauthorized("provider usage receipt required for swarm root usage".into())
        })?;
        let budget = SwarmBudget::replay(self.swarm_events.clone())?;
        let (observed_session, _, _) = budget.descriptor()?;
        if observed_session != session_id {
            return Err(Error::Conflict(
                "root usage belongs to another swarm session".into(),
            ));
        }
        let root_operation = self
            .operations
            .get(&session_id)
            .ok_or_else(|| Error::NotFound(format!("operation {session_id}")))?;
        if root_operation.spec.parent.is_some() {
            return Err(Error::Unauthorized(
                "root usage requires the session root operation".into(),
            ));
        }
        if root_operation.phase == OperationPhase::Terminal || root_operation.cancellation_requested
        {
            return Err(Error::Conflict(
                "terminal or cancelled swarm session cannot report usage".into(),
            ));
        }
        budget.apply_event(SwarmBudgetEvent::RootUsageReported {
            owner: owner.clone(),
            usage,
            receipt: receipt.clone(),
            fingerprint,
        })?;
        self.swarm_events.push(SwarmBudgetEvent::RootUsageReported {
            owner,
            usage,
            receipt: receipt.clone(),
            fingerprint,
        });
        if let Some(operation) = self.operations.get_mut(&session_id) {
            operation.revision = operation
                .revision
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("operation revision exhausted".into()))?;
        }
        Ok(())
    }

    fn apply_swarm_completed(
        &mut self,
        operation_id: OperationId,
        owner: SwarmOwnerFence,
        usage: SwarmUsage,
        receipt: Option<&SwarmUsageReceipt>,
        outcome: &Outcome<FileRef>,
    ) -> Result<()> {
        let receipt = receipt.ok_or_else(|| {
            Error::Unauthorized("provider usage receipt required for swarm completion".into())
        })?;
        let budget = SwarmBudget::replay(self.swarm_events.clone())?;
        if matches!(outcome, Outcome::Indeterminate { .. }) {
            budget.report_usage_event(operation_id, &owner, usage, Some(receipt))?;
            self.swarm_events.push(SwarmBudgetEvent::UsageReported {
                operation_id,
                owner,
                usage,
                receipt: receipt.clone(),
            });
        } else {
            let mut budget = budget;
            budget.complete_event(operation_id, &owner, usage, Some(receipt))?;
            self.swarm_events.push(SwarmBudgetEvent::ChildCompleted {
                operation_id,
                owner: owner.clone(),
                usage,
                receipt: receipt.clone(),
            });
            self.close_cancelled_ancestors(operation_id, &mut budget)?;
        }
        Ok(())
    }

    fn close_cancelled_ancestors(
        &mut self,
        operation_id: OperationId,
        budget: &mut SwarmBudget,
    ) -> Result<()> {
        let mut ancestor = self
            .operations
            .get(&operation_id)
            .and_then(|operation| operation.spec.parent.as_ref())
            .map(|parent| parent.operation_id);
        while let Some(candidate) = ancestor {
            let child_ids = self
                .children(candidate)
                .map(|(_, child)| child.spec.operation_id)
                .collect::<Vec<_>>();
            let Some(parent) = self.operations.get(&candidate) else {
                break;
            };
            if !parent.cancellation_requested
                || child_ids.iter().any(|child_id| {
                    self.operations
                        .get(child_id)
                        .is_some_and(|child| child.phase != OperationPhase::Terminal)
                })
            {
                break;
            }
            if let Some(reservation) = budget.reservation(candidate)?
                && matches!(
                    reservation.state,
                    SwarmReservationState::Reserved | SwarmReservationState::Active
                )
            {
                budget.cancel(candidate, &reservation.owner)?;
                self.swarm_events.push(SwarmBudgetEvent::ChildCancelled {
                    operation_id: candidate,
                    owner: reservation.owner,
                });
            }
            let (was_terminal, next_ancestor) = {
                let parent = self.mutable(candidate)?;
                let next_ancestor = parent
                    .spec
                    .parent
                    .as_ref()
                    .map(|parent| parent.operation_id);
                if parent.phase != OperationPhase::Terminal {
                    parent.reservation = None;
                    parent.phase = OperationPhase::Terminal;
                    parent.outcome = Some(Outcome::Cancelled);
                    parent.revision = parent
                        .revision
                        .checked_add(1)
                        .ok_or_else(|| Error::Invalid("operation revision exhausted".into()))?;
                    (false, next_ancestor)
                } else {
                    (true, next_ancestor)
                }
            };
            if !was_terminal && !self.completion_order.contains(&candidate) {
                self.completion_order.push(candidate);
            }
            ancestor = next_ancestor;
        }
        Ok(())
    }

    fn apply_swarm_cancelled(
        &mut self,
        frontier: Vec<OperationId>,
        owner: SwarmOwnerFence,
    ) -> Result<()> {
        let mut budget = SwarmBudget::replay(self.swarm_events.clone())?;
        let terminal_frontier = frontier.clone();
        for operation_id in frontier.into_iter().rev() {
            // A running or reconciling operation keeps its budget lease until
            // a fenced completion/cancellation acknowledgement arrives. The
            // scheduler reducer marks the cancellation request first; only a
            // terminalized frontier entry can release capacity here.
            let terminal_cancelled = self.operations.get(&operation_id).is_some_and(|operation| {
                operation.phase == OperationPhase::Terminal
                    && operation.outcome == Some(Outcome::Cancelled)
            });
            if !terminal_cancelled {
                continue;
            }
            let Some(reservation) = budget.reservation(operation_id)? else {
                continue;
            };
            if !matches!(
                reservation.state,
                SwarmReservationState::Reserved | SwarmReservationState::Active
            ) {
                continue;
            }
            budget.cancel(operation_id, &owner)?;
            self.swarm_events.push(SwarmBudgetEvent::ChildCancelled {
                operation_id,
                owner: owner.clone(),
            });
        }
        for operation_id in terminal_frontier {
            if self.operations.get(&operation_id).is_some_and(|operation| {
                operation.phase == OperationPhase::Terminal
                    && operation.outcome == Some(Outcome::Cancelled)
            }) {
                self.close_cancelled_ancestors(operation_id, &mut budget)?;
            }
        }
        Ok(())
    }

    fn apply_swarm_takeover(
        &mut self,
        session_id: OperationId,
        expected_owner: &SwarmOwnerFence,
        owner: SwarmOwnerFence,
    ) -> Result<()> {
        let budget = SwarmBudget::replay(self.swarm_events.clone())?;
        let (observed_session, _, _) = budget.descriptor()?;
        if observed_session != session_id {
            return Err(Error::Conflict("swarm takeover session differs".into()));
        }
        let next_generation = expected_owner
            .generation
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("swarm owner generation exhausted".into()))?;
        if owner.generation != next_generation {
            return Err(Error::Conflict(
                "swarm takeover generation does not advance exactly once".into(),
            ));
        }
        let root = self
            .operations
            .get(&session_id)
            .ok_or_else(|| Error::NotFound(format!("operation {session_id}")))?;
        if root.phase == OperationPhase::Terminal || root.cancellation_requested {
            return Err(Error::Conflict(
                "terminal or cancelled swarm session cannot be taken over".into(),
            ));
        }
        let observed = budget.takeover(expected_owner, owner.owner.clone())?;
        if observed != owner {
            return Err(Error::Conflict(
                "swarm takeover generation does not advance exactly once".into(),
            ));
        }
        self.swarm_events
            .push(SwarmBudgetEvent::OwnerTakenOver { owner });
        let root = self.mutable(session_id)?;
        root.revision = root
            .revision
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("operation revision exhausted".into()))?;
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

    fn mutable(&mut self, id: OperationId) -> Result<&mut OperationState> {
        self.operations
            .get_mut(&id)
            .ok_or_else(|| Error::NotFound(format!("operation {id}")))
    }

    pub(crate) fn has_swarm_lifecycle(&self, operation_id: OperationId) -> Result<bool> {
        if self
            .operations
            .get(&operation_id)
            .is_some_and(|operation| contains_swarm_resource(&operation.spec.resources))
        {
            return Ok(true);
        }
        let budget = if self.swarm_events.is_empty() {
            None
        } else {
            Some(SwarmBudget::replay(self.swarm_events.clone())?)
        };
        if let Some(budget) = budget.as_ref() {
            if budget.descriptor()?.0 == operation_id || budget.reservation(operation_id)?.is_some()
            {
                return Ok(true);
            }
        }
        let mut pending = vec![operation_id];
        while let Some(parent) = pending.pop() {
            for (_, child) in self.children(parent) {
                let reserved = budget
                    .as_ref()
                    .map(|budget| budget.reservation(child.spec.operation_id))
                    .transpose()?
                    .flatten()
                    .is_some();
                if contains_swarm_resource(&child.spec.resources) || reserved {
                    return Ok(true);
                }
                pending.push(child.spec.operation_id);
            }
        }
        Ok(false)
    }

    fn validate_swarm_usage_receipt(
        &self,
        operation_id: OperationId,
        usage: SwarmUsage,
        receipt: Option<&SwarmUsageReceipt>,
    ) -> Result<()> {
        let receipt = receipt.ok_or_else(|| {
            Error::Unauthorized("provider usage receipt required for swarm usage".into())
        })?;
        receipt.validate()?;
        let operation = self
            .operations
            .get(&operation_id)
            .ok_or_else(|| Error::NotFound(format!("operation {operation_id}")))?;
        let expected_sequence = operation
            .swarm_usage_sequence
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("swarm usage receipt sequence exhausted".into()))?;
        if receipt.operation_id != operation_id
            || receipt.usage != usage
            || receipt.sequence != expected_sequence
            || operation.swarm_dispatch_id.as_ref() != Some(&receipt.dispatch_id)
        {
            return Err(Error::Conflict(
                "swarm usage receipt is stale or not bound to the dispatch".into(),
            ));
        }
        Ok(())
    }

    fn validate_swarm_root_usage_receipt(
        &self,
        operation_id: OperationId,
        fence: &LeaseFence,
        usage: SwarmUsage,
        receipt: Option<&SwarmUsageReceipt>,
    ) -> Result<()> {
        let receipt = receipt.ok_or_else(|| {
            Error::Unauthorized("provider usage receipt required for swarm root usage".into())
        })?;
        receipt.validate()?;
        let operation = self
            .operations
            .get(&operation_id)
            .ok_or_else(|| Error::NotFound(format!("operation {operation_id}")))?;
        let expected_sequence = operation
            .swarm_usage_sequence
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("swarm usage receipt sequence exhausted".into()))?;
        if receipt.operation_id != operation_id
            || receipt.usage != usage
            || receipt.sequence != expected_sequence
            || receipt.dispatch_id.as_str() != fence.reservation_id
        {
            return Err(Error::Conflict(
                "swarm root usage receipt is stale or not bound to the root lease".into(),
            ));
        }
        Ok(())
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
    if reservation.id != fence.reservation_id || reservation.placement != fence.placement {
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

fn event_operation(event: &SchedulerEvent) -> OperationId {
    match event {
        SchedulerEvent::Declared { spec } => spec.operation_id,
        SchedulerEvent::WaitingForCapacity { operation_id }
        | SchedulerEvent::Admitted { operation_id, .. }
        | SchedulerEvent::PartiallyAdmitted { operation_id, .. }
        | SchedulerEvent::Rejected { operation_id, .. }
        | SchedulerEvent::Started { operation_id, .. }
        | SchedulerEvent::Checkpointed { operation_id, .. }
        | SchedulerEvent::WaitingForChildren { operation_id, .. }
        | SchedulerEvent::LeaseReleased { operation_id, .. }
        | SchedulerEvent::CancellationRequested { operation_id, .. }
        | SchedulerEvent::Completed { operation_id, .. }
        | SchedulerEvent::Orchestrated { operation_id, .. }
        | SchedulerEvent::SwarmAdmitted { operation_id, .. }
        | SchedulerEvent::SwarmDispatchStarted { operation_id, .. }
        | SchedulerEvent::SwarmRootProviderBound { session_id: operation_id, .. }
        | SchedulerEvent::SwarmUsageReported { operation_id, .. }
        | SchedulerEvent::SwarmCompleted { operation_id, .. }
        | SchedulerEvent::SwarmCancelled { operation_id, .. } => *operation_id,
        SchedulerEvent::SwarmRootUsageReported { session_id, .. }
        | SchedulerEvent::SwarmTakeover { session_id, .. } => *session_id,
    }
}

/// One ref-only durable inbox item with a gapless per-task sequence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InboxItem {
    /// Owning task.
    pub task_id: TaskId,
    /// Authenticated sender retained by the owner journal.
    pub sender: TaskId,
    /// Owner stream commit timestamp used for delivery ordering and replay.
    pub delivered_at_epoch_ms: u64,
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
        if item.task_id != self.task_id
            || item.sender.into_bytes() == [0; 16]
            || item.delivered_at_epoch_ms == 0
            || item.message_id.trim().is_empty()
        {
            return Err(Error::Invalid(
                "inbox item has invalid task, sender, timestamp, or message identity".into(),
            ));
        }
        item.payload.validate()?;
        if let Some(existing) = self
            .items
            .iter()
            .find(|existing| {
                existing.sender == item.sender && existing.message_id == item.message_id
            })
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
    fn canonical_swarm_operation_cannot_bypass_budget_before_first_admission() -> Result<()> {
        let operation_id = id(71);
        let mut scheduler = Scheduler::new();
        let mut declaration = spec(operation_id, Orchestration::Leaf)?;
        declaration.resources = canonical_swarm_resources(SwarmResourceRequest {
            model_steps: 1,
            output_bytes: 1,
            execution_time_ms: 1,
        });
        scheduler.apply(SchedulerEvent::Declared {
            spec: Box::new(declaration),
        })?;
        assert!(matches!(
            scheduler.apply(SchedulerEvent::Admitted {
                operation_id,
                reservation: Reservation {
                    id: "forged-lease".into(),
                    placement: "worker".into(),
                    admitted: ResourceRequest::default(),
                },
            }),
            Err(Error::Conflict(_))
        ));

        let parent = id(72);
        let child = id(73);
        let mut descendant_scheduler = Scheduler::new();
        descendant_scheduler
            .apply(descendant_scheduler.declare(spec(parent, Orchestration::Join)?)?)?;
        let mut child_declaration = spec(child, Orchestration::Leaf)?;
        child_declaration.parent = Some(ParentLink {
            operation_id: parent,
            slot: "child".into(),
        });
        child_declaration.resources = canonical_swarm_resources(SwarmResourceRequest {
            model_steps: 1,
            output_bytes: 1,
            execution_time_ms: 1,
        });
        descendant_scheduler.apply(SchedulerEvent::Declared {
            spec: Box::new(child_declaration),
        })?;
        assert!(matches!(
            descendant_scheduler.apply(SchedulerEvent::Admitted {
                operation_id: parent,
                reservation: Reservation {
                    id: "forged-parent-lease".into(),
                    placement: "worker".into(),
                    admitted: ResourceRequest::default(),
                },
            }),
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

    #[test]
    fn root_usage_requires_a_running_scheduler_lease() -> Result<()> {
        let session = id(74);
        let owner = SwarmOwnerFence::new(session.to_string(), 0)?;
        let mut scheduler = Scheduler::new();
        scheduler.apply(scheduler.declare(spec(session, Orchestration::Leaf)?)?)?;
        assert!(matches!(
            scheduler.apply(SchedulerEvent::SwarmRootUsageReported {
                session_id: session,
                owner,
                fence: LeaseFence {
                    reservation_id: "root-lease".into(),
                    placement: "worker".into(),
                },
                usage: SwarmUsage {
                    model_steps: 1,
                    output_bytes: 1,
                    execution_time_ms: 1,
                },
                receipt: None,
                fingerprint: [0; 32],
            }),
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

    #[test]
    fn indeterminate_completion_retains_the_worker_reservation() -> Result<()> {
        let mut scheduler = Scheduler::new();
        scheduler.apply(scheduler.declare(spec(id(70), Orchestration::Leaf)?)?)?;
        let reservation = Reservation {
            id: "lease-70".into(),
            placement: "worker".into(),
            admitted: ResourceRequest::default(),
        };
        scheduler.apply(SchedulerEvent::Admitted {
            operation_id: id(70),
            reservation: reservation.clone(),
        })?;
        scheduler.apply(SchedulerEvent::Started {
            operation_id: id(70),
            fence: LeaseFence::from(&reservation),
        })?;
        scheduler.apply(SchedulerEvent::Completed {
            operation_id: id(70),
            outcome: Outcome::Indeterminate {
                operation_id: id(70),
            },
            fence: Some(LeaseFence::from(&reservation)),
            execution_duration_ns: None,
        })?;
        let state = scheduler
            .operation(id(70))
            .ok_or_else(|| Error::NotFound("indeterminate operation".into()))?;
        assert_eq!(state.phase, OperationPhase::Reconciling);
        assert_eq!(state.reservation, Some(reservation));
        Ok(())
    }

    #[test]
    fn swarm_compound_events_share_scheduler_projection_and_retain_unknown_capacity() -> Result<()>
    {
        let session = id(90);
        let child = id(91);
        let owner = SwarmOwnerFence::new(session.to_string(), 0)?;
        let limits = SwarmBudgetLimits {
            max_active_agents: 2,
            max_total_agents: 2,
            max_recursion_depth: 1,
            max_model_steps: 8,
            max_output_bytes: 128,
            max_execution_time_ms: 200,
        };
        let mut scheduler = Scheduler::new();
        scheduler.apply(scheduler.declare(spec(session, Orchestration::Leaf)?)?)?;
        let root_lease = Reservation {
            id: "lease-root".into(),
            placement: "worker".into(),
            admitted: ResourceRequest::default(),
        };
        scheduler.apply(SchedulerEvent::Admitted {
            operation_id: session,
            reservation: root_lease.clone(),
        })?;
        scheduler.apply(SchedulerEvent::Started {
            operation_id: session,
            fence: LeaseFence::from(&root_lease),
        })?;
        let mut child_spec = spec(child, Orchestration::Leaf)?;
        child_spec.resources = canonical_swarm_resources(SwarmResourceRequest {
            model_steps: 4,
            output_bytes: 64,
            execution_time_ms: 100,
        });
        scheduler.apply(SchedulerEvent::Declared {
            spec: Box::new(child_spec),
        })?;
        let request = SwarmForkRequest {
            operation_id: child,
            idempotency_key: crate::IdempotencyKey::new("swarm-child")?,
            parent_operation_id: None,
            depth: 1,
            resources: crate::swarm_budget::SwarmResourceRequest {
                model_steps: 4,
                output_bytes: 64,
                execution_time_ms: 100,
            },
            admission_digest: None,
        };
        let lease = Reservation {
            id: "lease-child".into(),
            placement: "worker".into(),
            admitted: canonical_swarm_resources(request.resources),
        };
        scheduler.apply(SchedulerEvent::SwarmAdmitted {
            operation_id: child,
            session_id: session,
            limits,
            owner: owner.clone(),
            request,
            admission_reference: spec(child, Orchestration::Leaf)?.state,
            reservation: lease,
        })?;
        assert_eq!(
            scheduler
                .swarm_budget_usage()?
                .expect("swarm usage")
                .active_agents,
            2
        );
        scheduler.apply(SchedulerEvent::SwarmDispatchStarted {
            operation_id: child,
            dispatch_id: crate::IdempotencyKey::new("dispatch-child")?,
            fence: LeaseFence {
                reservation_id: "lease-child".into(),
                placement: "worker".into(),
            },
            owner: owner.clone(),
            publication: ForkPublication {
                operation_id: child,
                parent_operation_id: None,
                completed_boundary_digest: [1; 32],
                workspace_generation_digest: [2; 32],
            },
        })?;
        let usage = SwarmUsage {
            model_steps: 1,
            output_bytes: 8,
            execution_time_ms: 10,
        };
        let completion = SchedulerEvent::SwarmCompleted {
            operation_id: child,
            fence: Some(LeaseFence {
                reservation_id: "lease-child".into(),
                placement: "worker".into(),
            }),
            owner,
            usage,
            receipt: Some(SwarmUsageReceipt::new(
                child,
                crate::IdempotencyKey::new("dispatch-child")?,
                1,
                usage,
                "provider",
            )?),
            outcome: Outcome::Indeterminate {
                operation_id: child,
            },
        };
        scheduler.apply(completion.clone())?;
        assert!(matches!(
            scheduler.apply(completion),
            Err(Error::Conflict(_))
        ));
        let usage = scheduler.swarm_budget_usage()?.expect("swarm usage");
        assert_eq!(usage.active_agents, 2);
        assert_eq!(usage.consumed.model_steps, 1);
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
        let alternate_sender = TaskId::from_bytes([5; 16]);
        let item = InboxItem {
            task_id,
            sender: task_id,
            delivered_at_epoch_ms: 1,
            sequence: 1,
            message_id: "message-1".into(),
            payload: state_ref()?,
        };
        let mut inbox = TaskInbox::new(task_id);
        inbox.apply(item.clone())?;
        inbox.apply(item)?;
        assert_eq!(inbox.after(0, 10).len(), 1);
        inbox.apply(InboxItem {
            task_id,
            sender: alternate_sender,
            delivered_at_epoch_ms: 2,
            sequence: 2,
            message_id: "message-1".into(),
            payload: state_ref()?,
        })?;
        assert_eq!(inbox.after(0, 10).len(), 2);
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
