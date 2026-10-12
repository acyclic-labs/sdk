//! Deterministic durable scheduling and structured orchestration semantics.

use crate::contract::{COMPONENT_LABEL_MAX_BYTES, next_revision};
use crate::{
    Error, OperationId, Outcome, Result, TaskId, conversation::FileRef, core::Authority,
    resources::CheckpointRef,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

mod clone_owned;
pub(crate) use clone_owned::json_value_owned_bytes;

/// Versioned resumable entrypoint for durable work.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct EntrypointRef {
    /// Stable namespaced entrypoint name.
    pub name: String,
    /// Semantic implementation version.
    pub version: String,
    /// Digest of the state/input schema and implementation contract.
    pub digest: [u8; 32],
    /// JSON Schema for the durable result value.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmModelJsonSchema"))]
    pub result_schema: Value,
}

/// Logical resource quantities; providers decide how they map to capacity.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(
    feature = "wasm",
    tsify(large_number_types_as_bigints, hashmap_as_object)
)]
pub struct ResourceRequest(pub BTreeMap<String, u64>);

/// Available logical resources advertised to admission policy.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(
    feature = "wasm",
    tsify(large_number_types_as_bigints, hashmap_as_object)
)]
pub struct ResourceSnapshot(pub BTreeMap<String, u64>);

/// Immutable ceilings for one root task and its complete descendant session.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionLimits {
    /// Maximum simultaneously owned execution reservations.
    pub active_tasks: u64,
    /// Maximum retained tasks, including the root and terminal tasks.
    pub total_tasks: u64,
    /// Maximum parent edges from root to descendant; zero permits only root.
    pub depth: u32,
    /// Maximum distinct admitted model attempts across the session.
    pub model_steps: u64,
}

impl SessionLimits {
    /// Rejects empty or inconsistent session ceilings.
    pub fn validate(&self) -> Result<()> {
        self.validate_with_work(&mut None)
    }

    pub(crate) fn validate_with_work(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        observe(work)?;
        if self.active_tasks == 0
            || self.total_tasks == 0
            || self.model_steps == 0
            || self.active_tasks > self.total_tasks
            || [self.active_tasks, self.total_tasks, self.model_steps]
                .iter()
                .any(|value| *value > crate::conversation::MAX_EXACT_JS_INTEGER)
        {
            return Err(diagnostic(work, crate::wire::ErrorCode::Invalid, "session limits are invalid"));
        }
        Ok(())
    }
}

/// Explicit lifetime owner for durable work.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
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
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ParentLink {
    /// Parent operation.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub operation_id: OperationId,
    /// Stable logical slot, independent of observation order.
    pub slot: String,
}

/// Durable orchestration behavior represented as data.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
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
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(hashmap_as_object))]
pub struct OperationSpec {
    /// Stable operation identity.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub operation_id: OperationId,
    /// Optional structured parent.
    #[cfg_attr(feature = "wasm", tsify(type = "ParentLink | null"))]
    pub parent: Option<ParentLink>,
    /// Explicit durable owner.
    pub owner: DurableOwner,
    /// Original signed owner grant retained with this immutable declaration.
    pub owner_scope: crate::core::Scope,
    /// Restartable implementation identity.
    pub entrypoint: EntrypointRef,
    /// Dependencies that must succeed before admission.
    #[cfg_attr(feature = "wasm", tsify(type = "string[]"))]
    pub dependencies: BTreeSet<OperationId>,
    /// Logical capacity requirements.
    pub resources: ResourceRequest,
    /// Small provider-neutral placement labels, not process or file content.
    pub placement: BTreeMap<String, String>,
    /// Orchestration behavior.
    pub orchestration: Orchestration,
    /// Immutable, provider-owned initial state bytes.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub state: FileRef,
}

impl OperationSpec {
    pub(crate) fn verify_owner(
        &self,
        owner: &Authority,
        verifier: &crate::core::AuthorityVerifier,
    ) -> Result<()> {
        self.verify_owner_with_work(owner,verifier,&mut None)
    }

    pub(crate) fn verify_owner_with_work(
        &self, owner: &Authority, verifier: &crate::core::AuthorityVerifier,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        if self.owner.authority() != owner {
            return Err(diagnostic(work, crate::wire::ErrorCode::Unauthorized, "declaration belongs to another owner"));
        }
        verifier.verify_audience(owner, work)?;
        verifier.verify_scope_for_with_work(owner,&self.owner_scope,work)?;
        if !self
            .owner_scope
            .capabilities()
            .contains("operation:declare")
        {
            return Err(diagnostic(work, crate::wire::ErrorCode::Unauthorized, "original owner scope lacks operation:declare"));
        }
        Ok(())
    }
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
    /// Task workflow is durably suspended without an execution reservation.
    Suspended,
    /// External completion requires reconciliation.
    Reconciling,
    /// Terminal result is known.
    Terminal,
}

/// Pinned resource allocation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
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
    /// Registered workflow revision and its one retained resume input.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow: Option<WorkflowSuspension>,
    /// Terminal result when known.
    pub outcome: Option<Outcome<FileRef>>,
    /// Whether cancellation was requested but not yet observed.
    pub cancellation_requested: bool,
    /// Per-operation fencing revision advanced by every committed mutation.
    pub revision: u64,
}

/// One durable workflow suspension/resume slot in the existing scheduler.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowSuspension {
    /// Exact committed workflow revision at which execution suspended.
    pub revision: u64,
    /// Exact outbox command whose passive wait released the reservation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting_command: Option<OperationId>,
    /// Immutable wake input, retained through admission and worker recovery.
    pub input: Option<FileRef>,
}

/// One deterministic scheduler transition.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SchedulerEvent {
    /// Pins one root session budget before descendants or execution exist.
    SessionConfigured {
        /// Existing root operation.
        operation_id: OperationId,
        /// Immutable session ceilings.
        limits: SessionLimits,
    },
    /// Charges one exact model request before its execution journal dispatch claim.
    ModelDispatchClaimed {
        /// Task owning this model request.
        operation_id: OperationId,
        /// Durable turn identity within the owning task.
        attempt_id: OperationId,
        /// Stable model step within that turn.
        step: u32,
        /// Exact immutable model request digest.
        request_digest: [u8; 32],
        /// Current execution owner.
        fence: LeaseFence,
        /// Admitted task ceiling, no greater than its session ceiling.
        ceiling: u64,
    },
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
    /// A committed registered workflow suspended and released its active slot.
    WorkflowSuspended {
        /// Owned task operation.
        operation_id: OperationId,
        /// Exact active reservation being released.
        fence: LeaseFence,
        /// Committed workflow revision, not a sandbox checkpoint reference.
        workflow_revision: u64,
        /// Selected passive wait in that committed workflow outbox.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        waiting_command: Option<OperationId>,
    },
    /// Owner-authorized input made one exact suspended workflow ready again.
    WorkflowResumed {
        /// Suspended task operation.
        operation_id: OperationId,
        /// Revision whose one resume slot this input fills.
        workflow_revision: u64,
        /// Immutable owner-verified input staged before publication.
        input: FileRef,
    },
    /// The same retained owner resumed its reconciliation work, without releasing capacity or admitting another worker.
    ReconciliationResumed {
        /// Operation with an indeterminate observation.
        operation_id: OperationId,
        /// Exact reservation still retained through uncertainty.
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
        /// Required whenever an execution reservation is still owned.
        fence: Option<LeaseFence>,
        /// Measured monotonic worker execution duration.
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct TaskModelClaims {
    ceiling: u64,
    attempts: BTreeMap<OperationId, BTreeMap<u32, [u8; 32]>>,
}

impl TaskModelClaims {
    fn used(&self) -> u64 {
        self.attempts.values().map(|steps| steps.len() as u64).sum()
    }
}

/// Pure reducer for dependency, capacity, ownership, and cancellation state.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Scheduler {
    operations: BTreeMap<OperationId, OperationState>,
    child_slots: BTreeMap<(OperationId, String), OperationId>,
    completion_order: Vec<OperationId>,
    #[serde(default)]
    sessions: BTreeMap<OperationId, SessionLimits>,
    #[serde(default)]
    model_claims: BTreeMap<OperationId, TaskModelClaims>,
    // LAST: cached SDK payloads above are physically dropped before their original native allocation guards, including after query IO is gone.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[serde(skip)]
    native_custody: NativeSchedulerCustody,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[derive(Debug, Default)]
struct NativeSchedulerCustody {
    operations: BTreeMap<OperationId, NativeOperationCustody>,
    completion: Option<acyclic_fs::NativeAllocation>,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[derive(Debug, Default)]
struct NativeOperationCustody {
    declaration: Option<acyclic_fs::NativeAllocation>,
    reservation: Option<acyclic_fs::NativeAllocation>,
    checkpoint: Option<acyclic_fs::NativeAllocation>,
    workflow: Option<acyclic_fs::NativeAllocation>,
    outcome: Option<acyclic_fs::NativeAllocation>,
    metadata: Option<acyclic_fs::NativeAllocation>,
}

// Ordinary projection clones remain the existing unobserved API.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl Clone for NativeSchedulerCustody {
    fn clone(&self) -> Self { Self::default() }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl PartialEq for NativeSchedulerCustody {
    fn eq(&self, _: &Self) -> bool { true }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
type SchedulerAllocation = acyclic_fs::NativeAllocation;
#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
type SchedulerAllocation = ();

struct PreparedSchedulerEvent {
    event: SchedulerEvent,
    child_slot: Option<String>,
    cancellation: Vec<OperationId>,
    observer_owned: u64,
    retained_owned: u64,
    // LAST: discarded event fields and temporary vectors drop first.
    custody: Option<SchedulerAllocation>,
}

#[derive(Clone, Copy)]
enum CachedSchedulerField { Declaration, Reservation, Checkpoint, Workflow, Outcome }

impl Scheduler {
    /// Creates an empty scheduler projection.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            operations: BTreeMap::new(),
            child_slots: BTreeMap::new(),
            completion_order: Vec::new(),
            sessions: BTreeMap::new(),
            model_claims: BTreeMap::new(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_custody: NativeSchedulerCustody { operations: BTreeMap::new(), completion: None },
        }
    }

    /// Returns one operation projection.
    #[must_use]
    pub fn operation(&self, id: OperationId) -> Option<&OperationState> {
        self.operations.get(&id)
    }

    /// Plans a declaration, including stable child-slot and cycle checks.
    pub fn declare(&self, spec: OperationSpec) -> Result<SchedulerEvent> {
        self.validate_declaration(&spec, &mut None)?;
        Ok(SchedulerEvent::Declared { spec: Box::new(spec) })
    }

    pub(crate) fn validate_session_with_work(
        &self, spec: &OperationSpec, limits: &SessionLimits,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.validate_declaration(spec, work)?;
        limits.validate_with_work(work)?;
        self.validate_session_configuration(
            spec.operation_id, spec.parent.is_some(), false, limits, work,
        )
    }

    fn validate_session_configuration(
        &self, operation_id: OperationId, has_parent: bool, dispatched: bool,
        limits: &SessionLimits,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if has_parent || dispatched || self.children(operation_id).next().is_some() {
            return Err(diagnostic(work, crate::wire::ErrorCode::Conflict,
                "session must be configured before descendants or dispatch"));
        }
        if self.sessions.get(&operation_id).is_some_and(|existing| existing != limits) {
            return Err(diagnostic(work, crate::wire::ErrorCode::Conflict,
                "session limits are already pinned"));
        }
        Ok(())
    }

    fn validate_declaration(
        &self, spec: &OperationSpec,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        use crate::wire::ErrorCode::{Conflict, Invalid};
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        observe_text(&spec.entrypoint.name, work)?;
        observe_text(&spec.entrypoint.version, work)?;
        if spec.entrypoint.name.trim().is_empty() || spec.entrypoint.version.trim().is_empty() {
            return Err(diagnostic(work, Invalid, "durable entrypoint name and version are required"));
        }
        spec.state.validate_with_work(work)?;
        for (key, value) in &spec.placement {
            observe_text(key, work)?;
            observe_text(value, work)?;
            if key.is_empty() || key.len() > COMPONENT_LABEL_MAX_BYTES
                || value.len() > COMPONENT_LABEL_MAX_BYTES
                || key.chars().any(char::is_control) || value.chars().any(char::is_control)
            {
                return Err(diagnostic(work, Invalid, "placement labels are invalid"));
            }
        }
        crate::contract::compile_json_schema_with_work(&spec.entrypoint.result_schema, "entrypoint result", work)?;
        if self.operations.contains_key(&spec.operation_id) {
            return Err(diagnostic(work, Conflict, "operation identity already exists"));
        }
        for dependency in &spec.dependencies {
            if self.depends_on(*dependency, spec.operation_id, work)? {
                return Err(diagnostic(work, Conflict, "operation dependency cycle"));
            }
        }
        if let Some(parent) = &spec.parent {
            observe_text(&parent.slot, work)?;
            if parent.slot.trim().is_empty() || parent.slot.len() > COMPONENT_LABEL_MAX_BYTES
                || parent.slot.chars().any(char::is_control)
            {
                return Err(diagnostic(work, Invalid, "structured child slot is invalid"));
            }
            let parent_operation = self.operation(parent.operation_id)
                .ok_or_else(|| diagnostic(work, Invalid, "structured child requires an existing parent and slot"))?;
            for ((operation, slot), _) in self.child_slots.range((parent.operation_id, String::new())..) {
                observe_text(slot, work)?;
                if *operation != parent.operation_id || slot > &parent.slot { break; }
                if slot == &parent.slot {
                    return Err(diagnostic(work, Conflict, "parent child slot is already bound"));
                }
            }
            if parent_operation.phase == OperationPhase::Terminal || parent_operation.cancellation_requested {
                return Err(diagnostic(work, Conflict, "terminal parent cannot accept children"));
            }
            let (root, depth) = self.session_root(parent.operation_id, work)?;
            if let Some(limits) = self.sessions.get(&root) {
                let mut count = 0_u64;
                for id in self.operations.keys() {
                    if self.session_root(*id, work)?.0 == root { count += 1; }
                }
                if depth >= limits.depth || count >= limits.total_tasks {
                    return Err(diagnostic(work, Invalid, "session task or depth limit exhausted"));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn operation_ready_for(
        &self,
        operation_id: OperationId,
        capacity: &ResourceSnapshot,
        labels: &BTreeMap<String, String>,
        owner: Option<&Authority>,
    ) -> bool {
        self.operations
            .get(&operation_id)
            .is_some_and(|operation| self.ready_operation(operation, capacity, labels, owner))
    }

    fn ready_operation(
        &self,
        operation: &OperationState,
        capacity: &ResourceSnapshot,
        labels: &BTreeMap<String, String>,
        owner: Option<&Authority>,
    ) -> bool {
        owner.is_none_or(|owner| operation.spec.owner.authority() == owner)
            && self.resources_ready(operation, capacity, labels)
            && self
                .require_session_capacity(operation.spec.operation_id, &mut None)
                .is_ok()
    }

    /// Iterates placement- and owner-matched operations that can acquire capacity.
    pub fn ready<'a>(
        &'a self,
        capacity: &'a ResourceSnapshot,
        labels: &'a BTreeMap<String, String>,
        owner: Option<&'a Authority>,
    ) -> impl Iterator<Item = OperationId> + 'a {
        self.operations
            .values()
            .filter(move |operation| self.ready_operation(operation, capacity, labels, owner))
            .map(|operation| operation.spec.operation_id)
    }

    fn resources_ready(
        &self,
        operation: &OperationState,
        capacity: &ResourceSnapshot,
        labels: &BTreeMap<String, String>,
    ) -> bool {
        matches!(
            operation.phase,
            OperationPhase::WaitingForDependencies | OperationPhase::WaitingForCapacity
        ) && !operation.cancellation_requested
            && self.dependencies_succeeded(operation)
            && operation
                .spec
                .placement
                .iter()
                .all(|(key, value)| labels.get(key) == Some(value))
            && remaining_resources_fit(operation, capacity)
    }

    /// Subtracts held placement resources on the optional observer.
    pub fn available_for(
        &self, placement: &str, advertised: &ResourceSnapshot,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<ResourceSnapshot> {
        // Observe the original borrowed subtraction inputs before any output String or logical map slot is allocated.
        for operation in self.operations.values() {
            observe(work)?;
            if let Some(reservation) = &operation.reservation {
                observe_text(&reservation.placement, work)?;
                if reservation.placement == placement {
                    for resource in reservation.admitted.0.keys() { observe_text(resource, work)?; }
                }
            }
        }
        let mut available = advertised.clone_with_work(work)?;
        for reservation in self.operations.values().filter_map(|operation| operation.reservation.as_ref())
            .filter(|reservation| reservation.placement == placement)
        {
            for (resource, quantity) in &reservation.admitted.0 {
                if let Some(remaining) = available.0.get_mut(resource) { *remaining = remaining.saturating_sub(*quantity); }
            }
        }
        Ok(available)
    }

    /// Applies one committed scheduler event through the shared validator.
    #[cfg_attr(not(target_arch = "wasm32"), tracing::instrument(
        name = "acyclic.harness.scheduler.apply", level = "debug", skip_all,
        fields(outcome = crate::obs::Empty, error.kind = crate::obs::Empty)))]
    pub fn apply(&mut self, event: SchedulerEvent) -> Result<()> {
        self.validate_event_with_work(&event, &mut None)?;
        let prepared = self.prepare_event(event, &mut None)?;
        self.apply_validated(prepared);
        crate::obs::outcome(Ok(()))
    }

    /// Borrows resident metadata, validates before copying, and transfers the cloned fields' original allocation guards into this durable cache.
    pub(crate) fn apply_with_work(
        &mut self, event: &SchedulerEvent,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.validate_event_with_work(event, work)?;
        if matches!(event, SchedulerEvent::Completed { .. })
            && self.operation(event.operation_id()).is_some_and(|operation| operation.phase == OperationPhase::Terminal)
        {
            return Ok(());
        }
        let cloned = event.clone_with_work(work)?;
        let prepared = self.prepare_event(cloned, work)?;
        let discarded = prepared.observer_owned;
        self.apply_validated(prepared);
        if let Some(io) = work.as_deref_mut() { io.release_owned(discarded)?; }
        crate::obs::outcome(Ok(()))
    }

    /// The original deterministic validators for every scheduler event.
    pub(crate) fn validate_event_with_work(
        &self, event: &SchedulerEvent,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        use crate::wire::ErrorCode::{Conflict, Invalid, Unauthorized, Unsupported};
        observe(work)?;
        let primary = event.operation_id();
        let declared_parent = match event {
            SchedulerEvent::Declared { spec } => spec.parent.as_ref().map(|parent| parent.operation_id),
            _ => None,
        };
        for id in std::iter::once(primary).chain(declared_parent) {
            if let Some(operation) = self.operation(id) { revision_with_work(operation.revision, work)?; }
        }
        if let SchedulerEvent::Orchestrated { cancel, .. } = event {
            for id in cancel {
                observe(work)?;
                if let Some(operation) = self.operation(*id) { revision_with_work(operation.revision, work)?; }
            }
        }
        match event {
            SchedulerEvent::Declared { spec } => self.validate_declaration(spec, work)?,
            SchedulerEvent::SessionConfigured { limits, .. } => {
                limits.validate_with_work(work)?;
                let operation = self.validated_operation(primary, work)?;
                self.validate_session_configuration(
                    primary, operation.spec.parent.is_some(),
                    operation.reservation.is_some() || operation.cancellation_requested
                        || !matches!(operation.phase, OperationPhase::WaitingForDependencies | OperationPhase::WaitingForCapacity),
                    limits, work,
                )?;
            }
            SchedulerEvent::ModelDispatchClaimed { attempt_id, step, request_digest, fence, ceiling, .. } => {
                let operation = self.validated_operation(primary, work)?;
                require_execution_owner(operation, fence, false, work)?;
                if *request_digest == [0; 32] || *ceiling == 0 {
                    return Err(diagnostic(work, Invalid, "model dispatch claim is invalid or cancelled"));
                }
                let (root, _) = self.session_root(primary, work)?;
                let limits = self.sessions.get(&root)
                    .ok_or_else(|| diagnostic(work, Unsupported, "session budget is not configured"))?;
                if *ceiling > limits.model_steps {
                    return Err(diagnostic(work, Unauthorized, "task ceiling exceeds session"));
                }
                let claims = self.model_claims.get(&primary);
                if claims.is_some_and(|claims| *ceiling != claims.ceiling) {
                    return Err(diagnostic(work, Unauthorized, "task model ceiling differs from its admission"));
                }
                if let Some(existing) = claims.and_then(|claims| claims.attempts.get(attempt_id)?.get(step)) {
                    if existing != request_digest {
                        return Err(diagnostic(work, Conflict, "model attempt changed request"));
                    }
                } else {
                    let mut used = 0_u64;
                    for (id, claims) in &self.model_claims {
                        observe(work)?;
                        if self.session_root(*id, work)?.0 == root {
                            used = used.checked_add(claims.used())
                                .ok_or_else(|| diagnostic(work, Invalid, "model step accounting overflows"))?;
                        }
                    }
                    if used >= limits.model_steps || claims.map_or(0, TaskModelClaims::used) >= *ceiling {
                        return Err(diagnostic(work, Invalid, "model step budget exhausted"));
                    }
                }
            }
            SchedulerEvent::WaitingForCapacity { .. } => {
                self.require_dependencies_ready(primary, work)?;
                require_phase(self.validated_operation(primary, work)?, OperationPhase::WaitingForDependencies, work)?;
            }
            SchedulerEvent::Admitted { reservation, .. } => {
                self.require_dependencies_ready(primary, work)?;
                self.require_session_capacity(primary, work)?;
                let operation = self.validated_operation(primary, work)?;
                if !matches!(operation.phase, OperationPhase::WaitingForDependencies | OperationPhase::WaitingForCapacity) {
                    return Err(diagnostic(work, Conflict, "operation cannot be admitted in its current phase"));
                }
                if operation.cancellation_requested {
                    return Err(diagnostic(work, Conflict, "cancelled operation cannot be admitted"));
                }
                if !covers(&reservation.admitted.0, &operation.spec.resources, work)? {
                    return Err(diagnostic(work, Conflict, "partial reservation cannot start an operation"));
                }
                if operation.reservation.as_ref().is_some_and(|partial| !same_reservation(partial, reservation)) {
                    return Err(diagnostic(work, Conflict, "partial admission must be completed by the same reservation"));
                }
            }
            SchedulerEvent::PartiallyAdmitted { reservation, .. } => {
                self.require_dependencies_ready(primary, work)?;
                self.require_session_capacity(primary, work)?;
                let operation = self.validated_operation(primary, work)?;
                if !matches!(operation.phase, OperationPhase::WaitingForDependencies | OperationPhase::WaitingForCapacity)
                    || operation.cancellation_requested
                {
                    return Err(diagnostic(work, Conflict, "operation cannot be partially admitted in its current phase"));
                }
                if !covers(&operation.spec.resources.0, &reservation.admitted, work)?
                    || covers(&reservation.admitted.0, &operation.spec.resources, work)?
                {
                    return Err(diagnostic(work, Invalid, "partial admission must be non-excessive and incomplete"));
                }
                if let Some(existing) = &operation.reservation
                    && (!same_reservation(existing, reservation)
                        || !covers(&reservation.admitted.0, &existing.admitted, work)?)
                {
                    return Err(diagnostic(work, Conflict, "partial reservation must advance the same lease"));
                }
            }
            SchedulerEvent::Rejected { reason, .. } => {
                observe_text(reason, work)?;
                if reason.trim().is_empty() {
                    return Err(diagnostic(work, Invalid, "rejection reason is empty"));
                }
                let operation = self.validated_operation(primary, work)?;
                if !matches!(operation.phase, OperationPhase::WaitingForDependencies | OperationPhase::WaitingForCapacity | OperationPhase::Admitted)
                    || operation.reservation.is_some()
                {
                    return Err(diagnostic(work, Conflict, "owned operation requires a fenced release"));
                }
            }
            SchedulerEvent::Started { fence, .. } => {
                let operation = self.validated_operation(primary, work)?;
                require_phase(operation, OperationPhase::Admitted, work)?;
                require_fence(operation, fence, work)?;
                if operation.cancellation_requested {
                    return Err(diagnostic(work, Conflict, "cancelled operation cannot start"));
                }
            }
            SchedulerEvent::Checkpointed { fence, .. } => {
                let operation = self.validated_operation(primary, work)?;
                if operation.phase != OperationPhase::Running {
                    return Err(diagnostic(work, Conflict, "operation cannot checkpoint in its current phase"));
                }
                require_fence(operation, fence, work)?;
            }
            SchedulerEvent::WaitingForChildren { fence, .. } => {
                let operation = self.validated_operation(primary, work)?;
                require_phase(operation, OperationPhase::Running, work)?;
                require_fence(operation, fence, work)?;
            }
            SchedulerEvent::WorkflowSuspended { fence, workflow_revision, .. } => {
                let operation = self.validated_operation(primary, work)?;
                require_execution_owner(operation, fence, false, work)?;
                if *workflow_revision == 0 || operation.workflow.as_ref().is_some_and(|previous| {
                    *workflow_revision < previous.revision
                        || (*workflow_revision == previous.revision && previous.input.is_none())
                }) {
                    return Err(diagnostic(work, Conflict, "workflow suspension revision did not advance"));
                }
            }
            SchedulerEvent::WorkflowResumed { workflow_revision, input, .. } => {
                input.validate_with_work(work)?;
                let operation = self.validated_operation(primary, work)?;
                require_phase(operation, OperationPhase::Suspended, work)?;
                if operation.cancellation_requested || operation.workflow.as_ref().is_none_or(|slot| {
                    slot.revision != *workflow_revision || slot.input.is_some()
                }) {
                    return Err(diagnostic(work, Conflict, "workflow resume slot is not available"));
                }
            }
            SchedulerEvent::ReconciliationResumed { fence, .. } => {
                let operation = self.validated_operation(primary, work)?;
                require_phase(operation, OperationPhase::Reconciling, work)?;
                require_fence(operation, fence, work)?;
                if operation.cancellation_requested {
                    return Err(diagnostic(work, Conflict, "cancelled reconciliation cannot start fresh work"));
                }
            }
            SchedulerEvent::LeaseReleased { fence, .. } => {
                let operation = self.validated_operation(primary, work)?;
                if !matches!(operation.phase, OperationPhase::WaitingForCapacity | OperationPhase::Admitted | OperationPhase::Running) {
                    return Err(diagnostic(work, Conflict, "only an active lease can be released"));
                }
                require_fence(operation, fence, work)?;
            }
            SchedulerEvent::CancellationRequested { recursive, .. } => {
                self.validated_operation(primary, work)?;
                for (id, operation) in &self.operations {
                    observe(work)?;
                    if *id == primary || (*recursive && self.cancellation_descendant(*id, primary, work)?) {
                        revision_with_work(operation.revision, work)?;
                    }
                }
            }
            SchedulerEvent::Completed { outcome, fence, execution_duration_ns, .. } => {
                if execution_duration_ns.is_some() && (fence.is_none() || matches!(outcome, Outcome::Indeterminate { .. })) {
                    return Err(diagnostic(work, Invalid, "worker duration requires a fenced terminal completion"));
                }
                if let Outcome::Succeeded(reference) = outcome { reference.validate_with_work(work)?; }
                let operation = self.validated_operation(primary, work)?;
                if operation.phase == OperationPhase::Terminal {
                    if operation.outcome.as_ref() == Some(outcome) && execution_duration_ns.is_none() && fence.is_none() {
                        return Ok(());
                    }
                    return Err(diagnostic(work, Conflict, "terminal operation has another outcome"));
                }
                if let Outcome::Indeterminate { operation_id } = outcome && *operation_id != primary {
                    return Err(diagnostic(work, Invalid, "indeterminate outcome references another operation"));
                }
                if !matches!(operation.phase, OperationPhase::Running | OperationPhase::Reconciling)
                    && !(operation.cancellation_requested && matches!(outcome, Outcome::Cancelled))
                {
                    return Err(diagnostic(work, Conflict, "operation cannot complete in its current phase"));
                }
                if operation.reservation.is_some() {
                    let fence = fence.as_ref().ok_or_else(|| diagnostic(work, Unauthorized, "worker completion requires a lease fence"))?;
                    require_fence(operation, fence, work)?;
                } else if fence.is_some() {
                    return Err(diagnostic(work, Conflict, "completion fence is not active"));
                }
            }
            SchedulerEvent::Orchestrated { expected_revision, outcome, cancel, reducer, reduction_digest, .. } => {
                if let Outcome::Succeeded(reference) = outcome { reference.validate_with_work(work)?; }
                let parent = self.validated_operation(primary, work)?;
                if parent.revision != *expected_revision || parent.phase != OperationPhase::WaitingForChildren
                    || parent.cancellation_requested
                {
                    return Err(diagnostic(work, Conflict, "stale orchestration decision"));
                }
                if !self.matches_orchestration(primary, outcome, cancel, reducer.as_ref(), *reduction_digest, work)? {
                    return Err(diagnostic(work, Conflict, "orchestration decision no longer matches"));
                }
            }
        }
        Ok(())
    }

    fn validated_operation(
        &self, id: OperationId, work: &Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<&OperationState> {
        self.operation(id).ok_or_else(|| diagnostic(work, crate::wire::ErrorCode::NotFound, "scheduler operation"))
    }

    fn prepare_event(
        &mut self, event: SchedulerEvent,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<PreparedSchedulerEvent> {
        let owned = if work.is_some() { event.logical_owned_bytes()? } else { 0 };
        let mut prepared = PreparedSchedulerEvent {
            event, child_slot: None, cancellation: Vec::new(), observer_owned: owned,
            retained_owned: 0, custody: None,
        };
        let result = (|| {
            prepared.retained_owned = prepared.event.retained_clone_owned_bytes()?;
            if let SchedulerEvent::Declared { spec } = &prepared.event {
                let slots = std::mem::size_of::<(OperationId, OperationState)>() as u64
                    + if spec.parent.is_some() { std::mem::size_of::<((OperationId, String), OperationId)>() as u64 } else { 0 };
                if let Some(io) = work.as_deref_mut() {
                    io.admit_allocation(slots)?;
                    io.retain_owned(slots)?;
                    prepared.observer_owned += slots;
                }
                prepared.retained_owned += slots;
            }
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if !self.native_custody.operations.contains_key(&prepared.event.operation_id())
                && (matches!(prepared.event, SchedulerEvent::Declared { .. }) || prepared.retained_owned != 0)
                && let Some(io) = work.as_deref_mut().filter(|io| io.original_native_work().is_some())
            {
                let bytes = std::mem::size_of::<(OperationId, NativeOperationCustody)>() as u64;
                io.admit_allocation(bytes)?;
                io.retain_owned(bytes)?;
                prepared.observer_owned += bytes;
                prepared.retained_owned += bytes;
            }
            if let SchedulerEvent::Declared { spec } = &prepared.event
                && let Some(parent) = &spec.parent
            {
                let slot = match work.as_deref_mut() {
                    Some(io) => io.clone_text(&parent.slot)?,
                    None => parent.slot.clone(),
                };
                prepared.observer_owned = prepared.observer_owned.checked_add(if work.is_some() { slot.capacity() as u64 } else { 0 })
                    .ok_or_else(|| diagnostic(work, crate::wire::ErrorCode::Invalid, "scheduler clone custody overflows"))?;
                prepared.child_slot = Some(slot);
                prepared.retained_owned += prepared.child_slot.as_ref().expect("prepared child slot").capacity() as u64;
            }
            if let SchedulerEvent::CancellationRequested { operation_id, recursive } = &prepared.event {
                prepared.cancellation = self.cancellation_frontier(*operation_id, *recursive, work)?;
                if work.is_some() {
                    prepared.observer_owned = prepared.observer_owned.checked_add(
                        (prepared.cancellation.capacity() * std::mem::size_of::<OperationId>()) as u64,
                    ).ok_or_else(|| diagnostic(work, crate::wire::ErrorCode::Invalid, "scheduler clone custody overflows"))?;
                }
            }
            self.reserve_completion_for_event(&prepared.event, &prepared.cancellation, work)?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(io) = work.as_deref_mut() && io.original_native_work().is_some() {
                prepared.custody = Some(io.take_native_custody(prepared.observer_owned)?);
                prepared.observer_owned = 0;
            }
            Ok(())
        })();
        if let Err(error) = result {
            let bytes = prepared.observer_owned;
            drop(prepared);
            if let Some(io) = work.as_deref_mut() { let _ = io.release_owned(bytes); }
            return Err(error);
        }
        // Non-native observer cleanup remains on its original observer.
        if let Some(io) = work.as_deref_mut() {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if prepared.custody.is_some() { return Ok(prepared); }
            prepared.observer_owned = prepared.observer_owned.checked_sub(prepared.retained_owned)
                .ok_or_else(|| diagnostic(&Some(&mut *io), crate::wire::ErrorCode::Invalid, "scheduler clone custody underflows"))?;
        }
        Ok(prepared)
    }

    /// Mutation only: every fallible validator, copy and capacity preparation has completed before this shared ordinary/native body is entered.
    fn apply_validated(&mut self, mut prepared: PreparedSchedulerEvent) {
        let primary = prepared.event.operation_id();
        let declared_parent = match &prepared.event {
            SchedulerEvent::Declared { spec } => spec.parent.as_ref().map(|parent| parent.operation_id),
            _ => None,
        };
        let event = prepared.event;
        match event {
            SchedulerEvent::SessionConfigured { limits, .. } => { self.sessions.insert(primary, limits); }
            SchedulerEvent::ModelDispatchClaimed { attempt_id, step, request_digest, ceiling, .. } => {
                self.model_claims.entry(primary).or_insert_with(|| TaskModelClaims {
                    ceiling, attempts: BTreeMap::new(),
                }).attempts.entry(attempt_id).or_default().insert(step, request_digest);
            }
            SchedulerEvent::Declared { spec } => {
                let spec = *spec;
                if let Some(parent) = &spec.parent {
                    self.child_slots.insert((parent.operation_id, prepared.child_slot.take().expect("prepared child slot")), primary);
                }
                self.operations.insert(primary, OperationState {
                    spec, phase: OperationPhase::WaitingForDependencies, reservation: None,
                    checkpoint: None, workflow: None, outcome: None, cancellation_requested: false, revision: 0,
                });
                self.install_native_field(primary, CachedSchedulerField::Declaration, &mut prepared.custody);
            }
            SchedulerEvent::WaitingForCapacity { .. } => {
                self.operations.get_mut(&primary).expect("validated operation").phase = OperationPhase::WaitingForCapacity;
            }
            SchedulerEvent::Admitted { reservation, .. } => {
                let operation = self.operations.get_mut(&primary).expect("validated operation");
                operation.reservation = Some(reservation);
                operation.phase = OperationPhase::Admitted;
                self.install_native_field(primary, CachedSchedulerField::Reservation, &mut prepared.custody);
            }
            SchedulerEvent::PartiallyAdmitted { reservation, .. } => {
                let operation = self.operations.get_mut(&primary).expect("validated operation");
                operation.reservation = Some(reservation);
                operation.phase = OperationPhase::WaitingForCapacity;
                self.install_native_field(primary, CachedSchedulerField::Reservation, &mut prepared.custody);
            }
            SchedulerEvent::Rejected { reason, .. } => {
                let operation = self.operations.get_mut(&primary).expect("validated operation");
                operation.reservation = None;
                operation.phase = OperationPhase::Terminal;
                operation.outcome = Some(Outcome::Failed { message: reason });
                self.completion_order.push(primary);
                self.install_native_field(primary, CachedSchedulerField::Outcome, &mut prepared.custody);
            }
            SchedulerEvent::Started { .. } => {
                self.operations.get_mut(&primary).expect("validated operation").phase = OperationPhase::Running;
            }
            SchedulerEvent::Checkpointed { checkpoint, .. } => {
                self.operations.get_mut(&primary).expect("validated operation").checkpoint = Some(checkpoint);
                self.install_native_field(primary, CachedSchedulerField::Checkpoint, &mut prepared.custody);
            }
            SchedulerEvent::WaitingForChildren { .. } | SchedulerEvent::LeaseReleased { .. } => {
                let waiting_for_children = matches!(event, SchedulerEvent::WaitingForChildren { .. });
                let operation = self.operations.get_mut(&primary).expect("validated operation");
                operation.reservation = None;
                if operation.cancellation_requested {
                    operation.phase = OperationPhase::Terminal;
                    operation.outcome = Some(Outcome::Cancelled);
                    self.completion_order.push(primary);
                } else {
                    operation.phase = if waiting_for_children { OperationPhase::WaitingForChildren } else { OperationPhase::WaitingForCapacity };
                }
                self.retire_native_fields(primary);
            }
            SchedulerEvent::WorkflowSuspended { workflow_revision, waiting_command, .. } => {
                let operation = self.operations.get_mut(&primary).expect("validated operation");
                operation.workflow = Some(WorkflowSuspension { revision: workflow_revision, waiting_command, input: None });
                operation.reservation = None;
                operation.phase = OperationPhase::Suspended;
                self.retire_native_fields(primary);
            }
            SchedulerEvent::WorkflowResumed { input, .. } => {
                let operation = self.operations.get_mut(&primary).expect("validated operation");
                operation.workflow.as_mut().expect("validated resume slot").input = Some(input);
                operation.phase = OperationPhase::WaitingForCapacity;
                self.install_native_field(primary, CachedSchedulerField::Workflow, &mut prepared.custody);
            }
            SchedulerEvent::ReconciliationResumed { .. } => {
                let operation = self.operations.get_mut(&primary).expect("validated operation");
                operation.phase = OperationPhase::Running;
                operation.outcome = None;
                self.retire_native_fields(primary);
            }
            SchedulerEvent::CancellationRequested { .. } => {
                for target in &prepared.cancellation { self.cancel_validated(*target); }
                return;
            }
            SchedulerEvent::Completed { outcome, .. } => {
                let operation = self.operations.get_mut(&primary).expect("validated operation");
                if operation.phase == OperationPhase::Terminal { return; }
                operation.phase = if matches!(outcome, Outcome::Indeterminate { .. }) {
                    OperationPhase::Reconciling
                } else {
                    operation.reservation = None;
                    OperationPhase::Terminal
                };
                operation.outcome = Some(outcome);
                if operation.phase == OperationPhase::Terminal && !self.completion_order.contains(&primary) {
                    self.completion_order.push(primary);
                }
                self.install_native_field(primary, CachedSchedulerField::Outcome, &mut prepared.custody);
                self.retire_native_fields(primary);
            }
            SchedulerEvent::Orchestrated { outcome, cancel, .. } => {
                for child in cancel { self.cancel_validated(child); }
                let operation = self.operations.get_mut(&primary).expect("validated operation");
                operation.phase = if matches!(outcome, Outcome::Indeterminate { .. }) { OperationPhase::Reconciling } else { OperationPhase::Terminal };
                operation.outcome = Some(outcome);
                operation.reservation = None;
                if operation.phase == OperationPhase::Terminal { self.completion_order.push(primary); }
                self.install_native_field(primary, CachedSchedulerField::Outcome, &mut prepared.custody);
                self.retire_native_fields(primary);
            }
        }
        self.operations.get_mut(&primary).expect("validated operation").revision += 1;
        if let Some(parent) = declared_parent { self.operations.get_mut(&parent).expect("validated parent").revision += 1; }
    }

    fn cancellation_descendant(
        &self, mut candidate: OperationId, root: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<bool> {
        let owner = self.operation(root).map(|operation| operation.spec.owner.authority());
        while candidate != root {
            observe(work)?;
            let Some(state) = self.operation(candidate) else { return Ok(false); };
            if Some(state.spec.owner.authority()) != owner { return Ok(false); }
            let Some(parent) = &state.spec.parent else { return Ok(false); };
            candidate = parent.operation_id;
        }
        Ok(true)
    }

    fn reserve_completion_for_event(
        &mut self, event: &SchedulerEvent, cancellation: &[OperationId],
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let operation = self.operation(event.operation_id());
        let mut extra = match event {
            SchedulerEvent::Rejected { .. } => 1,
            SchedulerEvent::WaitingForChildren { .. } | SchedulerEvent::LeaseReleased { .. } => {
                usize::from(operation.is_some_and(|operation| operation.cancellation_requested))
            }
            SchedulerEvent::Completed { outcome, .. } => usize::from(
                !matches!(outcome, Outcome::Indeterminate { .. })
                    && operation.is_some_and(|operation| operation.phase != OperationPhase::Terminal)
                    && !self.completion_order.contains(&event.operation_id())),
            SchedulerEvent::Orchestrated { outcome, .. } => usize::from(!matches!(outcome, Outcome::Indeterminate { .. })),
            _ => 0,
        };
        let cancel = match event {
            SchedulerEvent::CancellationRequested { .. } => cancellation,
            SchedulerEvent::Orchestrated { cancel, .. } => cancel.as_slice(),
            _ => &[],
        };
        for id in cancel {
            if self.operation(*id).is_some_and(cancellation_closes) { extra += 1; }
        }
        let target = self.completion_order.len().checked_add(extra)
            .ok_or_else(|| diagnostic(work, crate::wire::ErrorCode::Invalid, "scheduler completion slots overflow"))?;
        if target <= self.completion_order.capacity() { return Ok(()); }
        let bytes = target.checked_mul(std::mem::size_of::<OperationId>())
            .ok_or_else(|| diagnostic(work, crate::wire::ErrorCode::Invalid, "scheduler completion slots overflow"))? as u64;
        if let Some(io) = work.as_deref_mut() {
            io.admit_owned(bytes)?;
            io.admit_copy((self.completion_order.len() * std::mem::size_of::<OperationId>()) as u64)?;
            io.admit_allocation(bytes)?;
            io.retain_owned(bytes)?;
            if let Err(error) = io.copy_bytes((self.completion_order.len() * std::mem::size_of::<OperationId>()) as u64) {
                let _ = io.release_owned(bytes);
                return Err(error);
            }
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let custody = match work.as_deref_mut() {
            Some(io) if io.original_native_work().is_some() => Some(io.take_native_custody(bytes)?),
            _ => None,
        };
        let result = self.completion_order.try_reserve_exact(target - self.completion_order.len());
        if let Err(cause) = result {
            let error = match work.as_deref_mut() {
                Some(io) => io.allocation_failure(cause),
                None => Error::Storage(cause.to_string()),
            };
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if custody.is_some() { return Err(error); }
            if let Some(io) = work.as_deref_mut() { let _ = io.release_owned(bytes); }
            return Err(error);
        }
        // try_reserve has retired the old vector allocation before its old guard is replaced.
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        { self.native_custody.completion = custody; }
        Ok(())
    }

    fn install_native_field(
        &mut self, id: OperationId, field: CachedSchedulerField,
        incoming: &mut Option<SchedulerAllocation>,
    ) {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        {
            if incoming.is_none() && !self.native_custody.operations.contains_key(&id) { return; }
            let state = &self.operations[&id];
            let bytes = if incoming.is_some() { match field {
                CachedSchedulerField::Declaration => state.spec.logical_owned_bytes()
                    .expect("preflight declaration inventory")
                    + std::mem::size_of::<(OperationId, OperationState)>() as u64
                    + state.spec.parent.as_ref().map_or(0, |parent| parent.slot.len() as u64
                        + std::mem::size_of::<((OperationId, String), OperationId)>() as u64),
                CachedSchedulerField::Reservation => state.reservation.as_ref().expect("installed reservation")
                    .logical_owned_bytes().expect("preflight reservation inventory"),
                CachedSchedulerField::Checkpoint => state.checkpoint.as_ref().expect("installed checkpoint")
                    .logical_owned_bytes().expect("preflight checkpoint inventory"),
                CachedSchedulerField::Workflow => state.workflow.as_ref().expect("installed workflow")
                    .logical_owned_bytes().expect("preflight workflow inventory"),
                CachedSchedulerField::Outcome => state.outcome.as_ref().expect("installed outcome")
                    .logical_owned_bytes().expect("preflight outcome inventory"),
            } } else { 0 };
            if bytes == 0 && !self.native_custody.operations.contains_key(&id) { return; }
            let metadata = if !self.native_custody.operations.contains_key(&id) {
                incoming.as_mut().map(|guard| guard.split_off(
                    std::mem::size_of::<(OperationId, NativeOperationCustody)>() as u64,
                ).expect("exact preclaimed SDK guard-map tuple transfer"))
            } else { None };
            let guard = incoming.as_mut().map(|guard| guard.split_off(bytes)
                .expect("exact preclaimed payload transfer does not consult quota or cancellation"));
            if guard.is_none() && !self.native_custody.operations.contains_key(&id) { return; }
            let held = self.native_custody.operations.entry(id).or_default();
            if metadata.is_some() { held.metadata = metadata; }
            // Assignment physically dropped the replaced DTO before this corresponding old allocation guard is dropped.
            match field {
                CachedSchedulerField::Declaration => held.declaration = guard,
                CachedSchedulerField::Reservation => held.reservation = guard,
                CachedSchedulerField::Checkpoint => held.checkpoint = guard,
                CachedSchedulerField::Workflow => held.workflow = guard,
                CachedSchedulerField::Outcome => held.outcome = guard,
            }
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        { let _ = (id, field, incoming); }
    }

    fn retire_native_fields(&mut self, id: OperationId) {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(held) = self.native_custody.operations.get_mut(&id) {
            let state = &self.operations[&id];
            if state.reservation.is_none() { held.reservation = None; }
            if state.checkpoint.is_none() { held.checkpoint = None; }
            if state.workflow.as_ref().is_none_or(|workflow| workflow.input.is_none()) { held.workflow = None; }
            if state.outcome.as_ref().is_none_or(|outcome| matches!(outcome, Outcome::Cancelled | Outcome::Indeterminate { .. })) {
                held.outcome = None;
            }
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        { let _ = id; }
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

    /// Plans orchestration on the optional observer.
    pub fn orchestration(
        &self, parent: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<OrchestrationDecision> {
        let planned = self.inspect_orchestration(parent, work)?;
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut values = Vec::new();
        let mut cancel = Vec::new();
        let mut value_slots = 0;
        let mut cancel_slots = 0;
        let result = (|| {
            let (collect_values, collect_cancel) = match planned {
                BorrowedOrchestration::Wait => return Ok(OrchestrationDecision::Wait),
                BorrowedOrchestration::Complete { cancel, .. } => (false, cancel),
                BorrowedOrchestration::Assemble { cancel, .. } => (true, cancel),
                BorrowedOrchestration::Reduce { .. } => (true, false),
            };
            if collect_values {
                match planned {
                    BorrowedOrchestration::Assemble { assembly: AssemblyKind::Quorum, count, .. } => {
                        for (index, file) in self.successful_completions(parent).take(count).enumerate() {
                            let mut digits = [0_u8; 20];
                            let slot = decimal_slot(index, &mut digits);
                            let slot = clone_scheduler_text(slot, quota.work())?;
                            quota.adopt(slot.capacity() as u64)?;
                            let file = clone_owned::clone_file_with_work(file, quota.work())?;
                            quota.adopt(file.logical_owned_bytes()?)?;
                            push_scheduler_item(&mut values, (slot, file), &mut value_slots, quota.work())?;
                        }
                    }
                    _ => {
                        for (slot, child) in self.children(parent) {
                            let Some(Outcome::Succeeded(file)) = &child.outcome else { continue; };
                            let slot = clone_scheduler_text(slot, quota.work())?;
                            quota.adopt(slot.capacity() as u64)?;
                            let file = clone_owned::clone_file_with_work(file, quota.work())?;
                            quota.adopt(file.logical_owned_bytes()?)?;
                            push_scheduler_item(&mut values, (slot, file), &mut value_slots, quota.work())?;
                        }
                    }
                }
            }
            if collect_cancel {
                for (_, child) in self.children(parent) {
                    if child.phase != OperationPhase::Terminal {
                        push_scheduler_item(&mut cancel, child.spec.operation_id, &mut cancel_slots, quota.work())?;
                    }
                }
            }
            let decision = match planned {
                BorrowedOrchestration::Wait => unreachable!("wait returned before construction"),
                BorrowedOrchestration::Complete { outcome, .. } => {
                    let outcome = outcome.clone_with_work(quota.work())?;
                    quota.adopt(outcome.logical_owned_bytes()?)?;
                    OrchestrationDecision::Complete { outcome, cancel: std::mem::take(&mut cancel) }
                }
                BorrowedOrchestration::Assemble { assembly, .. } => OrchestrationDecision::Assemble {
                    assembly, values: std::mem::take(&mut values), cancel: std::mem::take(&mut cancel),
                },
                BorrowedOrchestration::Reduce { reducer } => {
                    let reducer = reducer.clone_with_work(quota.work())?;
                    quota.adopt(reducer.logical_owned_bytes()?)?;
                    OrchestrationDecision::Reduce { reducer, values: std::mem::take(&mut values) }
                }
            };
            Ok(decision)
        })();
        // Temporary vectors retire before their quota on every failed branch.
        if let Err(error) = result {
            drop(values);
            drop(cancel);
            if let Some(io) = quota.work().as_deref_mut() {
                let _ = io.release_owned(value_slots + cancel_slots);
            }
            return Err(error);
        }
        quota.adopt(value_slots + cancel_slots)?;
        quota.transfer();
        result
    }

    fn successful_completions(&self, parent: OperationId) -> impl Iterator<Item = &FileRef> {
        self.completion_order.iter().filter_map(move |completed| {
            self.children(parent).find(|(_, child)| child.spec.operation_id == *completed)
                .and_then(|(_, child)| match &child.outcome {
                    Some(Outcome::Succeeded(file)) => Some(file), _ => None,
                })
        })
    }

    fn inspect_orchestration(
        &self, parent: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<BorrowedOrchestration<'_>> {
        observe(work)?;
        let Some(state) = self.operation(parent) else { return Ok(BorrowedOrchestration::Wait); };
        if state.phase != OperationPhase::WaitingForChildren || state.cancellation_requested {
            return Ok(BorrowedOrchestration::Wait);
        }
        let mut count = 0;
        let mut terminal = true;
        let mut successful = true;
        let mut possible = 0;
        for (_, child) in self.children(parent) {
            observe(work)?;
            count += 1;
            let success = matches!(child.outcome, Some(Outcome::Succeeded(_)));
            terminal &= child.phase == OperationPhase::Terminal;
            successful &= success;
            possible += usize::from(child.phase != OperationPhase::Terminal || success);
        }
        if count == 0 { return Ok(BorrowedOrchestration::Wait); }
        match &state.spec.orchestration {
            Orchestration::Leaf => Ok(BorrowedOrchestration::Wait),
            Orchestration::Join if terminal => self.inspect_ordered(parent, work),
            Orchestration::Reduce { reducer } if terminal && successful => {
                Ok(BorrowedOrchestration::Reduce { reducer })
            }
            Orchestration::Reduce { .. } if terminal => self.inspect_ordered(parent, work),
            Orchestration::Race | Orchestration::Quorum { .. } => {
                let mut successes = 0;
                for completed in &self.completion_order {
                    observe(work)?;
                    for (_, child) in self.children(parent) {
                        observe(work)?;
                        if child.spec.operation_id == *completed {
                            if let Some(Outcome::Succeeded(file)) = &child.outcome {
                                if matches!(state.spec.orchestration, Orchestration::Race) {
                                    return Ok(BorrowedOrchestration::Complete {
                                        outcome: BorrowedOutcome::Succeeded(file), cancel: true,
                                    });
                                }
                                successes += 1;
                                if let Orchestration::Quorum { required } = state.spec.orchestration
                                    && successes >= required as usize
                                {
                                    return Ok(BorrowedOrchestration::Assemble {
                                        assembly: AssemblyKind::Quorum, count: required as usize, cancel: true,
                                    });
                                }
                            }
                            break;
                        }
                    }
                }
                match state.spec.orchestration {
                    Orchestration::Race if terminal => Ok(BorrowedOrchestration::Complete {
                        outcome: BorrowedOutcome::Failed("every raced child failed"), cancel: false,
                    }),
                    Orchestration::Quorum { required } if successes >= required as usize => {
                        Ok(BorrowedOrchestration::Assemble { assembly: AssemblyKind::Quorum,
                            count: required as usize, cancel: true })
                    }
                    Orchestration::Quorum { required } if possible < required as usize => {
                        Ok(BorrowedOrchestration::Complete {
                            outcome: BorrowedOutcome::Failed("quorum is no longer reachable"), cancel: false,
                        })
                    }
                    _ => Ok(BorrowedOrchestration::Wait),
                }
            }
            _ => Ok(BorrowedOrchestration::Wait),
        }
    }

    fn inspect_ordered(
        &self, parent: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<BorrowedOrchestration<'_>> {
        let mut count = 0;
        for (slot, child) in self.children(parent) {
            observe(work)?;
            match &child.outcome {
                Some(Outcome::Succeeded(_)) => { count += 1; }
                Some(Outcome::Failed { message }) => return Ok(BorrowedOrchestration::Complete {
                    outcome: BorrowedOutcome::ChildFailed { slot, message }, cancel: false,
                }),
                Some(Outcome::Cancelled) => return Ok(BorrowedOrchestration::Complete {
                    outcome: BorrowedOutcome::Cancelled, cancel: false,
                }),
                Some(Outcome::Indeterminate { .. }) => return Ok(BorrowedOrchestration::Complete {
                    outcome: BorrowedOutcome::Indeterminate(parent), cancel: false,
                }),
                None => return Ok(BorrowedOrchestration::Wait),
            }
        }
        Ok(BorrowedOrchestration::Assemble { assembly: AssemblyKind::Join, count, cancel: false })
    }

    fn matches_orchestration(
        &self, parent: OperationId, outcome: &Outcome<FileRef>, cancel: &[OperationId],
        reducer: Option<&EntrypointRef>, digest: Option<[u8; 32]>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<bool> {
        let planned = self.inspect_orchestration(parent, work)?;
        let cancellation_matches = |required| {
            if required {
                self.children(parent).filter(|(_, child)| child.phase != OperationPhase::Terminal)
                    .map(|(_, child)| child.spec.operation_id).eq(cancel.iter().copied())
            } else { cancel.is_empty() }
        };
        match planned {
            BorrowedOrchestration::Wait => Ok(false),
            BorrowedOrchestration::Complete { outcome: expected, cancel: required } => {
                expected.observe(work)?;
                Ok(expected.matches(outcome) && cancellation_matches(required) && reducer.is_none() && digest.is_none())
            }
            BorrowedOrchestration::Assemble { assembly, count, cancel: required } => {
                if !matches!(outcome, Outcome::Succeeded(_)) || !cancellation_matches(required) || reducer.is_some() {
                    return Ok(false);
                }
                let values = BorrowedSchedulerValues { scheduler: self, parent, quorum: assembly == AssemblyKind::Quorum, count };
                let expected = crate::contract::canonical_json_digest_with_work(
                    &(assembly, values), acyclic_stream::MAX_RECORD_BYTES as u64, work,
                )?;
                Ok(digest == Some(expected))
            }
            BorrowedOrchestration::Reduce { reducer: expected } => {
                if !cancel.is_empty() || reducer != Some(expected) || matches!(outcome, Outcome::Indeterminate { .. }) {
                    return Ok(false);
                }
                let values = BorrowedSchedulerValues { scheduler: self, parent, quorum: false, count: self.children(parent).count() };
                let expected = crate::contract::canonical_json_digest_with_work(
                    &(expected, values), acyclic_stream::MAX_RECORD_BYTES as u64, work,
                )?;
                Ok(digest == Some(expected))
            }
        }
    }

    /// Returns the stable cancellation propagation order.
    pub(crate) fn cancellation_frontier(
        &self, operation_id: OperationId, recursive: bool,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Vec<OperationId>> {
        let mut frontier = Vec::new();
        let mut held = 0_u64;
        let result = (|| {
            push_scheduler_item(&mut frontier, operation_id, &mut held, work)?;
            if recursive {
                let owner = self.operation(operation_id).map(|operation| operation.spec.owner.authority());
                let mut index = 0;
                while index < frontier.len() {
                    let parent = frontier[index];
                    for (_, child) in self.children(parent) {
                        observe(work)?;
                        if owner == Some(child.spec.owner.authority()) && !frontier.contains(&child.spec.operation_id) {
                            push_scheduler_item(&mut frontier, child.spec.operation_id, &mut held, work)?;
                        }
                    }
                    index += 1;
                }
            }
            Ok(())
        })();
        if let Err(error) = result {
            drop(frontier);
            if let Some(io) = work.as_deref_mut() { let _ = io.release_owned(held); }
            return Err(error);
        }
        Ok(frontier)
    }

    pub(crate) fn operation_blocked_by_dependencies(
        &self,
        operation_id: OperationId,
        owner: Option<&Authority>,
    ) -> bool {
        self.operations
            .get(&operation_id)
            .is_some_and(|operation| self.dependency_blocked(operation, owner))
    }

    fn dependency_blocked(&self, operation: &OperationState, owner: Option<&Authority>) -> bool {
        owner.is_none_or(|owner| operation.spec.owner.authority() == owner)
            && matches!(
                operation.phase,
                OperationPhase::WaitingForDependencies | OperationPhase::WaitingForCapacity
            )
            && operation.spec.dependencies.iter().any(|dependency| {
                self.operations.get(dependency).is_some_and(|state| {
                    state.phase == OperationPhase::Terminal
                        && !matches!(state.outcome, Some(Outcome::Succeeded(_)))
                })
            })
    }

    /// Iterates owner-matched operations with failed dependencies.
    pub fn blocked_by_dependencies<'a>(
        &'a self,
        owner: Option<&'a Authority>,
    ) -> impl Iterator<Item = OperationId> + 'a {
        self.operations
            .values()
            .filter(move |operation| self.dependency_blocked(operation, owner))
            .map(|operation| operation.spec.operation_id)
    }

    /// Returns the pinned session ceilings on the optional observer.
    pub fn session_limits(
        &self, operation_id: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<SessionLimits> {
        let (root, _) = self.session_root(operation_id, work)?;
        observe(work)?;
        self.sessions.get(&root).copied()
            .ok_or_else(|| diagnostic(work, crate::wire::ErrorCode::Unsupported, "session budget is not configured"))
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn require_model_claim(
        &self, task: OperationId, attempt: OperationId, step: u32, digest: &[u8; 32],
    ) -> Result<()> {
        self.require_model_claim_with_work(task, attempt, step, digest, &mut None)
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn require_model_claim_with_work(
        &self, task: OperationId, attempt: OperationId, step: u32, digest: &[u8; 32],
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let reason = "model dispatch lacks its exact shared budget claim";
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        observe(work)?;
        let Some(claims) = self.model_claims.get(&task) else {
            return Err(diagnostic(work, crate::wire::ErrorCode::Conflict, reason));
        };
        observe(work)?;
        let Some(attempt) = claims.attempts.get(&attempt) else {
            return Err(diagnostic(work, crate::wire::ErrorCode::Conflict, reason));
        };
        observe(work)?;
        if attempt.get(&step) == Some(digest) { Ok(()) }
        else { Err(diagnostic(work, crate::wire::ErrorCode::Conflict, reason)) }
    }

    pub(crate) fn session_root(
        &self, mut id: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(OperationId, u32)> {
        let mut depth = 0_u32;
        let mut traversed = 0_usize;
        loop {
            observe(work)?;
            let state = self.operation(id)
                .ok_or_else(|| diagnostic(work, crate::wire::ErrorCode::NotFound, "session task"))?;
            traversed += 1;
            if traversed > self.operations.len() {
                return Err(diagnostic(work, crate::wire::ErrorCode::Invalid, "session task lineage contains a cycle"));
            }
            let Some(parent) = &state.spec.parent else { return Ok((id, depth)); };
            id = parent.operation_id;
            depth = depth.checked_add(1)
                .ok_or_else(|| diagnostic(work, crate::wire::ErrorCode::Invalid, "task depth overflow"))?;
        }
    }

    fn require_session_capacity(
        &self, id: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let (root, _) = self.session_root(id, work)?;
        let Some(limits) = self.sessions.get(&root) else { return Ok(()); };
        if self.operation(id).is_some_and(|state| state.reservation.is_some()) { return Ok(()); }
        let mut owned = 0_u64;
        for (candidate, state) in &self.operations {
            observe(work)?;
            if state.reservation.is_some() && self.session_root(*candidate, work)?.0 == root { owned += 1; }
        }
        if owned >= limits.active_tasks {
            return Err(diagnostic(work, crate::wire::ErrorCode::Invalid, "session active task limit exhausted"));
        }
        Ok(())
    }

    /// Mutation counterpart of the shared cancellation preflight.
    fn cancel_validated(&mut self, id: OperationId) {
        let operation = self.operations.get_mut(&id).expect("validated cancellation target");
        if operation.phase == OperationPhase::Terminal { return; }
        let close = !matches!(operation.phase, OperationPhase::Running | OperationPhase::Reconciling)
            && operation.reservation.is_none();
        operation.revision += 1;
        if close {
            operation.reservation = None;
            operation.phase = OperationPhase::Terminal;
            operation.outcome = Some(Outcome::Cancelled);
            self.completion_order.push(id);
        } else {
            operation.cancellation_requested = true;
        }
        self.retire_native_fields(id);
    }

    fn require_dependencies_ready(
        &self, id: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let operation = self.validated_operation(id, work)?;
        for dependency in &operation.spec.dependencies {
            observe(work)?;
            if !matches!(self.operation(*dependency).and_then(|state| state.outcome.as_ref()), Some(Outcome::Succeeded(_))) {
                return Err(diagnostic(work, crate::wire::ErrorCode::Conflict, "operation dependencies are not ready"));
            }
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

    fn depends_on(
        &self, candidate: OperationId, target: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<bool> {
        observe(work)?;
        if candidate == target { return Ok(true); }
        if let Some(operation) = self.operation(candidate) {
            for dependency in &operation.spec.dependencies {
                if self.depends_on(*dependency, target, work)? { return Ok(true); }
            }
        }
        Ok(false)
    }
}

fn remaining_resources_fit(operation: &OperationState, capacity: &ResourceSnapshot) -> bool {
    operation
        .spec
        .resources
        .0
        .iter()
        .all(|(resource, requested)| {
            let admitted = operation
                .reservation
                .as_ref()
                .and_then(|reservation| reservation.admitted.0.get(resource))
                .copied()
                .unwrap_or(0);
            requested.saturating_sub(admitted) <= capacity.0.get(resource).copied().unwrap_or(0)
        })
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

#[derive(Clone, Copy)]
enum BorrowedOrchestration<'a> {
    Wait,
    Complete { outcome: BorrowedOutcome<'a>, cancel: bool },
    Assemble { assembly: AssemblyKind, count: usize, cancel: bool },
    Reduce { reducer: &'a EntrypointRef },
}

#[derive(Clone, Copy)]
enum BorrowedOutcome<'a> {
    Succeeded(&'a FileRef),
    Failed(&'a str),
    ChildFailed { slot: &'a str, message: &'a str },
    Cancelled,
    Indeterminate(OperationId),
}

impl BorrowedOutcome<'_> {
    fn matches(self, actual: &Outcome<FileRef>) -> bool {
        match (self, actual) {
            (Self::Succeeded(expected), Outcome::Succeeded(actual)) => expected == actual,
            (Self::Failed(expected), Outcome::Failed { message }) => expected == message,
            (Self::ChildFailed { slot, message: expected }, Outcome::Failed { message }) => {
                b"child ".iter().chain(slot.as_bytes()).chain(b" failed: ").chain(expected.as_bytes())
                    .copied().eq(message.bytes())
            }
            (Self::Cancelled, Outcome::Cancelled) => true,
            (Self::Indeterminate(expected), Outcome::Indeterminate { operation_id }) => expected == *operation_id,
            _ => false,
        }
    }

    fn observe(self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<()> {
        match self {
            Self::Failed(message) => observe_text(message, work),
            Self::ChildFailed { slot, message } => { observe_text(slot, work)?; observe_text(message, work) }
            _ => observe(work),
        }
    }

    fn clone_with_work(self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<Outcome<FileRef>> {
        match self {
            Self::Succeeded(file) => clone_owned::clone_file_with_work(file, work).map(Outcome::Succeeded),
            Self::Failed(message) => clone_scheduler_text(message, work).map(|message| Outcome::Failed { message }),
            Self::ChildFailed { slot, message } => {
                let length = slot.len().checked_add(message.len()).and_then(|bytes| bytes.checked_add(14))
                    .ok_or_else(|| diagnostic(work, crate::wire::ErrorCode::Invalid, "scheduler child failure length overflows"))?;
                let Some(io) = work.as_deref_mut() else {
                    return Ok(Outcome::Failed { message: format!("child {slot} failed: {message}") });
                };
                io.admit_copy(length as u64)?;
                let mut output = String::new();
                io.reserve_text(&mut output, length)?;
                let owned = output.capacity() as u64;
                if let Err(error) = io.copy_bytes(length as u64) {
                    drop(output);
                    let _ = io.release_owned(owned);
                    return Err(error);
                }
                output.push_str("child ");
                output.push_str(slot);
                output.push_str(" failed: ");
                output.push_str(message);
                Ok(Outcome::Failed { message: output })
            }
            Self::Cancelled => Ok(Outcome::Cancelled),
            Self::Indeterminate(operation_id) => Ok(Outcome::Indeterminate { operation_id }),
        }
    }
}

struct BorrowedSchedulerValues<'a> {
    scheduler: &'a Scheduler,
    parent: OperationId,
    quorum: bool,
    count: usize,
}

impl Serialize for BorrowedSchedulerValues<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq as _;
        let mut sequence = serializer.serialize_seq(Some(self.count))?;
        if self.quorum {
            for (index, file) in self.scheduler.successful_completions(self.parent).take(self.count).enumerate() {
                let mut digits = [0_u8; 20];
                let slot = decimal_slot(index, &mut digits);
                sequence.serialize_element(&(slot, file))?;
            }
        } else {
            for (slot, child) in self.scheduler.children(self.parent) {
                if let Some(Outcome::Succeeded(file)) = &child.outcome { sequence.serialize_element(&(slot, file))?; }
            }
        }
        sequence.end()
    }
}

fn decimal_slot(mut index: usize, digits: &mut [u8; 20]) -> &str {
    let mut start = digits.len();
    loop {
        start -= 1;
        digits[start] = b'0' + (index % 10) as u8;
        index /= 10;
        if index == 0 { break; }
    }
    std::str::from_utf8(&digits[start..]).expect("decimal slot contains only ASCII digits")
}

fn cancellation_closes(operation: &OperationState) -> bool {
    !matches!(operation.phase, OperationPhase::Terminal | OperationPhase::Running | OperationPhase::Reconciling)
        && operation.reservation.is_none()
}

fn clone_scheduler_text(text: &str, work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<String> {
    match work.as_deref_mut() { Some(io) => io.clone_text(text), None => Ok(text.to_owned()) }
}

fn push_scheduler_item<T>(
    values: &mut Vec<T>, value: T, held: &mut u64,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if values.len() == values.capacity() {
        let target = values.capacity().checked_mul(2).unwrap_or(usize::MAX).max(4);
        let bytes = target.checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| diagnostic(work, crate::wire::ErrorCode::Invalid, "scheduler vector slots overflow"))? as u64;
        if let Some(io) = work.as_deref_mut() {
            io.admit_owned(bytes)?;
            io.admit_copy((values.len() * std::mem::size_of::<T>()) as u64)?;
            io.admit_allocation(bytes)?;
            io.retain_owned(bytes)?;
        }
        if let Err(cause) = values.try_reserve_exact(target - values.len()) {
            if let Some(io) = work.as_deref_mut() {
                let error = io.allocation_failure(cause);
                let _ = io.release_owned(bytes);
                return Err(error);
            }
            return Err(Error::Storage(cause.to_string()));
        }
        let old = *held;
        *held = (values.capacity() * std::mem::size_of::<T>()) as u64;
        if let Some(io) = work.as_deref_mut() {
            // The old buffer has physically retired; release cannot consult cancellation or overwrite the original failed copy cause.
            io.release_owned(old)?;
            io.copy_bytes((values.len() * std::mem::size_of::<T>()) as u64)?;
        }
    }
    values.push(value);
    Ok(())
}

fn observe(work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<()> {
    if let Some(io) = work.as_mut() {
        io.check_cancellation()?;
        io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
    }
    Ok(())
}

fn observe_text(_text: &str, work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<()> {
    observe(work)
}

fn native_observer(work: &Option<&mut dyn crate::store::history::HistoryIo>) -> bool {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = work.as_ref() { return io.original_native_work().is_some(); }
    let _ = work;
    false
}

fn diagnostic(
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
    code: crate::wire::ErrorCode, reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if native_observer(work) { return Error::NativeBoundary { code, reason }; }
    let _ = work;
    match code {
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        crate::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
        crate::wire::ErrorCode::NotFound => Error::NotFound(reason.into()),
        crate::wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        _ => Error::Invalid(reason.into()),
    }
}

fn revision_with_work(revision: u64, work: &Option<&mut dyn crate::store::history::HistoryIo>) -> Result<u64> {
    if work.is_none() { return next_revision(revision); }
    revision.checked_add(1).ok_or_else(|| diagnostic(work, crate::wire::ErrorCode::Invalid, "revision exhausted"))
}

fn same_reservation(left: &Reservation, right: &Reservation) -> bool {
    left.id == right.id && left.placement == right.placement
}

fn covers(
    held: &BTreeMap<String, u64>, request: &ResourceRequest,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<bool> {
    for (resource, quantity) in &request.0 {
        observe_text(resource, work)?;
        if held.get(resource).unwrap_or(&0) < quantity { return Ok(false); }
    }
    Ok(true)
}

fn require_phase(
    operation: &OperationState, expected: OperationPhase,
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if operation.phase == expected { Ok(()) }
    else { Err(diagnostic(work, crate::wire::ErrorCode::Conflict, "operation phase differs from the required phase")) }
}

pub(crate) fn require_execution_owner(
    operation: &OperationState, fence: &LeaseFence, settlement: bool,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
    if settlement {
        if !matches!(operation.phase, OperationPhase::Running | OperationPhase::Reconciling) {
            return Err(diagnostic(work, crate::wire::ErrorCode::Conflict, "task has no execution owner"));
        }
    } else {
        require_phase(operation, OperationPhase::Running, work)?;
    }
    require_fence(operation, fence, work)?;
    if operation.cancellation_requested && !settlement {
        return Err(diagnostic(work, crate::wire::ErrorCode::Conflict, "cancelled task cannot publish new execution work"));
    }
    Ok(())
}

fn require_fence(
    operation: &OperationState, fence: &LeaseFence,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    let reservation = operation.reservation.as_ref()
        .ok_or_else(|| diagnostic(work, crate::wire::ErrorCode::Conflict, "operation has no active lease"))?;
    observe_text(&fence.reservation_id, work)?;
    observe_text(&fence.placement, work)?;
    if reservation.id != fence.reservation_id || reservation.placement != fence.placement {
        return Err(diagnostic(work, crate::wire::ErrorCode::Unauthorized, "stale or foreign lease fence"));
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
            Self::SessionConfigured { operation_id, .. }
            | Self::ModelDispatchClaimed { operation_id, .. }
            | Self::WorkflowSuspended { operation_id, .. }
            | Self::WorkflowResumed { operation_id, .. }
            | Self::ReconciliationResumed { operation_id, .. }
            | Self::WaitingForCapacity { operation_id }
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
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
pub struct InboxItem {
    /// Owning task.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub task_id: TaskId,
    /// Gapless one-based sequence.
    pub sequence: u64,
    /// Sender-defined idempotency identity.
    pub message_id: String,
    /// Immutable payload bytes staged before Stream publication.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
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
    #[cfg_attr(
        not(target_arch = "wasm32"),
        tracing::instrument(
            name = "acyclic.harness.scheduler.inbox_apply",
            level = "debug",
            skip_all,
            fields(items = self.items.len() as u64, outcome = crate::obs::Empty, error.kind = crate::obs::Empty)
        )
    )]
    pub fn apply(&mut self, item: InboxItem) -> Result<()> {
        crate::obs::outcome(self.apply_item(item))
    }

    fn apply_item(&mut self, item: InboxItem) -> Result<()> {
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
            owner_scope: crate::core::AuthorityIssuer::new(
                "scheduler-test",
                [9; 32],
                Authority {
                    kind: AggregateKind::Task,
                    id: operation_id.to_string(),
                },
            )
            .root(
                "original-declare",
                crate::Capabilities::new(["operation:declare"]),
            ),
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
        ModelClaim(u8, u8, u8, u8, u8),
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
            4 => (op(), lease(), 0..2_u8, 0..3_u8, 0..4_u8)
                .prop_map(|(op, lease, attempt, step, digest)|
                    Step::ModelClaim(op, lease, attempt, step, digest)),
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

    fn two_slot_parent(orchestration: Orchestration) -> Result<Scheduler> {
        let mut scheduler = Scheduler::new();
        scheduler.apply(scheduler.declare(spec(id(1), orchestration)?)?)?;
        for (op, slot) in [(2, "a"), (3, "b")] {
            let mut child = spec(id(op), Orchestration::Leaf)?;
            child.owner = spec(id(1), Orchestration::Leaf)?.owner;
            child.parent = Some(ParentLink {
                operation_id: id(1),
                slot: slot.into(),
            });
            scheduler.apply(scheduler.declare(child)?)?;
        }
        for op in [1, 2, 3] {
            scheduler.apply(SchedulerEvent::Admitted {
                operation_id: id(op),
                reservation: reservation(0, 0),
            })?;
            scheduler.apply(SchedulerEvent::Started {
                operation_id: id(op),
                fence: fence(0),
            })?;
        }
        scheduler.apply(SchedulerEvent::WaitingForChildren {
            operation_id: id(1),
            fence: fence(0),
        })?;
        Ok(scheduler)
    }

    #[test]
    fn join_slot_order_survives_reversed_completion_and_rejects_swapped_digest() -> Result<()> {
        let mut scheduler = two_slot_parent(Orchestration::Join)?;
        let a = result_ref(b"11")?;
        let b = result_ref(b"22")?;
        for (op, value) in [(3, b.clone()), (2, a.clone())] {
            scheduler.apply(SchedulerEvent::Completed {
                operation_id: id(op),
                outcome: Outcome::Succeeded(value),
                fence: Some(fence(0)),
                execution_duration_ns: None,
            })?;
            if op == 3 {
                assert_eq!(scheduler.orchestration(id(1), &mut None)?, OrchestrationDecision::Wait);
            }
        }
        let values = vec![("a".into(), a), ("b".into(), b)];
        assert_eq!(scheduler.completion_order, vec![id(3), id(2)]);
        assert_eq!(
            scheduler.orchestration(id(1), &mut None)?,
            OrchestrationDecision::Assemble {
                assembly: AssemblyKind::Join,
                values: values.clone(),
                cancel: Vec::new(),
            }
        );
        let digest = assembly_invocation_digest(AssemblyKind::Join, &values)?;
        let swapped = assembly_invocation_digest(
            AssemblyKind::Join,
            &[values[1].clone(), values[0].clone()],
        )?;
        assert_ne!(digest, swapped);
        let before = scheduler.clone();
        let mut publication = SchedulerEvent::Orchestrated {
            operation_id: id(1),
            expected_revision: scheduler
                .operation(id(1))
                .ok_or_else(|| Error::NotFound("parent".into()))?
                .revision,
            outcome: Outcome::Succeeded(result_ref(b"[11,22]")?),
            cancel: Vec::new(),
            reducer: None,
            reduction_digest: Some(swapped),
        };
        assert!(matches!(
            scheduler.apply(publication.clone()),
            Err(Error::Conflict(_))
        ));
        assert_eq!(scheduler, before);
        if let SchedulerEvent::Orchestrated {
            reduction_digest, ..
        } = &mut publication
        {
            *reduction_digest = Some(digest);
        }
        scheduler.apply(publication)?;
        let parent = scheduler
            .operation(id(1))
            .ok_or_else(|| Error::NotFound("parent".into()))?;
        assert_eq!(parent.phase, OperationPhase::Terminal);
        assert_eq!(
            parent.outcome,
            Some(Outcome::Succeeded(result_ref(b"[11,22]")?))
        );
        Ok(())
    }

    #[test]
    fn quorum_waits_for_running_child_then_fails_at_exact_parent_revision() -> Result<()> {
        let mut scheduler = two_slot_parent(Orchestration::Quorum { required: 2 })?;
        scheduler.apply(SchedulerEvent::Completed {
            operation_id: id(2),
            outcome: Outcome::Succeeded(result_ref(b"11")?),
            fence: Some(fence(0)),
            execution_duration_ns: None,
        })?;
        assert_eq!(
            scheduler.operation(id(3)).map(|child| child.phase),
            Some(OperationPhase::Running)
        );
        assert_eq!(scheduler.orchestration(id(1), &mut None)?, OrchestrationDecision::Wait);
        let failure = Outcome::Failed {
            message: "quorum is no longer reachable".into(),
        };
        let revision = scheduler
            .operation(id(1))
            .ok_or_else(|| Error::NotFound("parent".into()))?
            .revision;
        let publication = |expected_revision| SchedulerEvent::Orchestrated {
            operation_id: id(1),
            expected_revision,
            outcome: failure.clone(),
            cancel: Vec::new(),
            reducer: None,
            reduction_digest: None,
        };
        let before = scheduler.clone();
        assert!(matches!(
            scheduler.apply(publication(revision)),
            Err(Error::Conflict(_))
        ));
        assert_eq!(scheduler, before);
        scheduler.apply(SchedulerEvent::Completed {
            operation_id: id(3),
            outcome: Outcome::Failed {
                message: "child failed".into(),
            },
            fence: Some(fence(0)),
            execution_duration_ns: None,
        })?;
        assert_eq!(
            scheduler.orchestration(id(1), &mut None)?,
            OrchestrationDecision::Complete {
                outcome: failure.clone(),
                cancel: Vec::new(),
            }
        );
        assert_eq!(
            scheduler.operation(id(1)).map(|parent| parent.revision),
            Some(revision)
        );
        let before = scheduler.clone();
        assert!(matches!(
            scheduler.apply(publication(revision - 1)),
            Err(Error::Conflict(_))
        ));
        assert_eq!(scheduler, before);
        scheduler.apply(publication(revision))?;
        let parent = scheduler
            .operation(id(1))
            .ok_or_else(|| Error::NotFound("parent".into()))?;
        assert_eq!(parent.phase, OperationPhase::Terminal);
        assert_eq!(parent.outcome, Some(failure.clone()));
        assert_eq!(parent.revision, revision + 1);
        assert!(matches!(
            scheduler.apply(publication(revision)),
            Err(Error::Conflict(_))
        ));
        Ok(())
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
            Step::ModelClaim(op, lease, attempt, step, digest) => {
                SchedulerEvent::ModelDispatchClaimed {
                    operation_id: id(op),
                    attempt_id: id(attempt),
                    step: u32::from(step),
                    request_digest: [digest; 32],
                    fence: fence(lease),
                    ceiling: 2,
                }
            }
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
                    OperationPhase::Suspended | OperationPhase::Terminal => return Ok(None),
                };
                return event(scheduler, next);
            }
            Step::Orchestrate(op) => {
                let Some(parent) = scheduler.operation(id(op)) else {
                    return Ok(None);
                };
                let (outcome, cancel, reduction_digest) = match scheduler.orchestration(id(op), &mut None)? {
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
                if parent.is_none() {
                    scheduler.apply(SchedulerEvent::SessionConfigured {
                        operation_id: id(op),
                        limits: SessionLimits {
                            active_tasks: 2,
                            total_tasks: 5,
                            depth: 3,
                            model_steps: 2,
                        },
                    }).map_err(fail)?;
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
                            | OperationPhase::Reconciling
                    ));
                }
                for (op, prior) in &before.model_claims {
                    let current = scheduler.model_claims.get(op);
                    prop_assert!(current.is_some_and(|current|
                        current.ceiling == prior.ceiling
                            && prior.attempts.iter().all(|(attempt, steps)|
                                current.attempts.get(attempt).is_some_and(|retained|
                                    steps.iter().all(|(step, digest)|
                                        retained.get(step) == Some(digest))))));
                }
                for (root, limits) in &scheduler.sessions {
                    let used = scheduler.model_claims.iter()
                        .filter(|(op, _)| scheduler.session_root(**op, &mut None)
                            .is_ok_and(|(candidate, _)| candidate == *root))
                        .map(|(_, claims)| claims.used())
                        .sum::<u64>();
                    prop_assert!(used <= limits.model_steps);
                }
                for claims in scheduler.model_claims.values() {
                    prop_assert!(claims.used() <= claims.ceiling);
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

    fn check_model_claim_orderings(scheduler: &Scheduler, remaining: &[SchedulerEvent]) -> u64 {
        if remaining.is_empty() {
            return 1;
        }
        let used = |state: &Scheduler| {
            state
                .model_claims
                .values()
                .map(TaskModelClaims::used)
                .sum::<u64>()
        };
        let mut leaves = 0;
        for (index, event) in remaining.iter().enumerate() {
            let mut next = scheduler.clone();
            let accepted = next.apply(event.clone()).is_ok();
            if !accepted {
                assert_eq!(&next, scheduler, "rejected event mutated state: {event:?}");
            }
            if matches!(event, SchedulerEvent::LeaseReleased { .. }) {
                assert!(!accepted, "stale fence released ownership: {event:?}");
            }
            if let SchedulerEvent::ModelDispatchClaimed {
                operation_id,
                attempt_id,
                step,
                request_digest,
                ..
            } = event
            {
                if scheduler
                    .operation(*operation_id)
                    .is_some_and(|op| op.cancellation_requested)
                {
                    assert!(!accepted, "cancelled owner charged work: {event:?}");
                }
                if let Some(retained) = scheduler
                    .model_claims
                    .get(operation_id)
                    .and_then(|claims| claims.attempts.get(attempt_id))
                    .and_then(|steps| steps.get(step))
                    && retained != request_digest
                {
                    assert!(!accepted, "changed request accepted as retry: {event:?}");
                }
                if accepted {
                    let existing = scheduler
                        .model_claims
                        .get(operation_id)
                        .and_then(|claims| claims.attempts.get(attempt_id))
                        .is_some_and(|steps| steps.contains_key(step));
                    assert_eq!(
                        used(&next),
                        used(scheduler) + u64::from(!existing),
                        "exact retry changed accounting: {event:?}"
                    );
                }
            }
            assert!(used(&next) <= 2, "shared budget exceeded: {event:?}");
            for (op, prior) in &scheduler.model_claims {
                let current = next.model_claims.get(op).expect("claim refunded");
                assert_eq!(current.ceiling, prior.ceiling);
                for (attempt, steps) in &prior.attempts {
                    for (step, digest) in steps {
                        assert_eq!(
                            current
                                .attempts
                                .get(attempt)
                                .and_then(|retained| retained.get(step)),
                            Some(digest),
                            "retained request changed: {event:?}"
                        );
                    }
                }
            }
            // Cancellation and uncertainty cannot free either owned slot.
            for op in [id(2), id(3)] {
                assert_eq!(
                    next.operation(op).and_then(|op| op.reservation.as_ref()),
                    scheduler
                        .operation(op)
                        .and_then(|op| op.reservation.as_ref()),
                    "uncertainty released ownership: {event:?}"
                );
            }
            let mut pending = remaining.to_vec();
            pending.remove(index);
            leaves += check_model_claim_orderings(&next, &pending);
        }
        leaves
    }

    /// Bounded exhaustive checking of the production reducer, not a second transition implementation.
    #[test]
    fn all_model_claim_cancellation_and_uncertainty_orders_preserve_accounting() -> Result<()> {
        let mut scheduler = Scheduler::new();
        scheduler.apply(scheduler.declare(spec(id(1), Orchestration::Join)?)?)?;
        scheduler.apply(SchedulerEvent::SessionConfigured {
            operation_id: id(1),
            limits: SessionLimits {
                active_tasks: 2,
                total_tasks: 3,
                depth: 1,
                model_steps: 2,
            },
        })?;
        for (op, lease) in [(2, 0), (3, 1)] {
            let mut child = spec(id(op), Orchestration::Leaf)?;
            child.parent = Some(ParentLink {
                operation_id: id(1),
                slot: op.to_string(),
            });
            scheduler.apply(scheduler.declare(child)?)?;
            scheduler.apply(
                event(&scheduler, Step::Admit(op, lease, 0))?
                    .ok_or_else(|| Error::Invalid("model admission missing".into()))?,
            )?;
            scheduler.apply(
                event(&scheduler, Step::Start(op, lease))?
                    .ok_or_else(|| Error::Invalid("model start missing".into()))?,
            )?;
        }
        let actions = [
            Step::ModelClaim(2, 0, 0, 0, 1),
            Step::ModelClaim(2, 0, 0, 0, 1), // exact retry
            Step::ModelClaim(2, 0, 0, 0, 2), // changed request negative control
            Step::ModelClaim(3, 1, 0, 0, 1),
            Step::ModelClaim(2, 0, 1, 0, 1), // competing fresh attempt
            Step::Cancel(1, true),
            Step::Complete(2, 3, Some(0)), // uncertainty retains the lease
            Step::ReleaseLease(2, 1),      // another task's fence negative control
        ]
        .into_iter()
        .map(|step| {
            event(&scheduler, step)?.ok_or_else(|| Error::Invalid("model action missing".into()))
        })
        .collect::<Result<Vec<_>>>()?;
        assert_eq!(
            check_model_claim_orderings(&scheduler, &actions),
            (1..=actions.len() as u64).product::<u64>()
        );
        Ok(())
    }

    #[test]
    fn declarations_store_state_references_and_accept_long_placement_metadata() -> Result<()> {
        let scheduler = Scheduler::new();
        let event = scheduler.declare(spec(id(1), Orchestration::Leaf)?)?;
        let encoded =
            serde_json::to_value(&event).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(encoded["spec"]["state"]["path"], "state/initial.json");
        assert_eq!(encoded["spec"]["state"]["descriptor"]["byte_length"], 4);
        let mut oversized = spec(id(2), Orchestration::Leaf)?;
        oversized.placement.insert("region".into(), "x".repeat(256));
        assert!(scheduler.declare(oversized).is_ok());
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_scheduler_budget() -> acyclic_fs::WorkBudget {
        acyclic_fs::WorkBudget {
            items_examined: 1_048_576, bytes_copied: 16_777_216,
            bytes_encoded: 16_777_216, bytes_hashed: 16_777_216,
            allocation_operations: 65_536, peak_allocation_bytes: 16_777_216,
            ..Default::default()
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_apply_event(
        scheduler: &mut Scheduler, event: &SchedulerEvent,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let mut io = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        scheduler.apply_with_work(event, &mut Some(&mut io))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_phase_fence_capacity_quota_and_precancel_controls_leave_state_unchanged() -> Result<()> {
        let mut scheduler = Scheduler::new();
        scheduler.apply(SchedulerEvent::Declared { spec: Box::new(spec(id(82), Orchestration::Leaf)?) })?;
        scheduler.apply(SchedulerEvent::SessionConfigured { operation_id: id(82), limits: SessionLimits {
            active_tasks: 1, total_tasks: 2, depth: 1, model_steps: 2,
        } })?;
        let mut child = spec(id(83), Orchestration::Leaf)?;
        child.parent = Some(ParentLink { operation_id: id(82), slot: "capacity".into() });
        scheduler.apply(SchedulerEvent::Declared { spec: Box::new(child) })?;
        scheduler.apply(SchedulerEvent::Admitted { operation_id: id(82), reservation: reservation(4, 2) })?;
        scheduler.apply(SchedulerEvent::Started { operation_id: id(82), fence: fence(4) })?;
        let original = scheduler.clone();
        let controls = [
            SchedulerEvent::Started { operation_id: id(82), fence: fence(4) },
            SchedulerEvent::Checkpointed { operation_id: id(82),
                checkpoint: CheckpointRef::new(ProviderRef::new("example", "machines", "1")?, vec![1], None)?,
                fence: fence(5) },
            SchedulerEvent::LeaseReleased { operation_id: id(82), fence: fence(5) },
            SchedulerEvent::Admitted { operation_id: id(83), reservation: reservation(6, 1) },
            SchedulerEvent::WorkflowSuspended { operation_id: id(82), fence: fence(4), workflow_revision: 0, waiting_command: None },
            SchedulerEvent::Completed { operation_id: id(82), outcome: Outcome::Indeterminate { operation_id: id(83) },
                fence: Some(fence(4)), execution_duration_ns: None },
        ];
        for event in controls {
            assert!(original.clone().apply(event.clone()).is_err());
            let cancellation = acyclic_fs::CancellationToken::new();
            let work = acyclic_fs::NativeMaterializationWork::new(native_scheduler_budget(), &cancellation)
                .expect("original negative-control ledger");
            let error = native_apply_event(&mut scheduler, &event, &work).expect_err("original validator rejects");
            assert!(error.is_native_failure());
            assert_ne!(error.code(), crate::wire::ErrorCode::Unsupported);
            assert_eq!(scheduler, original);
            assert_eq!(work.receipt().allocation_operations, 1, "no DTO allocation after validation failure");
            assert_eq!(work.receipt().bytes_copied, 0);
        }
        let valid = SchedulerEvent::LeaseReleased { operation_id: id(82), fence: fence(4) };
        let cancellation = acyclic_fs::CancellationToken::new();
        let mut denied = native_scheduler_budget();
        denied.bytes_copied = 0;
        let work = acyclic_fs::NativeMaterializationWork::new(denied, &cancellation).expect("original denied-copy ledger");
        assert!(native_apply_event(&mut scheduler, &valid, &work).expect_err("fence clone denied").is_native_failure());
        assert_eq!(scheduler, original);
        assert_eq!(work.receipt().allocation_operations, 1);
        assert_eq!(work.receipt().bytes_copied, 0);
        let work = acyclic_fs::NativeMaterializationWork::new(native_scheduler_budget(), &cancellation)
            .expect("original pre-cancel ledger");
        let mut io = crate::store::history::native_work_io(&work).expect("original observer before cancellation");
        cancellation.cancel();
        let before = work.receipt();
        assert!(scheduler.apply_with_work(&valid, &mut Some(&mut io)).expect_err("pre-cancel").is_native_failure());
        assert_eq!(scheduler, original);
        assert_eq!(work.receipt(), before);
        Ok(())
    }

    #[test]
    fn prospective_session_validation_reuses_configured_root_policy() -> Result<()> {
        let limits = SessionLimits { active_tasks: 1, total_tasks: 2, depth: 1, model_steps: 2 };
        let scheduler = Scheduler::new();
        let root = spec(id(89), Orchestration::Leaf)?;
        scheduler.validate_session_with_work(&root, &limits, &mut None)?;
        let mut projected = scheduler;
        projected.apply(SchedulerEvent::Declared { spec: Box::new(root.clone()) })?;
        projected.apply(SchedulerEvent::SessionConfigured { operation_id: root.operation_id, limits })?;
        assert!(matches!(projected.validate_session_with_work(&root, &limits, &mut None),
            Err(Error::Conflict(_))));
        let mut child = spec(id(90), Orchestration::Leaf)?;
        child.parent = Some(ParentLink { operation_id: root.operation_id, slot: "child".into() });
        assert!(matches!(projected.validate_session_with_work(&child, &limits, &mut None),
            Err(Error::Conflict(_))));
        projected.apply(SchedulerEvent::Declared { spec: Box::new(child) })?;
        assert!(matches!(projected.apply(SchedulerEvent::SessionConfigured { operation_id: id(90), limits }),
            Err(Error::Conflict(_))));
        Ok(())
    }

    #[test]
    fn workflow_suspension_releases_shared_capacity_and_fences_wakes() -> Result<()> {
        let mut scheduler = Scheduler::new();
        let root = spec(id(91), Orchestration::Leaf)?;
        scheduler.apply(scheduler.declare(root.clone())?)?;
        let legacy = serde_json::to_value(scheduler.operation(id(91)))
            .map_err(|error| Error::Invalid(error.to_string()))?;
        assert!(legacy.get("workflow").is_none());
        let decoded: OperationState =
            serde_json::from_value(legacy).map_err(|error| Error::Invalid(error.to_string()))?;
        assert!(decoded.workflow.is_none());
        scheduler.apply(SchedulerEvent::SessionConfigured {
            operation_id: id(91),
            limits: SessionLimits {
                active_tasks: 1,
                total_tasks: 2,
                depth: 1,
                model_steps: 1,
            },
        })?;
        let mut child = spec(id(92), Orchestration::Leaf)?;
        child.owner = root.owner;
        child.parent = Some(ParentLink {
            operation_id: id(91),
            slot: "child".into(),
        });
        scheduler.apply(scheduler.declare(child)?)?;
        let reservation = Reservation {
            id: "first".into(),
            placement: "worker".into(),
            admitted: ResourceRequest::default(),
        };
        let first = LeaseFence::from(&reservation);
        scheduler.apply(SchedulerEvent::Admitted {
            operation_id: id(91),
            reservation,
        })?;
        scheduler.apply(SchedulerEvent::Started {
            operation_id: id(91),
            fence: first.clone(),
        })?;
        assert!(
            !scheduler
                .ready(&ResourceSnapshot::default(), &BTreeMap::new(), None).collect::<Vec<_>>()
                .contains(&id(92))
        );
        let stale = LeaseFence {
            reservation_id: "wrong".into(),
            placement: "worker".into(),
        };
        assert!(
            scheduler
                .apply(SchedulerEvent::WorkflowSuspended {
                    operation_id: id(91),
                    fence: stale,
                    workflow_revision: 1,
                    waiting_command: None,
                })
                .is_err()
        );
        assert!(
            scheduler
                .apply(SchedulerEvent::WorkflowSuspended {
                    operation_id: id(91),
                    fence: first.clone(),
                    workflow_revision: 0,
                    waiting_command: None,
                })
                .is_err()
        );
        scheduler.apply(SchedulerEvent::WorkflowSuspended {
            operation_id: id(91),
            fence: first.clone(),
            workflow_revision: 1,
            waiting_command: None,
        })?;
        let suspended = scheduler
            .operation(id(91))
            .ok_or_else(|| Error::NotFound("root".into()))?;
        assert_eq!(suspended.phase, OperationPhase::Suspended);
        assert!(suspended.reservation.is_none());
        assert_eq!(
            suspended.workflow,
            Some(WorkflowSuspension {
                revision: 1,
                waiting_command: None,
                input: None
            })
        );
        assert!(
            scheduler
                .ready(&ResourceSnapshot::default(), &BTreeMap::new(), None).collect::<Vec<_>>()
                .contains(&id(92))
        );
        assert!(
            !scheduler
                .ready(&ResourceSnapshot::default(), &BTreeMap::new(), None).collect::<Vec<_>>()
                .contains(&id(91))
        );
        let input = result_ref(b"7")?;
        assert!(
            scheduler
                .apply(SchedulerEvent::WorkflowResumed {
                    operation_id: id(91),
                    workflow_revision: 2,
                    input: input.clone()
                })
                .is_err()
        );
        scheduler.apply(SchedulerEvent::WorkflowResumed {
            operation_id: id(91),
            workflow_revision: 1,
            input: input.clone(),
        })?;
        assert!(
            scheduler
                .apply(SchedulerEvent::WorkflowResumed {
                    operation_id: id(91),
                    workflow_revision: 1,
                    input
                })
                .is_err()
        );
        let second_reservation = Reservation {
            id: "second".into(),
            placement: "worker".into(),
            admitted: ResourceRequest::default(),
        };
        let second = LeaseFence::from(&second_reservation);
        scheduler.apply(SchedulerEvent::Admitted {
            operation_id: id(91),
            reservation: second_reservation,
        })?;
        scheduler.apply(SchedulerEvent::Started {
            operation_id: id(91),
            fence: second.clone(),
        })?;
        assert!(
            scheduler
                .apply(SchedulerEvent::WorkflowSuspended {
                    operation_id: id(91),
                    fence: first,
                    workflow_revision: 2,
                    waiting_command: None,
                })
                .is_err()
        );
        let mut polled = scheduler.clone();
        polled.apply(SchedulerEvent::WorkflowSuspended {
            operation_id: id(91),
            fence: second.clone(),
            workflow_revision: 1,
            waiting_command: None,
        })?;
        assert!(
            polled
                .operation(id(91))
                .and_then(|state| state.workflow.as_ref())
                .is_some_and(|slot| slot.input.is_none())
        );
        scheduler.apply(SchedulerEvent::WorkflowSuspended {
            operation_id: id(91),
            fence: second,
            workflow_revision: 2,
            waiting_command: None,
        })?;
        assert!(
            scheduler
                .operation(id(91))
                .and_then(|state| state.workflow.as_ref())
                .is_some_and(|slot| slot.input.is_none() && slot.revision == 2)
        );
        scheduler.apply(SchedulerEvent::CancellationRequested {
            operation_id: id(91),
            recursive: true,
        })?;
        assert_eq!(
            scheduler.operation(id(91)).map(|state| &state.outcome),
            Some(&Some(Outcome::Cancelled))
        );
        assert!(
            scheduler
                .apply(SchedulerEvent::WorkflowResumed {
                    operation_id: id(91),
                    workflow_revision: 2,
                    input: result_ref(b"7")?
                })
                .is_err()
        );
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
            scheduler.orchestration(id(1), &mut None)?,
            OrchestrationDecision::Complete {
                outcome: Outcome::Succeeded(result_ref(b"\"winner\"")?),
                cancel: vec![id(2)],
            }
        );
        let expected_revision = scheduler
            .operation(id(1))
            .ok_or_else(|| Error::NotFound("parent".into()))?
            .revision;
        let mut suspended_loser = scheduler.clone();
        suspended_loser.apply(SchedulerEvent::WorkflowSuspended {
            operation_id: id(2),
            fence: LeaseFence {
                reservation_id: "lease-a".into(),
                placement: "worker".into(),
            },
            workflow_revision: 1,
            waiting_command: None,
        })?;
        suspended_loser.apply(SchedulerEvent::Orchestrated {
            operation_id: id(1),
            expected_revision,
            outcome: Outcome::Succeeded(result_ref(b"\"winner\"")?),
            cancel: vec![id(2)],
            reducer: None,
            reduction_digest: None,
        })?;
        assert!(
            suspended_loser
                .operation(id(2))
                .is_some_and(|state| state.phase == OperationPhase::Terminal
                    && state.outcome == Some(Outcome::Cancelled))
        );
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
    fn session_ceilings_are_shared_and_claims_survive_recovery() -> Result<()> {
        let mut scheduler = Scheduler::new();
        scheduler.apply(scheduler.declare(spec(id(40), Orchestration::Join)?)?)?;
        let limits = SessionLimits {
            active_tasks: 1,
            total_tasks: 3,
            depth: 1,
            model_steps: 2,
        };
        scheduler.apply(SchedulerEvent::SessionConfigured {
            operation_id: id(40),
            limits,
        })?;
        for index in [41, 42] {
            let mut child = spec(id(index), Orchestration::Leaf)?;
            child.parent = Some(ParentLink {
                operation_id: id(40),
                slot: index.to_string(),
            });
            let event = scheduler.declare(child)?;
            scheduler.apply(event)?;
            if index == 41 {
                let mut too_deep = spec(id(43), Orchestration::Leaf)?;
                too_deep.parent = Some(ParentLink {
                    operation_id: id(41),
                    slot: "too-deep".into(),
                });
                assert!(scheduler.declare(too_deep).is_err());
            }
        }
        let mut excess = spec(id(43), Orchestration::Leaf)?;
        excess.parent = Some(ParentLink {
            operation_id: id(40),
            slot: "excess".into(),
        });
        assert!(scheduler.declare(excess).is_err());
        let reservation = Reservation {
            id: "root-lease".into(),
            placement: "one".into(),
            admitted: ResourceRequest::default(),
        };
        let root_fence = LeaseFence::from(&reservation);
        scheduler.apply(SchedulerEvent::Admitted {
            operation_id: id(40),
            reservation,
        })?;
        scheduler.apply(SchedulerEvent::Started {
            operation_id: id(40),
            fence: root_fence.clone(),
        })?;
        assert!(scheduler.ready(&ResourceSnapshot::default(), &BTreeMap::new(), None).collect::<Vec<_>>().is_empty());
        let child_reservation = Reservation {
            id: "child-lease".into(),
            placement: "two".into(),
            admitted: ResourceRequest::default(),
        };
        assert!(
            scheduler
                .apply(SchedulerEvent::Admitted {
                    operation_id: id(41),
                    reservation: child_reservation.clone()
                })
                .is_err()
        );
        let claim = SchedulerEvent::ModelDispatchClaimed {
            operation_id: id(40),
            attempt_id: id(40),
            step: 0,
            request_digest: [1; 32],
            fence: root_fence.clone(),
            ceiling: 2,
        };
        scheduler.apply(claim.clone())?;
        scheduler.apply(claim)?;
        assert_eq!(
            scheduler
                .model_claims
                .get(&id(40))
                .map(TaskModelClaims::used),
            Some(1)
        );
        let before = scheduler.clone();
        assert!(
            scheduler
                .apply(SchedulerEvent::ModelDispatchClaimed {
                    operation_id: id(40),
                    attempt_id: id(40),
                    step: 0,
                    request_digest: [2; 32],
                    fence: root_fence.clone(),
                    ceiling: 2
                })
                .is_err()
        );
        assert_eq!(scheduler, before);
        scheduler.apply(SchedulerEvent::WaitingForChildren {
            operation_id: id(40),
            fence: root_fence,
        })?;
        let child_fence = LeaseFence::from(&child_reservation);
        scheduler.apply(SchedulerEvent::Admitted {
            operation_id: id(41),
            reservation: child_reservation,
        })?;
        scheduler.apply(SchedulerEvent::Started {
            operation_id: id(41),
            fence: child_fence.clone(),
        })?;
        scheduler.apply(SchedulerEvent::ModelDispatchClaimed {
            operation_id: id(41),
            attempt_id: id(41),
            step: 0,
            request_digest: [2; 32],
            fence: child_fence.clone(),
            ceiling: 2,
        })?;
        let before = scheduler.clone();
        assert!(
            scheduler
                .apply(SchedulerEvent::ModelDispatchClaimed {
                    operation_id: id(41),
                    attempt_id: id(42),
                    step: 0,
                    request_digest: [3; 32],
                    fence: child_fence.clone(),
                    ceiling: 2
                })
                .is_err()
        );
        assert_eq!(scheduler, before);
        scheduler.apply(SchedulerEvent::Completed {
            operation_id: id(41),
            outcome: Outcome::Indeterminate {
                operation_id: id(41),
            },
            fence: Some(child_fence),
            execution_duration_ns: None,
        })?;
        assert!(scheduler.ready(&ResourceSnapshot::default(), &BTreeMap::new(), None).collect::<Vec<_>>().is_empty());
        assert_eq!(
            scheduler
                .model_claims
                .values()
                .map(TaskModelClaims::used)
                .sum::<u64>(),
            2
        );
        Ok(())
    }

    #[test]
    fn cancellation_retains_owned_capacity_until_fenced_acknowledgement() -> Result<()> {
        for recursive in [false, true] {
            for partial in [false, true] {
                let mut scheduler = Scheduler::new();
                let mut parent = spec(id(30), Orchestration::Join)?;
                parent.resources = ResourceRequest(BTreeMap::from([("cpu".into(), 2)]));
                let owner = parent.owner.clone();
                scheduler.apply(scheduler.declare(parent)?)?;
                let target = if recursive { id(31) } else { id(30) };
                if recursive {
                    let mut child = spec(target, Orchestration::Leaf)?;
                    child.owner = owner;
                    child.parent = Some(ParentLink {
                        operation_id: id(30),
                        slot: "child".into(),
                    });
                    child.resources = ResourceRequest(BTreeMap::from([("cpu".into(), 2)]));
                    scheduler.apply(scheduler.declare(child)?)?;
                }
                let reservation = Reservation {
                    id: "owned".into(),
                    placement: "worker".into(),
                    admitted: ResourceRequest(BTreeMap::from([(
                        "cpu".into(),
                        if partial { 1 } else { 2 },
                    )])),
                };
                let fence = LeaseFence::from(&reservation);
                scheduler.apply(if partial {
                    SchedulerEvent::PartiallyAdmitted {
                        operation_id: target,
                        reservation: reservation.clone(),
                    }
                } else {
                    SchedulerEvent::Admitted {
                        operation_id: target,
                        reservation: reservation.clone(),
                    }
                })?;
                scheduler.apply(SchedulerEvent::CancellationRequested {
                    operation_id: id(30),
                    recursive,
                })?;
                assert_eq!(
                    scheduler
                        .operation(target)
                        .and_then(|state| state.reservation.as_ref()),
                    Some(&reservation)
                );
                assert!(
                    scheduler
                        .operation(target)
                        .is_some_and(|state| state.cancellation_requested)
                );
                assert!(
                    scheduler
                        .apply(SchedulerEvent::Rejected {
                            operation_id: target,
                            reason: "cancel".into()
                        })
                        .is_err()
                );
                assert!(
                    scheduler
                        .apply(SchedulerEvent::Completed {
                            operation_id: target,
                            outcome: Outcome::Cancelled,
                            fence: None,
                            execution_duration_ns: None,
                        })
                        .is_err()
                );
                assert!(
                    scheduler
                        .apply(SchedulerEvent::Started {
                            operation_id: target,
                            fence: fence.clone()
                        })
                        .is_err()
                );
                scheduler.apply(if partial {
                    SchedulerEvent::LeaseReleased {
                        operation_id: target,
                        fence,
                    }
                } else {
                    SchedulerEvent::Completed {
                        operation_id: target,
                        outcome: Outcome::Cancelled,
                        fence: Some(fence),
                        execution_duration_ns: None,
                    }
                })?;
                assert_eq!(
                    scheduler
                        .operation(target)
                        .and_then(|state| state.outcome.as_ref()),
                    Some(&Outcome::Cancelled)
                );
                assert!(
                    scheduler
                        .operation(target)
                        .is_some_and(|state| state.reservation.is_none())
                );
            }
        }
        Ok(())
    }

    #[test]
    fn single_pull_selection_preserves_partial_slots_and_advertised_resource_keys() -> Result<()> {
        let mut scheduler = Scheduler::new();
        scheduler.apply(scheduler.declare(spec(id(1), Orchestration::Join)?)?)?;
        scheduler.apply(SchedulerEvent::SessionConfigured {
            operation_id: id(1),
            limits: SessionLimits {
                active_tasks: 1,
                total_tasks: 2,
                depth: 1,
                model_steps: 1,
            },
        })?;
        let mut child = spec(id(2), Orchestration::Leaf)?;
        child.parent = Some(ParentLink {
            operation_id: id(1),
            slot: "child".into(),
        });
        child.resources = ResourceRequest(BTreeMap::from([("cpu".into(), 2)]));
        child.placement = BTreeMap::from([("zone".into(), "a".into())]);
        scheduler.apply(scheduler.declare(child)?)?;
        let parent = Reservation {
            id: "parent".into(),
            placement: "worker".into(),
            admitted: ResourceRequest::default(),
        };
        scheduler.apply(SchedulerEvent::Admitted {
            operation_id: id(1),
            reservation: parent.clone(),
        })?;
        scheduler.apply(SchedulerEvent::Started {
            operation_id: id(1),
            fence: (&parent).into(),
        })?;
        scheduler.apply(SchedulerEvent::WaitingForChildren {
            operation_id: id(1),
            fence: (&parent).into(),
        })?;
        let partial = Reservation {
            id: "partial".into(),
            placement: "worker".into(),
            admitted: ResourceRequest(BTreeMap::from([("cpu".into(), 1)])),
        };
        scheduler.apply(SchedulerEvent::PartiallyAdmitted {
            operation_id: id(2),
            reservation: partial,
        })?;
        let mut other = spec(id(3), Orchestration::Leaf)?;
        other.resources = ResourceRequest(BTreeMap::from([("cpu".into(), 1)]));
        scheduler.apply(scheduler.declare(other)?)?;
        // Existing reservations may name resources this worker no longer advertises.
        for value in 60..140 {
            let mut declaration = spec(id(value), Orchestration::Leaf)?;
            declaration.resources =
                ResourceRequest(BTreeMap::from([(format!("retired-{value}"), 1)]));
            let admitted = declaration.resources.clone();
            scheduler.apply(scheduler.declare(declaration)?)?;
            scheduler.apply(SchedulerEvent::Admitted {
                operation_id: id(value),
                reservation: Reservation {
                    id: format!("held-{value}"),
                    placement: "worker".into(),
                    admitted,
                },
            })?;
        }
        let advertised = ResourceSnapshot(BTreeMap::from([("cpu".into(), 2)]));
        let available = scheduler.available_for("worker", &advertised, &mut None)?;
        assert_eq!(
            available,
            ResourceSnapshot(BTreeMap::from([("cpu".into(), 1)]))
        );
        let labels = BTreeMap::from([("zone".into(), "a".into())]);
        // Partial ownership already occupies the session's sole slot.
        assert_eq!(
            scheduler.ready(&available, &labels, None).next(),
            Some(id(2))
        );
        assert_eq!(scheduler.ready(&available, &labels, None).collect::<Vec<_>>(), vec![id(2), id(3)]);
        assert_eq!(
            scheduler.ready(&available, &BTreeMap::new(), None).next(),
            Some(id(3))
        );
        let less = ResourceSnapshot(BTreeMap::from([("cpu".into(), 0)]));
        assert_eq!(scheduler.ready(&less, &labels, None).next(), None);
        Ok(())
    }

    #[test]
    fn uncertain_execution_retains_capacity_and_rejects_stale_owners() -> Result<()> {
        let mut scheduler = Scheduler::new();
        let mut declaration = spec(id(32), Orchestration::Leaf)?;
        declaration.resources = ResourceRequest(BTreeMap::from([("cpu".into(), 1)]));
        scheduler.apply(scheduler.declare(declaration)?)?;
        let reservation = Reservation {
            id: "current".into(),
            placement: "worker".into(),
            admitted: ResourceRequest(BTreeMap::from([("cpu".into(), 1)])),
        };
        let fence = LeaseFence::from(&reservation);
        scheduler.apply(SchedulerEvent::Admitted {
            operation_id: id(32),
            reservation: reservation.clone(),
        })?;
        scheduler.apply(SchedulerEvent::Started {
            operation_id: id(32),
            fence: fence.clone(),
        })?;
        scheduler.apply(SchedulerEvent::Completed {
            operation_id: id(32),
            outcome: Outcome::Indeterminate {
                operation_id: id(32),
            },
            fence: Some(fence.clone()),
            execution_duration_ns: None,
        })?;
        assert_eq!(
            scheduler
                .operation(id(32))
                .and_then(|state| state.reservation.as_ref()),
            Some(&reservation)
        );
        let capacity = ResourceSnapshot(BTreeMap::from([("cpu".into(), 1)]));
        assert_eq!(
            scheduler.available_for("worker", &capacity, &mut None)?.0.get("cpu"),
            Some(&0)
        );
        let replayed: Scheduler = serde_json::from_value(
            serde_json::to_value(&scheduler).map_err(|error| Error::Invalid(error.to_string()))?,
        )
        .map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(replayed, scheduler);
        let mut resumed = scheduler.clone();
        assert!(
            resumed
                .apply(SchedulerEvent::ReconciliationResumed {
                    operation_id: id(32),
                    fence: LeaseFence {
                        reservation_id: "stale".into(),
                        placement: "worker".into()
                    },
                })
                .is_err()
        );
        resumed.apply(SchedulerEvent::ReconciliationResumed {
            operation_id: id(32),
            fence: fence.clone(),
        })?;
        assert_eq!(
            resumed.available_for("worker", &capacity, &mut None)?.0.get("cpu"),
            Some(&0)
        );
        assert!(
            resumed
                .ready(&capacity, &BTreeMap::new(), None).collect::<Vec<_>>()
                .iter()
                .all(|operation| *operation != id(32))
        );
        assert_eq!(
            resumed
                .operation(id(32))
                .and_then(|state| state.reservation.as_ref()),
            Some(&reservation)
        );
        scheduler.apply(SchedulerEvent::CancellationRequested {
            operation_id: id(32),
            recursive: true,
        })?;
        assert!(
            scheduler
                .apply(SchedulerEvent::ReconciliationResumed {
                    operation_id: id(32),
                    fence: fence.clone()
                })
                .is_err()
        );
        let before = scheduler.clone();
        for candidate in [
            None,
            Some(LeaseFence {
                reservation_id: "stale".into(),
                placement: "worker".into(),
            }),
        ] {
            assert!(
                scheduler
                    .apply(SchedulerEvent::Completed {
                        operation_id: id(32),
                        outcome: Outcome::Cancelled,
                        fence: candidate,
                        execution_duration_ns: None,
                    })
                    .is_err()
            );
            assert_eq!(scheduler, before);
        }
        assert!(
            scheduler
                .apply(SchedulerEvent::LeaseReleased {
                    operation_id: id(32),
                    fence: fence.clone()
                })
                .is_err()
        );
        assert_eq!(scheduler, before);
        scheduler.apply(SchedulerEvent::Completed {
            operation_id: id(32),
            outcome: Outcome::Cancelled,
            fence: Some(fence.clone()),
            execution_duration_ns: None,
        })?;
        assert_eq!(scheduler.available_for("worker", &capacity, &mut None)?, capacity);
        assert!(
            scheduler
                .apply(SchedulerEvent::Completed {
                    operation_id: id(32),
                    outcome: Outcome::Cancelled,
                    fence: Some(fence),
                    execution_duration_ns: None,
                })
                .is_err()
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
        let OrchestrationDecision::Reduce { values, .. } = scheduler.orchestration(id(11), &mut None)? else {
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
        assert_eq!(scheduler.blocked_by_dependencies(None).collect::<Vec<_>>(), vec![id(2)]);
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
            scheduler.ready(&ResourceSnapshot(BTreeMap::from([("cpu".into(), 2)])), &BTreeMap::new(), None).collect::<Vec<_>>(),
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
