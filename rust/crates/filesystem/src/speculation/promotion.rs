//! Capability-aware planning for speculative movement between storage locations.

use super::{ResidencyPermit, ResidencyReason};
use crate::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::foundation::{GenerationId, OperationId, VolumeId};
use crate::performance::{WorkBudget, WorkCounters, WorkError};
use crate::storage::{
    ObjectFailure, ObjectId, ObjectReadRequest, ObjectReadRetention, ObjectReceipt, ObjectResult,
    ObjectStoreError,
};
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use thiserror::Error;

const BASIS_POINTS: u128 = 10_000;

/// Opaque identity of one caller-configured storage location.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StorageLocationId([u8; 16]);

impl StorageLocationId {
    /// Constructs a location identity from canonical bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns canonical location bytes.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 16] {
        self.0
    }
}

/// Generic storage class. It does not imply a cloud, machine, or provider.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum StorageTier {
    /// Process-local disposable acceleration.
    ProcessMemory,
    /// Node-local durable or disposable object acceleration.
    NodeLocal,
    /// Shared low-latency cache selected by a downstream control plane.
    SharedCache,
    /// Durable origin capable of restoring the object after cache loss.
    DurableOrigin,
}

/// Caller-observed exact object residency at one opaque location.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectResidency {
    /// Authenticated immutable object.
    pub object_id: ObjectId,
    /// Opaque configured location.
    pub location_id: StorageLocationId,
    /// Generic tier represented by that location.
    pub tier: StorageTier,
    /// Deterministic preference when selecting a readable source.
    pub source_priority: u16,
}

/// One available destination and its exact movement capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PromotionDestination {
    /// Opaque configured location.
    pub location_id: StorageLocationId,
    /// Generic tier represented by that location.
    pub tier: StorageTier,
    /// Whether the adapter currently admits immutable-object publication.
    pub writable: bool,
    /// Maximum complete canonical object accepted.
    pub maximum_object_bytes: u64,
    /// Deterministic preference within one requested tier.
    pub priority: u16,
    /// Conservative caller-supplied movement cost per canonical byte.
    pub cost_units_per_byte: u64,
}

/// Generation-bound request produced from a residency prediction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PromotionCandidate {
    /// Stable idempotency identity retained through execution and retry.
    pub operation_id: OperationId,
    /// Independently configured volume.
    pub volume_id: VolumeId,
    /// Immutable generation that motivated movement.
    pub generation_id: GenerationId,
    /// Exact authenticated object and complete-byte bound.
    pub request: ObjectReadRequest,
    /// Ordered destination tiers, most preferred first.
    pub accepted_tiers: Vec<StorageTier>,
    /// Original prediction source retained for policy and diagnostics.
    pub reason: ResidencyReason,
}

impl PromotionCandidate {
    /// Converts an admitted residency prediction into a promotion request.
    #[must_use]
    pub fn from_residency(permit: ResidencyPermit, accepted_tiers: Vec<StorageTier>) -> Self {
        let candidate = permit.candidate();
        Self {
            operation_id: candidate.operation_id,
            volume_id: candidate.volume_id,
            generation_id: candidate.generation_id,
            request: candidate.request,
            accepted_tiers,
            reason: candidate.reason,
        }
    }
}

/// One bounded idempotent movement selected from caller-supplied facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PromotionPlan {
    /// Complete generation-bound request.
    pub candidate: PromotionCandidate,
    /// Existing exact source selected deterministically.
    pub source: ObjectResidency,
    /// Writable destination selected deterministically.
    pub destination: PromotionDestination,
    /// Conservative maximum movement cost.
    pub estimated_cost_units: u64,
}

/// Hard bounds for one promotion planner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PromotionSpeculatorOptions {
    /// Maximum concurrently executing plans.
    pub maximum_active_operations: u32,
    /// Maximum canonical bytes across active plans.
    pub maximum_active_bytes: u64,
    /// Maximum estimated movement cost across active plans.
    pub maximum_active_cost_units: u64,
    /// Maximum residency facts accepted per candidate.
    pub maximum_residency_facts: u32,
    /// Maximum destination capabilities accepted per candidate.
    pub maximum_destinations: u32,
    /// Maximum ordered tiers accepted per candidate.
    pub maximum_accepted_tiers: u32,
    /// Number of recent terminal outcomes retained.
    pub outcome_window: u32,
    /// Outcomes required before adaptive rejection is enabled.
    pub minimum_usefulness_samples: u32,
    /// Minimum useful plans per 10,000 terminal plans.
    pub minimum_usefulness_basis_points: u16,
}

impl Default for PromotionSpeculatorOptions {
    fn default() -> Self {
        Self {
            maximum_active_operations: 4,
            maximum_active_bytes: 16 * 1024 * 1024,
            maximum_active_cost_units: u64::MAX,
            maximum_residency_facts: 64,
            maximum_destinations: 32,
            maximum_accepted_tiers: 4,
            outcome_window: 32,
            minimum_usefulness_samples: 4,
            minimum_usefulness_basis_points: 5_000,
        }
    }
}

impl PromotionSpeculatorOptions {
    fn validate(self) -> Result<Self, PromotionSpeculatorError> {
        if self.maximum_active_operations == 0
            || self.maximum_active_bytes == 0
            || self.maximum_active_cost_units == 0
            || self.maximum_residency_facts == 0
            || self.maximum_destinations == 0
            || self.maximum_accepted_tiers == 0
            || self.outcome_window == 0
            || self.minimum_usefulness_samples == 0
        {
            return Err(PromotionSpeculatorError::ZeroBound);
        }
        if self.minimum_usefulness_samples > self.outcome_window {
            return Err(PromotionSpeculatorError::UsefulnessWindowTooSmall);
        }
        if u128::from(self.minimum_usefulness_basis_points) > BASIS_POINTS {
            return Err(PromotionSpeculatorError::InvalidBasisPoints);
        }
        Ok(self)
    }
}

/// Stable promotion configuration, state, or feedback error.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum PromotionSpeculatorError {
    /// One hard bound is zero.
    #[error("promotion speculation bounds must be positive")]
    ZeroBound,
    /// Adaptive policy cannot reach its minimum sample count.
    #[error("promotion usefulness window is smaller than its minimum sample count")]
    UsefulnessWindowTooSmall,
    /// A configured ratio exceeds 10,000 basis points.
    #[error("promotion usefulness basis points exceed 10,000")]
    InvalidBasisPoints,
    /// Exact accounting cannot be represented.
    #[error("promotion speculation accounting overflowed")]
    Overflow,
    /// Terminal feedback did not identify an active plan.
    #[error("promotion speculation plan is not active")]
    UnknownPlan,
}

/// Typed reason why planning performed no movement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PromotionRejection {
    /// Candidate belongs to another volume.
    WrongVolume,
    /// Candidate belongs to a generation no longer selected.
    StaleGeneration,
    /// Request or accepted-tier list is empty, duplicated, or over bound.
    InvalidRequest,
    /// Residency or destination input exceeds its explicit bound.
    InputCapacity,
    /// No observed readable source contains the exact object.
    MissingSource,
    /// The same immutable object already has an active movement plan.
    DuplicateObject,
    /// The same idempotency identity already names an active movement plan.
    DuplicateOperation,
    /// Concurrent operation, byte, or estimated-cost capacity is exhausted.
    ActiveCapacity,
    /// No writable compatible destination can accept the complete object.
    NoDestination,
    /// Recent movement has not been useful enough.
    LowUsefulness,
}

/// Exact planning outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PromotionAdmission {
    /// An accepted tier already contains the exact object.
    Satisfied(ObjectResidency),
    /// A bounded idempotent movement may execute.
    Planned(PromotionPlan),
    /// No movement may begin.
    Rejected(PromotionRejection),
}

/// Cumulative and current payload-free promotion observations.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PromotionMetrics {
    /// Candidates evaluated.
    pub candidates: u64,
    /// Candidates already satisfied.
    pub satisfied: u64,
    /// Movement plans admitted.
    pub planned: u64,
    /// Active plans.
    pub active: u64,
    /// Canonical bytes represented by active plans.
    pub active_bytes: u64,
    /// Conservative cost represented by active plans.
    pub active_cost_units: u64,
    /// Plans consumed by later foreground demand.
    pub useful: u64,
    /// Plans failed, preempted, or expired unused.
    pub wasted: u64,
    /// Candidate rejections.
    pub rejected: u64,
}

/// Disposable capability-aware promotion planner for one volume generation.
#[derive(Clone)]
pub struct PromotionSpeculator {
    options: PromotionSpeculatorOptions,
    volume_id: VolumeId,
    generation_id: GenerationId,
    active: BTreeMap<OperationId, PromotionPlan>,
    outcomes: VecDeque<bool>,
    metrics: PromotionMetrics,
}

impl PromotionSpeculator {
    /// Creates one planner bound to an exact volume generation.
    ///
    /// # Errors
    ///
    /// Rejects zero, contradictory, or out-of-range policy bounds.
    pub fn new(
        options: PromotionSpeculatorOptions,
        volume_id: VolumeId,
        generation_id: GenerationId,
    ) -> Result<Self, PromotionSpeculatorError> {
        Ok(Self {
            options: options.validate()?,
            volume_id,
            generation_id,
            active: BTreeMap::new(),
            outcomes: VecDeque::new(),
            metrics: PromotionMetrics::default(),
        })
    }

    /// Selects at most one movement from bounded residency and capability facts.
    ///
    /// # Errors
    ///
    /// Fails closed if exact cost or metric accounting cannot be represented.
    pub fn plan(
        &mut self,
        candidate: PromotionCandidate,
        residency: &[ObjectResidency],
        destinations: &[PromotionDestination],
    ) -> Result<PromotionAdmission, PromotionSpeculatorError> {
        let mut next = self.clone();
        let admission = next.plan_in_place(candidate, residency, destinations)?;
        *self = next;
        Ok(admission)
    }

    fn plan_in_place(
        &mut self,
        candidate: PromotionCandidate,
        residency: &[ObjectResidency],
        destinations: &[PromotionDestination],
    ) -> Result<PromotionAdmission, PromotionSpeculatorError> {
        self.metrics.candidates = checked_increment(self.metrics.candidates)?;
        if let Some(rejection) = self.basic_rejection(&candidate, residency, destinations) {
            return self.reject(rejection);
        }
        if let Some(current) = best_satisfied(&candidate, residency) {
            self.metrics.satisfied = checked_increment(self.metrics.satisfied)?;
            return Ok(PromotionAdmission::Satisfied(current));
        }
        let Some(source) = best_source(candidate.request.object_id, residency) else {
            return self.reject(PromotionRejection::MissingSource);
        };
        let Some(destination) = best_destination(&candidate, source.location_id, destinations)
        else {
            return self.reject(PromotionRejection::NoDestination);
        };
        if !self.useful_enough() {
            return self.reject(PromotionRejection::LowUsefulness);
        }
        let estimated_cost_units = candidate
            .request
            .maximum_bytes
            .checked_mul(destination.cost_units_per_byte)
            .ok_or(PromotionSpeculatorError::Overflow)?;
        if self.active.len() >= self.options.maximum_active_operations as usize
            || self
                .metrics
                .active_bytes
                .checked_add(candidate.request.maximum_bytes)
                .is_none_or(|bytes| bytes > self.options.maximum_active_bytes)
            || self
                .metrics
                .active_cost_units
                .checked_add(estimated_cost_units)
                .is_none_or(|cost| cost > self.options.maximum_active_cost_units)
        {
            return self.reject(PromotionRejection::ActiveCapacity);
        }
        let plan = PromotionPlan {
            candidate,
            source,
            destination,
            estimated_cost_units,
        };
        self.metrics.planned = checked_increment(self.metrics.planned)?;
        self.metrics.active = checked_increment(self.metrics.active)?;
        self.metrics.active_bytes = self
            .metrics
            .active_bytes
            .checked_add(plan.candidate.request.maximum_bytes)
            .ok_or(PromotionSpeculatorError::Overflow)?;
        self.metrics.active_cost_units = self
            .metrics
            .active_cost_units
            .checked_add(plan.estimated_cost_units)
            .ok_or(PromotionSpeculatorError::Overflow)?;
        self.active
            .insert(plan.candidate.operation_id, plan.clone());
        Ok(PromotionAdmission::Planned(plan))
    }

    /// Completes one active plan exactly once.
    ///
    /// # Errors
    ///
    /// Rejects unknown or duplicate terminal feedback and accounting overflow.
    pub fn finish(
        &mut self,
        operation_id: OperationId,
        useful: bool,
    ) -> Result<(), PromotionSpeculatorError> {
        let mut next = self.clone();
        next.finish_in_place(operation_id, useful)?;
        *self = next;
        Ok(())
    }

    fn finish_in_place(
        &mut self,
        operation_id: OperationId,
        useful: bool,
    ) -> Result<(), PromotionSpeculatorError> {
        let plan = self
            .active
            .remove(&operation_id)
            .ok_or(PromotionSpeculatorError::UnknownPlan)?;
        self.metrics.active = self
            .metrics
            .active
            .checked_sub(1)
            .ok_or(PromotionSpeculatorError::Overflow)?;
        self.metrics.active_bytes = self
            .metrics
            .active_bytes
            .checked_sub(plan.candidate.request.maximum_bytes)
            .ok_or(PromotionSpeculatorError::Overflow)?;
        self.metrics.active_cost_units = self
            .metrics
            .active_cost_units
            .checked_sub(plan.estimated_cost_units)
            .ok_or(PromotionSpeculatorError::Overflow)?;
        if useful {
            self.metrics.useful = checked_increment(self.metrics.useful)?;
        } else {
            self.metrics.wasted = checked_increment(self.metrics.wasted)?;
        }
        self.outcomes.push_back(useful);
        while self.outcomes.len() > self.options.outcome_window as usize {
            self.outcomes.pop_front();
        }
        Ok(())
    }

    /// Preempts every active plan and returns executor cancellation identities.
    ///
    /// # Errors
    ///
    /// Fails closed if terminal accounting cannot be represented.
    pub fn preempt_for_foreground(&mut self) -> Result<Vec<OperationId>, PromotionSpeculatorError> {
        let mut next = self.clone();
        let operations = next.preempt_in_place()?;
        *self = next;
        Ok(operations)
    }

    fn preempt_in_place(&mut self) -> Result<Vec<OperationId>, PromotionSpeculatorError> {
        let operations: Vec<_> = self.active.keys().copied().collect();
        for operation_id in &operations {
            self.finish_in_place(*operation_id, false)?;
        }
        Ok(operations)
    }

    /// Fences all old plans and selects a new immutable generation.
    ///
    /// # Errors
    ///
    /// Fails closed if preemption accounting cannot be represented.
    pub fn replace_generation(
        &mut self,
        generation_id: GenerationId,
    ) -> Result<Vec<OperationId>, PromotionSpeculatorError> {
        let mut next = self.clone();
        let preempted = next.preempt_in_place()?;
        next.generation_id = generation_id;
        *self = next;
        Ok(preempted)
    }

    /// Returns exact current and cumulative metrics.
    #[must_use]
    pub const fn metrics(&self) -> PromotionMetrics {
        self.metrics
    }

    fn basic_rejection(
        &self,
        candidate: &PromotionCandidate,
        residency: &[ObjectResidency],
        destinations: &[PromotionDestination],
    ) -> Option<PromotionRejection> {
        if candidate.volume_id != self.volume_id {
            return Some(PromotionRejection::WrongVolume);
        }
        if candidate.generation_id != self.generation_id {
            return Some(PromotionRejection::StaleGeneration);
        }
        if candidate.request.maximum_bytes == 0
            || candidate.accepted_tiers.is_empty()
            || candidate.accepted_tiers.len() > self.options.maximum_accepted_tiers as usize
            || has_duplicate_tiers(&candidate.accepted_tiers)
            || residency
                .iter()
                .any(|fact| fact.object_id != candidate.request.object_id)
            || has_duplicate_residency_locations(residency)
            || has_duplicate_destination_locations(destinations)
        {
            return Some(PromotionRejection::InvalidRequest);
        }
        if residency.len() > self.options.maximum_residency_facts as usize
            || destinations.len() > self.options.maximum_destinations as usize
        {
            return Some(PromotionRejection::InputCapacity);
        }
        if self
            .active
            .values()
            .any(|plan| plan.candidate.request.object_id == candidate.request.object_id)
        {
            return Some(PromotionRejection::DuplicateObject);
        }
        if self.active.contains_key(&candidate.operation_id) {
            return Some(PromotionRejection::DuplicateOperation);
        }
        None
    }

    fn useful_enough(&self) -> bool {
        if self.outcomes.len() < self.options.minimum_usefulness_samples as usize {
            return true;
        }
        let useful = self.outcomes.iter().filter(|useful| **useful).count() as u128;
        useful * BASIS_POINTS
            >= self.outcomes.len() as u128
                * u128::from(self.options.minimum_usefulness_basis_points)
    }

    fn reject(
        &mut self,
        rejection: PromotionRejection,
    ) -> Result<PromotionAdmission, PromotionSpeculatorError> {
        self.metrics.rejected = checked_increment(self.metrics.rejected)?;
        Ok(PromotionAdmission::Rejected(rejection))
    }
}

/// Backend-specific execution boundary for one already selected plan.
pub trait PromotionExecutor {
    /// Copies, authenticates, and durably admits the exact object at the plan's
    /// destination, retaining the operation identity for idempotent retry.
    fn promote(
        &self,
        plan: &PromotionPlan,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = ObjectResult<()>>;
}

/// Canonical zero-copy-capable executor between two configured object stores.
///
/// The executor binds opaque plan locations to exact store handles. It reads
/// through the source's authenticated object contract and moves the returned
/// `Bytes` directly into idempotent destination admission. It never discovers
/// objects, chooses infrastructure, or retains a second body.
pub struct StorePromotionExecutor<S, D> {
    source_location_id: StorageLocationId,
    destination_location_id: StorageLocationId,
    source: S,
    destination: D,
}

impl<S, D> StorePromotionExecutor<S, D> {
    /// Binds two distinct opaque locations to their exact store handles.
    ///
    /// # Errors
    ///
    /// Rejects a self-copy configuration before either store can be accessed.
    pub fn new(
        source_location_id: StorageLocationId,
        source: S,
        destination_location_id: StorageLocationId,
        destination: D,
    ) -> Result<Self, StorePromotionExecutorError> {
        if source_location_id == destination_location_id {
            return Err(StorePromotionExecutorError::SameLocation);
        }
        Ok(Self {
            source_location_id,
            destination_location_id,
            source,
            destination,
        })
    }

    /// Returns the exact configured source handle.
    #[must_use]
    pub const fn source(&self) -> &S {
        &self.source
    }

    /// Returns the exact configured destination handle.
    #[must_use]
    pub const fn destination(&self) -> &D {
        &self.destination
    }
}

/// Invalid canonical store-promotion composition.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum StorePromotionExecutorError {
    /// Source and destination must identify distinct configured locations.
    #[error("promotion source and destination locations must differ")]
    SameLocation,
}

impl<S: AsyncObjectStore, D: AsyncObjectStore> PromotionExecutor for StorePromotionExecutor<S, D> {
    async fn promote(
        &self,
        plan: &PromotionPlan,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        validate_executor_plan(plan, self.source_location_id, self.destination_location_id)?;
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let request = plan.candidate.request;
        let read = self
            .source
            .read(
                request.object_id,
                request.maximum_bytes,
                budget,
                cancellation,
            )
            .await?;
        let live_bytes = match read.value.retention {
            ObjectReadRetention::Shared => 0,
            ObjectReadRetention::Owned { logical_bytes } => logical_bytes,
        };
        let mut remaining = read
            .work
            .remaining(budget)
            .map_err(|error| ObjectFailure::new(error.into(), read.work))?;
        remaining.peak_allocation_bytes = remaining
            .peak_allocation_bytes
            .checked_sub(live_bytes)
            .ok_or_else(|| {
            ObjectFailure::new(
                ObjectStoreError::Work(WorkError::BudgetExceeded {
                    counter: "peak_allocation_bytes",
                    observed: live_bytes,
                    maximum: budget.peak_allocation_bytes,
                }),
                read.work,
            )
        })?;
        let prior = read.work;
        let put = self
            .destination
            .put(request.object_id, read.value.bytes, remaining, cancellation)
            .await
            .map_err(|failure| merge_promotion_failure(prior, live_bytes, failure))?;
        let work = merge_promotion_work(prior, live_bytes, put.work)
            .map_err(|error| ObjectFailure::new(error.into(), prior))?;
        work.verify(budget)
            .map_err(|error| ObjectFailure::new(error.into(), work))?;
        Ok(ObjectReceipt { value: (), work })
    }
}

/// Executes one selected plan without changing planner or filesystem authority.
///
/// # Errors
///
/// Returns the executor's typed storage, cancellation, authentication, or work
/// failure together with all exact work completed before failure.
pub async fn execute_promotion<E: PromotionExecutor>(
    speculator: &PromotionSpeculator,
    executor: &E,
    plan: &PromotionPlan,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> ObjectResult<()> {
    if speculator.active.get(&plan.candidate.operation_id) != Some(plan) {
        return Err(crate::performance::OperationFailure::before_work(
            ObjectStoreError::Rejected(
                "promotion speculation plan is stale or inactive".to_owned(),
            ),
        ));
    }
    executor.promote(plan, budget, cancellation).await
}

fn validate_executor_plan(
    plan: &PromotionPlan,
    source_location_id: StorageLocationId,
    destination_location_id: StorageLocationId,
) -> ObjectResult<()> {
    if plan.source.location_id != source_location_id
        || plan.destination.location_id != destination_location_id
        || plan.source.object_id != plan.candidate.request.object_id
        || source_location_id == destination_location_id
    {
        return Err(ObjectFailure::before_work(ObjectStoreError::Rejected(
            "promotion plan does not match configured stores".to_owned(),
        )));
    }
    Ok(ObjectReceipt {
        value: (),
        work: WorkCounters::default(),
    })
}

fn merge_promotion_work(
    prior: WorkCounters,
    live_bytes: u64,
    nested: WorkCounters,
) -> Result<WorkCounters, WorkError> {
    let simultaneous_peak = live_bytes
        .checked_add(nested.peak_allocation_bytes)
        .ok_or(WorkError::Overflow)?;
    let mut combined = prior.checked_add(nested)?;
    combined.peak_allocation_bytes = combined.peak_allocation_bytes.max(simultaneous_peak);
    Ok(combined)
}

fn merge_promotion_failure(
    prior: WorkCounters,
    live_bytes: u64,
    failure: ObjectFailure,
) -> ObjectFailure {
    match merge_promotion_work(prior, live_bytes, *failure.work) {
        Ok(combined) => ObjectFailure::new(failure.error, combined),
        Err(error) => ObjectFailure::new(error.into(), prior),
    }
}

fn best_satisfied(
    candidate: &PromotionCandidate,
    residency: &[ObjectResidency],
) -> Option<ObjectResidency> {
    candidate.accepted_tiers.iter().find_map(|tier| {
        residency
            .iter()
            .filter(|fact| fact.object_id == candidate.request.object_id && fact.tier == *tier)
            .min_by_key(|fact| (fact.source_priority, fact.location_id))
            .copied()
    })
}

fn best_source(object_id: ObjectId, residency: &[ObjectResidency]) -> Option<ObjectResidency> {
    residency
        .iter()
        .filter(|fact| fact.object_id == object_id)
        .min_by_key(|fact| (fact.source_priority, fact.location_id))
        .copied()
}

fn best_destination(
    candidate: &PromotionCandidate,
    source_location: StorageLocationId,
    destinations: &[PromotionDestination],
) -> Option<PromotionDestination> {
    candidate.accepted_tiers.iter().find_map(|tier| {
        destinations
            .iter()
            .filter(|destination| {
                destination.tier == *tier
                    && destination.location_id != source_location
                    && destination.writable
                    && destination.maximum_object_bytes >= candidate.request.maximum_bytes
            })
            .min_by_key(|destination| (destination.priority, destination.location_id))
            .copied()
    })
}

fn has_duplicate_tiers(tiers: &[StorageTier]) -> bool {
    tiers
        .iter()
        .enumerate()
        .any(|(index, tier)| tiers[..index].contains(tier))
}

fn has_duplicate_residency_locations(residency: &[ObjectResidency]) -> bool {
    residency.iter().enumerate().any(|(index, fact)| {
        residency[..index]
            .iter()
            .any(|prior| prior.location_id == fact.location_id)
    })
}

fn has_duplicate_destination_locations(destinations: &[PromotionDestination]) -> bool {
    destinations.iter().enumerate().any(|(index, destination)| {
        destinations[..index]
            .iter()
            .any(|prior| prior.location_id == destination.location_id)
    })
}

fn checked_increment(value: u64) -> Result<u64, PromotionSpeculatorError> {
    value
        .checked_add(1)
        .ok_or(PromotionSpeculatorError::Overflow)
}

#[cfg(test)]
#[path = "tests/promotion.rs"]
mod tests;
