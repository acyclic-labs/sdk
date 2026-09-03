//! Generation-fenced prediction and admission for immutable-object residency.

use crate::async_storage::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::foundation::{GenerationId, OperationId, VolumeId};
use crate::performance::WorkBudget;
use crate::storage::{ObjectReadRequest, ObjectReceipt, ObjectResult};
use std::collections::{BTreeMap, VecDeque};
use thiserror::Error;

const BASIS_POINTS: u128 = 10_000;

/// Why a foreground filesystem operation predicts one immutable object.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResidencyReason {
    /// A bounded directory page exposed an exact successor frontier.
    DirectorySuccessor,
    /// Consecutive reads exposed the next immutable extent or blob page.
    SequentialRange,
    /// A metadata or file-table frontier exposed one likely sibling object.
    MetadataSuccessor,
    /// A downstream consumer supplied an object-exact semantic hint.
    ConsumerHint,
}

/// Object-exact opportunity emitted by an authenticated foreground access plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResidencyHint {
    /// Exact immutable object and maximum canonical bytes.
    pub request: ObjectReadRequest,
    /// Physical access pattern that exposed the opportunity.
    pub reason: ResidencyReason,
}

/// One object-exact, generation-bound prediction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResidencyCandidate {
    /// Stable identity used for admission, execution, and terminal feedback.
    pub operation_id: OperationId,
    /// Independently configured volume that owns the generation.
    pub volume_id: VolumeId,
    /// Immutable generation against which the prediction was derived.
    pub generation_id: GenerationId,
    /// Exact authenticated object and maximum admitted canonical bytes.
    pub request: ObjectReadRequest,
    /// Observable prediction source.
    pub reason: ResidencyReason,
}

impl ResidencyCandidate {
    /// Binds one authenticated hint to an exact volume generation and stable
    /// idempotency identity without changing the hinted object.
    #[must_use]
    pub const fn from_hint(
        operation_id: OperationId,
        volume_id: VolumeId,
        generation_id: GenerationId,
        hint: ResidencyHint,
    ) -> Self {
        Self {
            operation_id,
            volume_id,
            generation_id,
            request: hint.request,
            reason: hint.reason,
        }
    }
}

/// Hard bounds and adaptive admission policy for one residency engine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResidencySpeculatorOptions {
    /// Maximum concurrently admitted predictions.
    pub maximum_active_operations: u32,
    /// Maximum estimated bytes across active predictions.
    pub maximum_active_bytes: u64,
    /// Number of recent terminal outcomes retained for usefulness feedback.
    pub outcome_window: u32,
    /// Number of recent foreground/speculative byte samples retained.
    pub traffic_window: u32,
    /// Maximum speculative bytes per 10,000 observed foreground bytes.
    pub speculative_cost_basis_points: u16,
    /// Number of outcomes required before low usefulness may disable admission.
    pub minimum_usefulness_samples: u32,
    /// Minimum useful outcomes per 10,000 terminal outcomes.
    pub minimum_usefulness_basis_points: u16,
}

impl Default for ResidencySpeculatorOptions {
    fn default() -> Self {
        Self {
            maximum_active_operations: 4,
            maximum_active_bytes: 4 * 1024 * 1024,
            outcome_window: 32,
            traffic_window: 64,
            speculative_cost_basis_points: 1_000,
            minimum_usefulness_samples: 4,
            minimum_usefulness_basis_points: 5_000,
        }
    }
}

impl ResidencySpeculatorOptions {
    fn validate(self) -> Result<Self, ResidencySpeculatorError> {
        if self.maximum_active_operations == 0
            || self.maximum_active_bytes == 0
            || self.outcome_window == 0
            || self.traffic_window == 0
            || self.minimum_usefulness_samples == 0
        {
            return Err(ResidencySpeculatorError::ZeroBound);
        }
        if self.minimum_usefulness_samples > self.outcome_window {
            return Err(ResidencySpeculatorError::UsefulnessWindowTooSmall);
        }
        if u128::from(self.speculative_cost_basis_points) > BASIS_POINTS
            || u128::from(self.minimum_usefulness_basis_points) > BASIS_POINTS
        {
            return Err(ResidencySpeculatorError::InvalidBasisPoints);
        }
        Ok(self)
    }
}

/// Stable configuration, state, or terminal-feedback error.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ResidencySpeculatorError {
    /// A required capacity or feedback bound is zero.
    #[error("residency speculation bounds must be positive")]
    ZeroBound,
    /// Adaptive admission cannot reach its configured minimum sample count.
    #[error("residency usefulness window is smaller than its minimum sample count")]
    UsefulnessWindowTooSmall,
    /// A ratio exceeds 10,000 basis points.
    #[error("residency speculation basis points exceed 10,000")]
    InvalidBasisPoints,
    /// Exact counters cannot be represented.
    #[error("residency speculation accounting overflowed")]
    Overflow,
    /// Terminal feedback did not identify an active permit.
    #[error("residency speculation permit is not active")]
    UnknownPermit,
}

/// Typed reason why a candidate performed no backend work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResidencyRejection {
    /// Candidate belongs to another independently configured volume.
    WrongVolume,
    /// Candidate was derived from a generation no longer selected.
    StaleGeneration,
    /// Object or byte request is not positive and bounded.
    InvalidRequest,
    /// The same immutable object already has an active prediction.
    DuplicateObject,
    /// The same idempotency identity already names an active prediction.
    DuplicateOperation,
    /// Concurrent operation capacity is exhausted.
    OperationCapacity,
    /// Concurrent predicted-byte capacity is exhausted.
    ByteCapacity,
    /// Recent speculation exceeds the configured foreground-cost ratio.
    CostBudget,
    /// Recent predictions have not been useful enough.
    LowUsefulness,
}

/// Successful admission token. Dropping it performs no work and changes no authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResidencyPermit {
    candidate: ResidencyCandidate,
}

impl ResidencyPermit {
    /// Returns the complete admitted prediction.
    #[must_use]
    pub const fn candidate(self) -> ResidencyCandidate {
        self.candidate
    }
}

/// Exact admission outcome for one prediction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResidencyAdmission {
    /// Candidate may execute through the ordinary authenticated object store.
    Admitted(ResidencyPermit),
    /// Candidate performed no backend work.
    Rejected(ResidencyRejection),
}

/// Cumulative and current payload-free observations.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ResidencyMetrics {
    /// Candidates evaluated.
    pub candidates: u64,
    /// Candidates admitted.
    pub admitted: u64,
    /// Active predictions.
    pub active: u64,
    /// Estimated bytes held by active predictions.
    pub active_bytes: u64,
    /// Predictions consumed by later foreground work.
    pub useful: u64,
    /// Predictions that expired, failed, or were preempted unused.
    pub wasted: u64,
    /// Candidates rejected for a stale generation or wrong volume.
    pub rejected_fence: u64,
    /// Candidates rejected as duplicate active objects.
    pub rejected_duplicate: u64,
    /// Candidates rejected by operation or byte capacity.
    pub rejected_capacity: u64,
    /// Candidates rejected by the foreground-cost budget.
    pub rejected_cost: u64,
    /// Candidates rejected by adaptive usefulness feedback.
    pub rejected_usefulness: u64,
}

#[derive(Clone, Copy)]
struct TrafficSample {
    speculative: bool,
    bytes: u64,
}

/// One disposable per-volume predictor admission engine.
#[derive(Clone)]
pub struct ResidencySpeculator {
    options: ResidencySpeculatorOptions,
    volume_id: VolumeId,
    generation_id: GenerationId,
    active: BTreeMap<OperationId, ResidencyCandidate>,
    outcomes: VecDeque<bool>,
    traffic: VecDeque<TrafficSample>,
    metrics: ResidencyMetrics,
}

impl ResidencySpeculator {
    /// Creates one engine bound to an exact volume generation.
    ///
    /// # Errors
    ///
    /// Rejects zero, contradictory, or out-of-range policy bounds.
    pub fn new(
        options: ResidencySpeculatorOptions,
        volume_id: VolumeId,
        generation_id: GenerationId,
    ) -> Result<Self, ResidencySpeculatorError> {
        Ok(Self {
            options: options.validate()?,
            volume_id,
            generation_id,
            active: BTreeMap::new(),
            outcomes: VecDeque::new(),
            traffic: VecDeque::new(),
            metrics: ResidencyMetrics::default(),
        })
    }

    /// Records exact foreground bytes before evaluating dependent predictions.
    ///
    /// # Errors
    ///
    /// Fails closed if cumulative accounting cannot be represented.
    pub fn record_foreground(&mut self, bytes: u64) -> Result<(), ResidencySpeculatorError> {
        let mut next = self.clone();
        next.push_traffic(TrafficSample {
            speculative: false,
            bytes,
        })?;
        *self = next;
        Ok(())
    }

    /// Evaluates one candidate without performing object-store work.
    ///
    /// # Errors
    ///
    /// Fails closed if exact counters cannot be represented.
    pub fn admit(
        &mut self,
        candidate: ResidencyCandidate,
    ) -> Result<ResidencyAdmission, ResidencySpeculatorError> {
        let mut next = self.clone();
        let admission = next.admit_in_place(candidate)?;
        *self = next;
        Ok(admission)
    }

    fn admit_in_place(
        &mut self,
        candidate: ResidencyCandidate,
    ) -> Result<ResidencyAdmission, ResidencySpeculatorError> {
        self.metrics.candidates = checked_increment(self.metrics.candidates)?;
        if let Some(rejection) = self.rejection(candidate)? {
            self.record_rejection(rejection)?;
            return Ok(ResidencyAdmission::Rejected(rejection));
        }
        self.metrics.admitted = checked_increment(self.metrics.admitted)?;
        self.metrics.active = checked_increment(self.metrics.active)?;
        self.metrics.active_bytes = self
            .metrics
            .active_bytes
            .checked_add(candidate.request.maximum_bytes)
            .ok_or(ResidencySpeculatorError::Overflow)?;
        self.push_traffic(TrafficSample {
            speculative: true,
            bytes: candidate.request.maximum_bytes,
        })?;
        self.active.insert(candidate.operation_id, candidate);
        Ok(ResidencyAdmission::Admitted(ResidencyPermit { candidate }))
    }

    /// Completes one admitted prediction exactly once.
    ///
    /// # Errors
    ///
    /// Rejects unknown or duplicate terminal feedback and accounting overflow.
    pub fn finish(
        &mut self,
        operation_id: OperationId,
        useful: bool,
    ) -> Result<(), ResidencySpeculatorError> {
        let mut next = self.clone();
        next.finish_in_place(operation_id, useful)?;
        *self = next;
        Ok(())
    }

    fn finish_in_place(
        &mut self,
        operation_id: OperationId,
        useful: bool,
    ) -> Result<(), ResidencySpeculatorError> {
        let candidate = self
            .active
            .remove(&operation_id)
            .ok_or(ResidencySpeculatorError::UnknownPermit)?;
        self.metrics.active = self
            .metrics
            .active
            .checked_sub(1)
            .ok_or(ResidencySpeculatorError::Overflow)?;
        self.metrics.active_bytes = self
            .metrics
            .active_bytes
            .checked_sub(candidate.request.maximum_bytes)
            .ok_or(ResidencySpeculatorError::Overflow)?;
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

    /// Preempts every active prediction before foreground dispatch.
    ///
    /// Returned operation identities let an executor cancel the matching work.
    ///
    /// # Errors
    ///
    /// Fails closed if terminal accounting cannot be represented.
    pub fn preempt_for_foreground(&mut self) -> Result<Vec<OperationId>, ResidencySpeculatorError> {
        let mut next = self.clone();
        let operations = next.preempt_in_place()?;
        *self = next;
        Ok(operations)
    }

    fn preempt_in_place(&mut self) -> Result<Vec<OperationId>, ResidencySpeculatorError> {
        let operations: Vec<_> = self.active.keys().copied().collect();
        for operation_id in &operations {
            self.finish_in_place(*operation_id, false)?;
        }
        Ok(operations)
    }

    /// Fences stale work and selects a new immutable generation.
    ///
    /// # Errors
    ///
    /// Fails closed if preemption accounting cannot be represented.
    pub fn replace_generation(
        &mut self,
        generation_id: GenerationId,
    ) -> Result<Vec<OperationId>, ResidencySpeculatorError> {
        let mut next = self.clone();
        let preempted = next.preempt_in_place()?;
        next.generation_id = generation_id;
        *self = next;
        Ok(preempted)
    }

    /// Returns exact current and cumulative metrics.
    #[must_use]
    pub const fn metrics(&self) -> ResidencyMetrics {
        self.metrics
    }

    /// Returns a copy of one currently active execution permit.
    ///
    /// The executor revalidates the complete permit against this engine before
    /// performing I/O, so operation identity alone never authorizes work.
    #[must_use]
    pub fn active_permit(&self, operation_id: OperationId) -> Option<ResidencyPermit> {
        self.active
            .get(&operation_id)
            .copied()
            .map(|candidate| ResidencyPermit { candidate })
    }

    fn rejection(
        &self,
        candidate: ResidencyCandidate,
    ) -> Result<Option<ResidencyRejection>, ResidencySpeculatorError> {
        if candidate.volume_id != self.volume_id {
            return Ok(Some(ResidencyRejection::WrongVolume));
        }
        if candidate.generation_id != self.generation_id {
            return Ok(Some(ResidencyRejection::StaleGeneration));
        }
        if candidate.request.maximum_bytes == 0 {
            return Ok(Some(ResidencyRejection::InvalidRequest));
        }
        if self
            .active
            .values()
            .any(|active| active.request.object_id == candidate.request.object_id)
        {
            return Ok(Some(ResidencyRejection::DuplicateObject));
        }
        if self.active.contains_key(&candidate.operation_id) {
            return Ok(Some(ResidencyRejection::DuplicateOperation));
        }
        if self.active.len() >= self.options.maximum_active_operations as usize {
            return Ok(Some(ResidencyRejection::OperationCapacity));
        }
        if self
            .metrics
            .active_bytes
            .checked_add(candidate.request.maximum_bytes)
            .is_none_or(|bytes| bytes > self.options.maximum_active_bytes)
        {
            return Ok(Some(ResidencyRejection::ByteCapacity));
        }
        if !self.useful_enough() {
            return Ok(Some(ResidencyRejection::LowUsefulness));
        }
        let (foreground, speculative) = self.traffic_bytes()?;
        let allowed = (u128::from(foreground) + 1)
            .checked_mul(u128::from(self.options.speculative_cost_basis_points))
            .ok_or(ResidencySpeculatorError::Overflow)?
            / BASIS_POINTS;
        let proposed = u128::from(speculative) + u128::from(candidate.request.maximum_bytes);
        if proposed > allowed {
            return Ok(Some(ResidencyRejection::CostBudget));
        }
        Ok(None)
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

    fn traffic_bytes(&self) -> Result<(u64, u64), ResidencySpeculatorError> {
        let mut foreground = 0_u64;
        let mut speculative = 0_u64;
        for sample in &self.traffic {
            let total = if sample.speculative {
                &mut speculative
            } else {
                &mut foreground
            };
            *total = total
                .checked_add(sample.bytes)
                .ok_or(ResidencySpeculatorError::Overflow)?;
        }
        Ok((foreground, speculative))
    }

    fn push_traffic(&mut self, sample: TrafficSample) -> Result<(), ResidencySpeculatorError> {
        self.traffic.push_back(sample);
        while self.traffic.len() > self.options.traffic_window as usize {
            self.traffic.pop_front();
        }
        self.traffic_bytes().map(|_| ())
    }

    fn record_rejection(
        &mut self,
        rejection: ResidencyRejection,
    ) -> Result<(), ResidencySpeculatorError> {
        let counter = match rejection {
            ResidencyRejection::WrongVolume | ResidencyRejection::StaleGeneration => {
                &mut self.metrics.rejected_fence
            }
            ResidencyRejection::InvalidRequest
            | ResidencyRejection::OperationCapacity
            | ResidencyRejection::ByteCapacity => &mut self.metrics.rejected_capacity,
            ResidencyRejection::DuplicateObject | ResidencyRejection::DuplicateOperation => {
                &mut self.metrics.rejected_duplicate
            }
            ResidencyRejection::CostBudget => &mut self.metrics.rejected_cost,
            ResidencyRejection::LowUsefulness => &mut self.metrics.rejected_usefulness,
        };
        *counter = checked_increment(*counter)?;
        Ok(())
    }
}

/// Executes an admitted prediction through the ordinary authenticated read path.
///
/// When `store` is a [`crate::CachedObjectStore`], successful execution warms
/// that same bounded cache. The returned body is discarded, so speculation
/// cannot retain a second unmeasured byte store.
///
/// # Errors
///
/// Returns the ordinary typed object-store failure with all work completed
/// before failure. Cancellation and byte-budget rejection therefore behave
/// exactly like a foreground read.
pub async fn execute_residency<S: AsyncObjectStore>(
    speculator: &ResidencySpeculator,
    store: &S,
    permit: ResidencyPermit,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> ObjectResult<u64> {
    if speculator.active.get(&permit.candidate.operation_id) != Some(&permit.candidate) {
        return Err(crate::performance::OperationFailure::before_work(
            crate::storage::ObjectStoreError::Rejected(
                "residency speculation permit is stale or inactive".to_owned(),
            ),
        ));
    }
    let request = permit.candidate.request;
    let receipt = store
        .read(
            request.object_id,
            request.maximum_bytes,
            budget,
            cancellation,
        )
        .await?;
    let byte_length = crate::foundation::usize_to_u64(receipt.value.bytes.len());
    Ok(ObjectReceipt {
        value: byte_length,
        work: receipt.work,
    })
}

fn checked_increment(value: u64) -> Result<u64, ResidencySpeculatorError> {
    value
        .checked_add(1)
        .ok_or(ResidencySpeculatorError::Overflow)
}

#[cfg(test)]
#[path = "tests/residency.rs"]
mod tests;
