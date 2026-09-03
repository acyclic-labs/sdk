//! Atomic composition of residency and promotion prediction state machines.

use super::{
    ObjectResidency, PromotionAdmission, PromotionCandidate, PromotionDestination,
    PromotionMetrics, PromotionSpeculator, PromotionSpeculatorError, PromotionSpeculatorOptions,
    ResidencyAdmission, ResidencyCandidate, ResidencyHint, ResidencyMetrics, ResidencyPermit,
    ResidencySpeculator, ResidencySpeculatorError, ResidencySpeculatorOptions, StorageTier,
};
use crate::foundation::{GenerationId, OperationId, VolumeId};
use thiserror::Error;

/// Complete policy for both correctness-inert speculation engines.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SpeculationOptions {
    /// Process-local immutable-object residency policy.
    pub residency: ResidencySpeculatorOptions,
    /// Cross-location immutable-object promotion policy.
    pub promotion: PromotionSpeculatorOptions,
}

/// Operation identities terminalized by one foreground or generation fence.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SpeculationPreemption {
    /// Cancelled local residency operations in stable identity order.
    pub residency: Vec<OperationId>,
    /// Cancelled cross-location promotions in stable identity order.
    pub promotion: Vec<OperationId>,
}

/// Exact payload-free metrics from both engines at one instant.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SpeculationMetrics {
    /// Local residency state and cumulative decisions.
    pub residency: ResidencyMetrics,
    /// Cross-location promotion state and cumulative decisions.
    pub promotion: PromotionMetrics,
}

/// Atomic composition failure. Neither engine changes when a composed
/// transition fails.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SpeculationControllerError {
    /// Residency policy, admission, feedback, or accounting failed.
    #[error(transparent)]
    Residency(#[from] ResidencySpeculatorError),
    /// Promotion policy, planning, feedback, or accounting failed.
    #[error(transparent)]
    Promotion(#[from] PromotionSpeculatorError),
}

/// Runtime-neutral owner of one volume generation's two speculation engines.
///
/// The controller performs no I/O and spawns no work. It gives embedded,
/// browser, daemon, and fleet adapters one atomic transition surface while
/// execution remains pull-based through `execute_residency` and
/// `execute_promotion`.
#[derive(Clone)]
pub struct SpeculationController {
    residency: ResidencySpeculator,
    promotion: PromotionSpeculator,
}

impl SpeculationController {
    /// Creates both engines against the same exact volume generation.
    ///
    /// # Errors
    ///
    /// Rejects an invalid policy without retaining either partial engine.
    pub fn new(
        options: SpeculationOptions,
        volume_id: VolumeId,
        generation_id: GenerationId,
    ) -> Result<Self, SpeculationControllerError> {
        let residency = ResidencySpeculator::new(options.residency, volume_id, generation_id)?;
        let promotion = PromotionSpeculator::new(options.promotion, volume_id, generation_id)?;
        Ok(Self {
            residency,
            promotion,
        })
    }

    /// Records foreground demand and evaluates its already-authenticated
    /// successor as one all-or-nothing transition.
    ///
    /// # Errors
    ///
    /// Leaves foreground history and admission state unchanged on exact
    /// accounting failure.
    pub fn observe_hint(
        &mut self,
        operation_id: OperationId,
        volume_id: VolumeId,
        generation_id: GenerationId,
        foreground_bytes: u64,
        hint: ResidencyHint,
    ) -> Result<ResidencyAdmission, SpeculationControllerError> {
        let mut residency = self.residency.clone();
        residency.record_foreground(foreground_bytes)?;
        let admission = residency.admit(ResidencyCandidate::from_hint(
            operation_id,
            volume_id,
            generation_id,
            hint,
        ))?;
        self.residency = residency;
        Ok(admission)
    }

    /// Converts an admitted local prediction into a bounded promotion plan.
    ///
    /// # Errors
    ///
    /// Leaves promotion state unchanged on policy or accounting failure.
    pub fn plan_promotion(
        &mut self,
        permit: ResidencyPermit,
        accepted_tiers: Vec<StorageTier>,
        residency: &[ObjectResidency],
        destinations: &[PromotionDestination],
    ) -> Result<PromotionAdmission, SpeculationControllerError> {
        Ok(self.promotion.plan(
            PromotionCandidate::from_residency(permit, accepted_tiers),
            residency,
            destinations,
        )?)
    }

    /// Records terminal local-residency usefulness exactly once.
    ///
    /// # Errors
    ///
    /// Rejects unknown identities or accounting overflow without mutation.
    pub fn finish_residency(
        &mut self,
        operation_id: OperationId,
        useful: bool,
    ) -> Result<(), SpeculationControllerError> {
        Ok(self.residency.finish(operation_id, useful)?)
    }

    /// Records terminal promotion usefulness exactly once.
    ///
    /// # Errors
    ///
    /// Rejects unknown identities or accounting overflow without mutation.
    pub fn finish_promotion(
        &mut self,
        operation_id: OperationId,
        useful: bool,
    ) -> Result<(), SpeculationControllerError> {
        Ok(self.promotion.finish(operation_id, useful)?)
    }

    /// Preempts both engines before recording new foreground traffic.
    ///
    /// # Errors
    ///
    /// Commits all cancellations and traffic together or changes neither
    /// engine.
    pub fn preempt_for_foreground(
        &mut self,
        foreground_bytes: u64,
    ) -> Result<SpeculationPreemption, SpeculationControllerError> {
        let mut residency = self.residency.clone();
        let mut promotion = self.promotion.clone();
        let residency_operations = residency.preempt_for_foreground()?;
        let promotion_operations = promotion.preempt_for_foreground()?;
        residency.record_foreground(foreground_bytes)?;
        self.residency = residency;
        self.promotion = promotion;
        Ok(SpeculationPreemption {
            residency: residency_operations,
            promotion: promotion_operations,
        })
    }

    /// Atomically fences both engines onto a new immutable generation.
    ///
    /// # Errors
    ///
    /// Leaves both engines on the old generation if either transition fails.
    pub fn replace_generation(
        &mut self,
        generation_id: GenerationId,
    ) -> Result<SpeculationPreemption, SpeculationControllerError> {
        let mut residency = self.residency.clone();
        let mut promotion = self.promotion.clone();
        let residency_operations = residency.replace_generation(generation_id)?;
        let promotion_operations = promotion.replace_generation(generation_id)?;
        self.residency = residency;
        self.promotion = promotion;
        Ok(SpeculationPreemption {
            residency: residency_operations,
            promotion: promotion_operations,
        })
    }

    /// Returns both exact metrics from one committed controller state.
    #[must_use]
    pub const fn metrics(&self) -> SpeculationMetrics {
        SpeculationMetrics {
            residency: self.residency.metrics(),
            promotion: self.promotion.metrics(),
        }
    }

    /// Borrows the local residency engine for pull-based execution.
    #[must_use]
    pub const fn residency(&self) -> &ResidencySpeculator {
        &self.residency
    }

    /// Borrows the promotion engine for pull-based execution.
    #[must_use]
    pub const fn promotion(&self) -> &PromotionSpeculator {
        &self.promotion
    }

    /// Returns one currently active local execution permit.
    #[must_use]
    pub fn active_residency_permit(&self, operation_id: OperationId) -> Option<ResidencyPermit> {
        self.residency.active_permit(operation_id)
    }
}

#[cfg(test)]
#[path = "tests/controller.rs"]
mod tests;
