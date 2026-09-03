//! Disposable, correctness-inert residency and storage-promotion speculation.

mod controller;
mod promotion;
mod residency;

pub use controller::{
    SpeculationController, SpeculationControllerError, SpeculationMetrics, SpeculationOptions,
    SpeculationPreemption,
};

pub use promotion::{
    ObjectResidency, PromotionAdmission, PromotionCandidate, PromotionDestination,
    PromotionExecutor, PromotionMetrics, PromotionPlan, PromotionRejection, PromotionSpeculator,
    PromotionSpeculatorError, PromotionSpeculatorOptions, StorageLocationId, StorageTier,
    StorePromotionExecutor, StorePromotionExecutorError, execute_promotion,
};
pub use residency::{
    ResidencyAdmission, ResidencyCandidate, ResidencyHint, ResidencyMetrics, ResidencyPermit,
    ResidencyReason, ResidencyRejection, ResidencySpeculator, ResidencySpeculatorError,
    ResidencySpeculatorOptions, execute_residency,
};
