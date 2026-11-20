use acyclic_fs::model::{AccessMode, CheckoutMode, ConsistencyMode, MutationMode};

/// Opens a writable checkout following live generations.
pub fn writable_live() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadWrite,
        consistency: ConsistencyMode::Live,
        mutations: MutationMode::DirectLive,
    }
}
