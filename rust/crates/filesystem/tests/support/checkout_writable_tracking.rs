use acyclic_fs::model::{AccessMode, CheckoutMode, ConsistencyMode, MutationMode};

pub(crate) fn writable_tracking() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadWrite,
        consistency: ConsistencyMode::TrackingSafe,
        mutations: MutationMode::PrivateOverlay,
    }
}
