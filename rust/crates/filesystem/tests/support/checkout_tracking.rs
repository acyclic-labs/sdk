use acyclic_fs::model::{AccessMode, CheckoutMode, ConsistencyMode, MutationMode};

pub(crate) fn tracking() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadOnly,
        consistency: ConsistencyMode::TrackingSafe,
        mutations: MutationMode::None,
    }
}
