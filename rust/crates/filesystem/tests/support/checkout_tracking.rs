use acyclic_fs_format::model::{AccessMode, CheckoutMode, ConsistencyMode, MutationMode};

pub(crate) fn tracking() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadOnly,
        consistency: ConsistencyMode::TrackingSafe,
        mutations: MutationMode::None,
    }
}
