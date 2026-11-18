use acyclic_fs::model::{AccessMode, CheckoutMode, ConsistencyMode, MutationMode};

pub(crate) fn writable_pinned() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadWrite,
        consistency: ConsistencyMode::Pinned,
        mutations: MutationMode::PrivateOverlay,
    }
}
