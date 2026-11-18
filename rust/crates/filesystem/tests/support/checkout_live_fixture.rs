use acyclic_fs::model::{AccessMode, CheckoutMode, ConsistencyMode, MutationMode};

pub(crate) fn writable_live() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadWrite,
        consistency: ConsistencyMode::Live,
        mutations: MutationMode::DirectLive,
    }
}
