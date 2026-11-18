use acyclic_fs_format::model::{CheckoutMode, ConsistencyMode};

pub(crate) fn writable_manual() -> CheckoutMode {
    CheckoutMode {
        consistency: ConsistencyMode::Manual,
        ..CheckoutMode::tracking_transaction()
    }
}
