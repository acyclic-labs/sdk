use acyclic_fs::model::{CheckoutMode, ConsistencyMode};

/// Opens a writable checkout that follows new generations on explicit rebase.
pub fn writable_manual() -> CheckoutMode {
    CheckoutMode {
        consistency: ConsistencyMode::Manual,
        ..CheckoutMode::tracking_transaction()
    }
}
