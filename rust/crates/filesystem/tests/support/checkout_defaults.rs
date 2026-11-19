use acyclic_fs::model::{CheckoutMode, ConsistencyMode, Lifecycle, VolumeConfig};

/// Opens a writable checkout pinned to its initial generation.
pub fn writable_pinned() -> CheckoutMode {
    CheckoutMode {
        consistency: ConsistencyMode::Pinned,
        ..CheckoutMode::tracking_transaction()
    }
}

/// Returns the portable ephemeral volume configuration used by fixture checkouts.
pub fn config() -> VolumeConfig {
    VolumeConfig::portable(Lifecycle::Ephemeral)
}
