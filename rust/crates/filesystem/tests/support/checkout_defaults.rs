use acyclic_fs_format::model::{CheckoutMode, ConsistencyMode, Lifecycle, VolumeConfig};

pub(crate) fn writable_pinned() -> CheckoutMode {
    CheckoutMode {
        consistency: ConsistencyMode::Pinned,
        ..CheckoutMode::tracking_transaction()
    }
}

pub(crate) fn config() -> VolumeConfig {
    VolumeConfig::portable(Lifecycle::Ephemeral)
}
