use acyclic_fs::model::{ConcurrencyMode, Lifecycle, VolumeConfig};

/// Uses serialized authority on the standard portable ephemeral volume.
pub fn serialized_config() -> VolumeConfig {
    VolumeConfig {
        concurrency: ConcurrencyMode::SerializedAuthority,
        ..VolumeConfig::portable(Lifecycle::Ephemeral)
    }
}
