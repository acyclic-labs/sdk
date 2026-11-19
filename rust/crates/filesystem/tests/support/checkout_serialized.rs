use acyclic_fs::model::{ConcurrencyMode, VolumeConfig};

use super::checkout_fixture::config;

/// Uses serialized authority on the standard portable ephemeral volume.
pub fn serialized_config() -> VolumeConfig {
    VolumeConfig {
        concurrency: ConcurrencyMode::SerializedAuthority,
        ..config()
    }
}
