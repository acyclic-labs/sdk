use acyclic_fs_format::model::{ConcurrencyMode, VolumeConfig};

use super::checkout_fixture::config;

pub(crate) fn serialized_config() -> VolumeConfig {
    VolumeConfig {
        concurrency: ConcurrencyMode::SerializedAuthority,
        ..config()
    }
}
