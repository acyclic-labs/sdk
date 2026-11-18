use acyclic_fs::model::{
    AccessMode, CheckoutMode, ConcurrencyMode, ConsistencyMode, MutationMode, VolumeConfig,
};

use super::checkout_fixture::config;

pub(crate) fn serialized_config() -> VolumeConfig {
    VolumeConfig {
        concurrency: ConcurrencyMode::SerializedAuthority,
        ..config()
    }
}

pub(crate) fn writable_live() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadWrite,
        consistency: ConsistencyMode::Live,
        mutations: MutationMode::DirectLive,
    }
}
