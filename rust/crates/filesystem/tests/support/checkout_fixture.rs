use acyclic_fs::kernel::{LogicalName, NameEncoding, NamespacePath};
use acyclic_fs::model::{
    AccessMode, CheckoutMode, ConsistencyMode, Lifecycle, MutationMode, VolumeConfig,
};

pub(crate) fn writable_pinned() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadWrite,
        consistency: ConsistencyMode::Pinned,
        mutations: MutationMode::PrivateOverlay,
    }
}

pub(crate) fn path(value: &str) -> Result<NamespacePath, Box<dyn std::error::Error>> {
    Ok(NamespacePath::new(
        vec![LogicalName::new(
            NameEncoding::Utf8,
            value.as_bytes().to_vec(),
        )?],
        VolumeConfig::portable(Lifecycle::Ephemeral).limits,
    )?)
}
