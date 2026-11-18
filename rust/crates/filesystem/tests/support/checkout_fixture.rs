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

pub(crate) fn config() -> VolumeConfig {
    VolumeConfig::portable(Lifecycle::Ephemeral)
}

pub(crate) fn path(value: &str) -> Result<NamespacePath, Box<dyn std::error::Error>> {
    path_parts(&[value])
}

pub(crate) fn path_parts(values: &[&str]) -> Result<NamespacePath, Box<dyn std::error::Error>> {
    let components = values
        .iter()
        .map(|value| LogicalName::new(NameEncoding::Utf8, value.as_bytes().to_vec()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(NamespacePath::new(components, config().limits)?)
}
