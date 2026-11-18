use acyclic_fs::kernel::{LogicalName, NameEncoding, NamespacePath};
#[path = "checkout_defaults.rs"]
mod checkout_defaults;
pub(crate) use checkout_defaults::{config, writable_pinned};

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
