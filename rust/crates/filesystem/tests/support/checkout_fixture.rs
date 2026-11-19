use acyclic_fs::kernel::{LogicalName, NameEncoding, NamespacePath};
#[path = "checkout_defaults.rs"]
mod checkout_defaults;
pub use checkout_defaults::{config, writable_pinned};

/// Builds a namespace path with one UTF-8 component.
pub fn path(value: &str) -> Result<NamespacePath, Box<dyn std::error::Error>> {
    path_parts(&[value])
}

/// Builds a namespace path with the provided UTF-8 components.
pub fn path_parts(values: &[&str]) -> Result<NamespacePath, Box<dyn std::error::Error>> {
    let components = values
        .iter()
        .map(|value| LogicalName::new(NameEncoding::Utf8, value.as_bytes().to_vec()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(NamespacePath::new(components, config().limits)?)
}
