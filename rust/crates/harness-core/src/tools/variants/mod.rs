//! Native shapes implemented over the shared execution primitives.
mod patch;
pub use patch::apply_patch;

/// The canonical variant a native profile layer names as a declaration's
/// implementation.
///
/// # Errors
/// [`crate::Error::Invalid`] for a name no variant has.
pub fn tool(name: &str) -> crate::Result<crate::tool::Tool> {
    match name {
        "apply_patch" => apply_patch(),
        _ => Err(crate::Error::Invalid(format!(
            "native implementation `{name}` is unknown"
        ))),
    }
}
