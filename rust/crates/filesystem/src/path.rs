//! Strict portable paths used at the volume boundary.

pub use acyclic_fs_format::path::{PathError, PortablePath};
pub(crate) use acyclic_fs_format::path::{is_canonical_relative, prefix};
// Only native materialization meters a path check step by step.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use acyclic_fs_format::path::is_canonical_relative_with_step;

#[cfg(test)]
#[path = "tests/path.rs"]
mod tests;
