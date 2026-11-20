//! Strict portable paths used at the volume boundary.

pub use acyclic_fs_format::path::{PathError, PortablePath};
pub(crate) use acyclic_fs_format::path::{
    is_canonical_relative, is_canonical_relative_with_step, prefix,
};

#[cfg(test)]
#[path = "tests/path.rs"]
mod tests;
