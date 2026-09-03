//! Bounded profile-independent namespace paths.

use super::{LogicalName, NameEncoding, TreePageError};
use crate::model::VolumeLimits;
use crate::path::PortablePath;
use thiserror::Error;

/// Canonical absolute path represented as exact logical-name components.
///
/// An empty component vector denotes the volume root. Unlike
/// [`PortablePath`], this type preserves raw POSIX bytes and Windows UTF-16LE
/// names. A volume executor additionally checks that every component encoding
/// is admitted by that volume's filesystem profile.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct NamespacePath {
    components: Vec<LogicalName>,
    encoded_bytes: u32,
}

impl NamespacePath {
    /// Constructs one bounded absolute namespace path.
    ///
    /// # Errors
    ///
    /// Rejects excessive depth, component bytes, total bytes, or arithmetic
    /// overflow before the path reaches a storage backend.
    pub fn new(
        components: Vec<LogicalName>,
        limits: VolumeLimits,
    ) -> Result<Self, NamespacePathError> {
        if u16::try_from(components.len()).unwrap_or(u16::MAX) > limits.maximum_path_depth {
            return Err(NamespacePathError::TooDeep);
        }
        let mut encoded_bytes = 1_u32;
        for component in &components {
            let length = u32::try_from(component.as_bytes().len())
                .map_err(|_| NamespacePathError::ComponentTooLong)?;
            if length > limits.maximum_component_bytes {
                return Err(NamespacePathError::ComponentTooLong);
            }
            encoded_bytes = encoded_bytes
                .checked_add(length)
                .and_then(|value| value.checked_add(1))
                .ok_or(NamespacePathError::PathTooLong)?;
        }
        if !components.is_empty() {
            encoded_bytes = encoded_bytes.saturating_sub(1);
        }
        if encoded_bytes > limits.maximum_path_bytes {
            return Err(NamespacePathError::PathTooLong);
        }
        Ok(Self {
            components,
            encoded_bytes,
        })
    }

    /// Converts a strict UTF-8 portable path without losing information.
    ///
    /// # Errors
    ///
    /// Returns the same component and total bounds as [`Self::new`], plus a
    /// canonical-name failure if the portable input violates the name format.
    pub fn from_portable(
        path: &PortablePath,
        limits: VolumeLimits,
    ) -> Result<Self, NamespacePathError> {
        let components = path
            .components()
            .map(|component| {
                LogicalName::new(
                    NameEncoding::Utf8,
                    component.as_bytes().to_vec(),
                    limits.maximum_component_bytes,
                )
                .map_err(NamespacePathError::Name)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Self::new(components, limits)
    }

    /// Returns whether this is the volume root.
    #[must_use]
    pub fn is_root(&self) -> bool {
        self.components.is_empty()
    }

    /// Exact logical path depth.
    #[must_use]
    pub fn depth(&self) -> usize {
        self.components.len()
    }

    /// Canonical encoded bytes including separators and the leading root mark.
    #[must_use]
    pub const fn encoded_bytes(&self) -> u32 {
        self.encoded_bytes
    }

    /// Borrows exact logical-name components in root-to-leaf order.
    #[must_use]
    pub fn components(&self) -> &[LogicalName] {
        &self.components
    }

    /// Returns the parent path and final name, or `None` for root.
    #[must_use]
    pub fn split_last(&self) -> Option<(&[LogicalName], &LogicalName)> {
        self.components
            .split_last()
            .map(|(name, parent)| (parent, name))
    }

    /// Returns whether this path is equal to or below `ancestor`.
    #[must_use]
    pub fn is_within(&self, ancestor: &Self) -> bool {
        self.components.starts_with(&ancestor.components)
    }
}

/// Profile-independent namespace path failures.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum NamespacePathError {
    /// Component count exceeds the volume bound.
    #[error("namespace path exceeds its depth bound")]
    TooDeep,
    /// One exact encoded component exceeds the volume bound.
    #[error("namespace path component exceeds its byte bound")]
    ComponentTooLong,
    /// Total canonical encoded path bytes exceed the volume bound.
    #[error("namespace path exceeds its byte bound")]
    PathTooLong,
    /// One portable component could not become a canonical logical name.
    #[error(transparent)]
    Name(TreePageError),
}

#[cfg(test)]
#[path = "tests/namespace_path.rs"]
mod tests;
