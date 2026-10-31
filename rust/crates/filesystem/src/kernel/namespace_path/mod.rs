//! Bounded profile-independent namespace paths.

use super::allocation::{KernelAccounting, KernelOwnedAllocation};
use super::{LogicalName, NameEncoding, TreePageError};
use crate::model::{FilesystemProfile, VolumeLimits};
use crate::path::PortablePath;
use crate::{CancellationToken, WorkBudget, WorkCounters};
use thiserror::Error;
mod allocation;
mod operations;
pub(super) use allocation::finish_owned;

/// Canonical absolute path represented as exact logical-name components.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct NamespacePath {
    components: Vec<LogicalName>,
    encoded_bytes: u32,
}

impl NamespacePath {
    /// Constructs one bounded absolute namespace path.
    pub fn new(
        components: Vec<LogicalName>,
        limits: VolumeLimits,
    ) -> Result<Self, NamespacePathError> {
        let encoded_bytes = Self::validate_component_lengths(
            components.iter().map(|name| name.as_bytes().len()),
            limits,
        )?;
        Ok(Self {
            components,
            encoded_bytes,
        })
    }

    /// Converts a strict UTF-8 portable path without losing information.
    pub fn from_portable(
        path: &PortablePath,
        limits: VolumeLimits,
    ) -> Result<Self, NamespacePathError> {
        Self::from_portable_in_profile(path, FilesystemProfile::Portable, limits)
    }

    /// Converts a portable path to the canonical names of one volume profile.
    pub fn from_portable_in_profile(
        path: &PortablePath,
        profile: FilesystemProfile,
        limits: VolumeLimits,
    ) -> Result<Self, NamespacePathError> {
        Self::from_text_in_profile(path.as_str(), profile, limits)
    }

    /// Parses borrowed Unicode text directly into the destination profile.
    pub(crate) fn from_text_in_profile(
        path: &str,
        profile: FilesystemProfile,
        limits: VolumeLimits,
    ) -> Result<Self, NamespacePathError> {
        let cancellation = CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        Self::parse_in_profile(path, profile, limits, &mut accounting, &cancellation)
            .map(|(path, _)| path)
            .map_err(operations::path_error)
    }

    /// Exact backing capacity retained by this path.
    pub fn logical_owned_bytes(&self) -> u64 {
        (self.components.capacity() * std::mem::size_of::<LogicalName>()
            + self
                .components
                .iter()
                .map(LogicalName::retained_bytes)
                .sum::<usize>()) as u64
    }

    /// Exact backing capacity retained by this path.
    pub fn retained_bytes(&self) -> u64 {
        self.logical_owned_bytes()
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

    /// Returns the exact parent path, or `None` for the namespace root.
    #[must_use]
    pub fn parent(&self) -> Option<Self> {
        let (parents, name) = self.split_last()?;
        let encoded_bytes = if self.components.len() == 1 {
            1
        } else {
            self.encoded_bytes
                .saturating_sub(u32::try_from(name.as_bytes().len()).unwrap_or(u32::MAX))
                .saturating_sub(1)
        };
        let components = parents.to_vec();
        Some(Self {
            components,
            encoded_bytes,
        })
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
    /// The Unicode spelling violates the canonical portable rules.
    #[error(transparent)]
    Path(crate::path::PathError),
    /// A valid opaque native name cannot be represented by the Unicode API.
    #[error("non-Unicode path cannot be projected as a portable string")]
    NonUnicodeProjection,
    /// An admitted work limit was exceeded.
    #[error(transparent)]
    Work(crate::WorkError),
    /// The caller cancelled before the next allocation.
    #[error("filesystem operation was cancelled")]
    Cancelled,
    /// A requested owned capacity could not be allocated.
    #[error("namespace path allocation failed")]
    AllocationFailed,
}

impl From<NamespacePathError> for crate::FsError {
    fn from(error: NamespacePathError) -> Self {
        match error {
            NamespacePathError::Work(error) => Self::Work(error),
            NamespacePathError::Cancelled => Self::Cancelled(crate::CancellationError),
            error => Self::Path(super::PathLookupError::NamespacePath(error)),
        }
    }
}

#[cfg(test)]
#[path = "../tests/namespace_path.rs"]
mod tests;
