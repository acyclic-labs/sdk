//! Strict portable absolute paths used at the volume boundary.

use crate::model::VolumeLimits;
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

/// Canonical absolute path with `/` separators and no relative components.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PortablePath(String);

impl PortablePath {
    /// Canonical volume root.
    pub const ROOT: &'static str = "/";

    /// Parses and bounds one canonical absolute path.
    ///
    /// # Errors
    ///
    /// Rejects relative, repeated-separator, dot, parent, NUL, trailing
    /// separator, or over-limit paths. Host adapters parse native paths before
    /// this portable boundary.
    pub fn parse(value: &str, limits: VolumeLimits) -> Result<Self, PathError> {
        let path_bytes = u32::try_from(value.len()).map_err(|_| PathError::PathTooLong)?;
        if path_bytes > limits.maximum_path_bytes {
            return Err(PathError::PathTooLong);
        }
        if value == Self::ROOT {
            return Ok(Self(value.to_owned()));
        }
        if !value.starts_with('/') {
            return Err(PathError::NotAbsolute);
        }
        if value.ends_with('/') {
            return Err(PathError::TrailingSeparator);
        }

        let mut depth = 0_u16;
        for component in value[1..].split('/') {
            if component.is_empty() {
                return Err(PathError::EmptyComponent);
            }
            if component == "." {
                return Err(PathError::CurrentComponent);
            }
            if component == ".." {
                return Err(PathError::ParentComponent);
            }
            if component.as_bytes().contains(&0) {
                return Err(PathError::Nul);
            }
            let component_bytes =
                u32::try_from(component.len()).map_err(|_| PathError::ComponentTooLong)?;
            if component_bytes > limits.maximum_component_bytes {
                return Err(PathError::ComponentTooLong);
            }
            depth = depth.checked_add(1).ok_or(PathError::TooDeep)?;
            if depth > limits.maximum_path_depth {
                return Err(PathError::TooDeep);
            }
        }
        Ok(Self(value.to_owned()))
    }

    /// Borrows the canonical wire string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns whether this path is the same as or lies below `ancestor`.
    #[must_use]
    pub fn is_within(&self, ancestor: &Self) -> bool {
        if ancestor.0 == Self::ROOT {
            return true;
        }
        self.0 == ancestor.0
            || self
                .0
                .strip_prefix(&ancestor.0)
                .is_some_and(|suffix| suffix.starts_with('/'))
    }

    /// Returns the number of logical components.
    #[must_use]
    pub fn depth(&self) -> usize {
        if self.0 == Self::ROOT {
            0
        } else {
            self.0[1..].split('/').count()
        }
    }

    /// Iterates canonical components without allocating.
    pub fn components(&self) -> impl Iterator<Item = &str> {
        self.0
            .strip_prefix('/')
            .unwrap_or_default()
            .split('/')
            .filter(|component| !component.is_empty())
    }
}

impl fmt::Debug for PortablePath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("PortablePath")
            .field(&self.0)
            .finish()
    }
}

impl fmt::Display for PortablePath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Portable path admission failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PathError {
    /// Path is not absolute.
    #[error("portable path must be absolute")]
    NotAbsolute,
    /// A non-root path ends in `/`.
    #[error("portable path must not end in a separator")]
    TrailingSeparator,
    /// Two separators produced an empty name.
    #[error("portable path contains an empty component")]
    EmptyComponent,
    /// `.` is never canonical.
    #[error("portable path contains a current-directory component")]
    CurrentComponent,
    /// `..` is never admitted.
    #[error("portable path contains a parent component")]
    ParentComponent,
    /// NUL cannot be represented by host adapters.
    #[error("portable path contains NUL")]
    Nul,
    /// Encoded path exceeds its configured byte bound.
    #[error("portable path exceeds its byte bound")]
    PathTooLong,
    /// One encoded component exceeds its configured byte bound.
    #[error("portable path component exceeds its byte bound")]
    ComponentTooLong,
    /// Component depth exceeds its configured bound.
    #[error("portable path exceeds its depth bound")]
    TooDeep,
}

#[cfg(test)]
#[path = "tests/path.rs"]
mod tests;
