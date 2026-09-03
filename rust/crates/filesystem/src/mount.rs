//! Composition of independent volume checkouts at distinct mount paths.

use crate::async_storage::{AsyncAuthorityStore, AsyncObjectStore};
use crate::facade::Checkout;
use crate::foundation::{GenerationId, MountId, VolumeId};
use crate::kernel::{NamespacePath, NamespacePathError};
use crate::model::{AccessMode, ConsistencyMode};
use crate::path::{PathError, PortablePath};
use std::collections::BTreeMap;
use thiserror::Error;

/// One independently configured checkout mounted into a composed view.
pub struct MountedCheckout<A, O> {
    mount_id: MountId,
    path: PortablePath,
    checkout: Checkout<A, O>,
}

impl<A, O> MountedCheckout<A, O> {
    /// Stable identity of this mount-table entry.
    #[must_use]
    pub const fn mount_id(&self) -> MountId {
        self.mount_id
    }

    /// Canonical path where the checkout becomes visible.
    #[must_use]
    pub const fn path(&self) -> &PortablePath {
        &self.path
    }

    /// Independently atomic owning volume.
    #[must_use]
    pub const fn volume_id(&self) -> VolumeId {
        self.checkout.volume_id()
    }

    /// Borrows the mounted checkout.
    #[must_use]
    pub const fn checkout(&self) -> &Checkout<A, O> {
        &self.checkout
    }

    /// Mutably borrows the mounted checkout.
    #[must_use]
    pub const fn checkout_mut(&mut self) -> &mut Checkout<A, O> {
        &mut self.checkout
    }
}

/// One resolved operation route into a volume-relative namespace.
pub struct RoutedCheckout<'a, A, O> {
    /// Stable selected mount identity.
    pub mount_id: MountId,
    /// Canonical path relative to the selected volume root.
    pub path: NamespacePath,
    /// The selected independently configured checkout.
    pub checkout: &'a mut Checkout<A, O>,
}

/// Deterministic longest-prefix router that owns independently configured checkouts.
pub struct MountedView<A, O> {
    bindings: BTreeMap<PortablePath, MountedCheckout<A, O>>,
}

impl<A, O> MountedView<A, O> {
    /// Starts constructing an empty mounted view.
    #[must_use]
    pub fn builder() -> MountedViewBuilder<A, O> {
        MountedViewBuilder::default()
    }

    /// Resolves one path by longest component-prefix match.
    #[must_use]
    pub fn resolve(&self, path: &PortablePath) -> Option<&MountedCheckout<A, O>> {
        self.bindings
            .values()
            .filter(|binding| path.is_within(&binding.path))
            .max_by_key(|binding| binding.path.depth())
    }

    /// Resolves and mutably borrows one checkout with a volume-relative path.
    ///
    /// # Errors
    ///
    /// Returns a typed error for unmapped paths or an impossible relative-path
    /// conversion under the selected volume's explicit limits.
    pub fn route_mut(
        &mut self,
        path: &PortablePath,
    ) -> Result<RoutedCheckout<'_, A, O>, MountError> {
        let selected = self
            .bindings
            .keys()
            .filter(|candidate| path.is_within(candidate))
            .max_by_key(|candidate| candidate.depth())
            .cloned()
            .ok_or(MountError::UnmappedPath)?;
        let binding = self
            .bindings
            .get_mut(&selected)
            .ok_or(MountError::UnmappedPath)?;
        let relative = relative_path(path, &selected, binding.checkout.volume_config().limits)?;
        Ok(RoutedCheckout {
            mount_id: binding.mount_id,
            path: NamespacePath::from_portable(&relative, binding.checkout.volume_config().limits)
                .map_err(MountError::InvalidNamespacePath)?,
            checkout: &mut binding.checkout,
        })
    }

    /// Captures the exact generation selected for every mount.
    #[must_use]
    pub fn snapshot(&self) -> MountedViewSnapshot {
        MountedViewSnapshot {
            bindings: self
                .bindings
                .values()
                .map(|binding| MountedGeneration {
                    mount_id: binding.mount_id,
                    path: binding.path.clone(),
                    volume_id: binding.checkout.volume_id(),
                    generation: binding.checkout.generation_id(),
                    access: binding.checkout.mode().access,
                    consistency: binding.checkout.mode().consistency,
                })
                .collect(),
        }
    }

    /// Validates that a rename stays within one independently atomic checkout.
    ///
    /// # Errors
    ///
    /// Returns [`MountError::CrossVolume`] when endpoints resolve to different
    /// mount entries, even when both entries happen to select the same volume.
    pub fn validate_rename(
        &self,
        source: &PortablePath,
        destination: &PortablePath,
    ) -> Result<(), MountError> {
        let source_binding = self.resolve(source).ok_or(MountError::UnmappedPath)?;
        let destination_binding = self.resolve(destination).ok_or(MountError::UnmappedPath)?;
        if source_binding.mount_id != destination_binding.mount_id {
            return Err(MountError::CrossVolume);
        }
        Ok(())
    }
}

/// Builder that rejects ambiguous mount tables.
pub struct MountedViewBuilder<A, O> {
    bindings: BTreeMap<PortablePath, MountedCheckout<A, O>>,
}

impl<A, O> Default for MountedViewBuilder<A, O> {
    fn default() -> Self {
        Self {
            bindings: BTreeMap::new(),
        }
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> MountedViewBuilder<A, O> {
    /// Adds one checkout at a distinct canonical path.
    ///
    /// # Errors
    ///
    /// Rejects malformed or duplicate paths before changing the builder.
    pub fn mount(mut self, path: &str, checkout: Checkout<A, O>) -> Result<Self, MountError> {
        let path = PortablePath::parse(path, checkout.volume_config().limits)
            .map_err(MountError::InvalidPath)?;
        if self.bindings.contains_key(&path) {
            return Err(MountError::DuplicatePath);
        }
        let mount_id = MountId::new();
        self.bindings.insert(
            path.clone(),
            MountedCheckout {
                mount_id,
                path,
                checkout,
            },
        );
        Ok(self)
    }

    /// Seals the table after requiring a root binding.
    ///
    /// # Errors
    ///
    /// A mounted filesystem view must route every absolute path through `/`.
    pub fn build(self) -> Result<MountedView<A, O>, MountError> {
        if !self
            .bindings
            .keys()
            .any(|path| path.as_str() == PortablePath::ROOT)
        {
            return Err(MountError::MissingRoot);
        }
        Ok(MountedView {
            bindings: self.bindings,
        })
    }
}

fn relative_path(
    path: &PortablePath,
    mount: &PortablePath,
    limits: crate::model::VolumeLimits,
) -> Result<PortablePath, MountError> {
    if mount.as_str() == PortablePath::ROOT {
        return Ok(path.clone());
    }
    if path == mount {
        return PortablePath::parse(PortablePath::ROOT, limits).map_err(MountError::InvalidPath);
    }
    let relative = path
        .as_str()
        .strip_prefix(mount.as_str())
        .ok_or(MountError::UnmappedPath)?;
    PortablePath::parse(relative, limits).map_err(MountError::InvalidPath)
}

/// Reproducible selection of one generation for each mounted volume.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MountedViewSnapshot {
    /// Bindings sorted by canonical mount path.
    pub bindings: Vec<MountedGeneration>,
}

/// One mounted generation in a reproducible view snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MountedGeneration {
    /// Stable mount identity.
    pub mount_id: MountId,
    /// Canonical mount path.
    pub path: PortablePath,
    /// Independent volume identity.
    pub volume_id: VolumeId,
    /// Selected immutable generation.
    pub generation: GenerationId,
    /// Access admitted through this binding.
    pub access: AccessMode,
    /// Refresh behavior selected for this binding.
    pub consistency: ConsistencyMode,
}

/// Mount composition failures.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum MountError {
    /// Two volumes were assigned the same path.
    #[error("mount path is already bound")]
    DuplicatePath,
    /// A complete mounted view requires `/`.
    #[error("mounted view has no root binding")]
    MissingRoot,
    /// No volume owns the supplied path.
    #[error("path is not mapped to a volume")]
    UnmappedPath,
    /// Portable rename cannot cross independently atomic checkouts.
    #[error("cross-volume rename is not supported")]
    CrossVolume,
    /// A mount or routed path violates canonical path bounds.
    #[error("invalid mounted path: {0}")]
    InvalidPath(PathError),
    /// A routed portable path could not become an exact namespace path.
    #[error("invalid routed namespace path: {0}")]
    InvalidNamespacePath(NamespacePathError),
}

#[cfg(test)]
#[path = "tests/mount.rs"]
mod tests;
